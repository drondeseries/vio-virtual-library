package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const queuePagePath = "/admin/virtual-library"

type adminRoutes struct {
	pb.UnimplementedHttpRoutesServer
	server *runtimeServer
}

type prowlarrReleaseResult struct {
	Title       string `json:"title"`
	Indexer     string `json:"indexer"`
	Size        int64  `json:"size"`
	IMDbID      int64  `json:"imdb_id,omitempty"`
	TMDBID      int64  `json:"tmdb_id,omitempty"`
	TVDBID      int64  `json:"tvdb_id,omitempty"`
	PublishDate string `json:"publish_date,omitempty"`
	Score       int    `json:"score"`
	Rejected    bool   `json:"rejected,omitempty"`
}

func newAdminRoutes(server *runtimeServer) *adminRoutes { return &adminRoutes{server: server} }

// sensitiveQueryParamPattern strips credential-bearing query parameters so
// upstream transport errors (which embed full URLs) can be shown to admins
// without disclosing API keys or tokens.
var sensitiveQueryParamPattern = regexp.MustCompile(`(?i)([?&](?:api_?key|apikey|token|secret|password|access_?token)=)[^&\s'"]+`)

func redactError(err error) string {
	if err == nil {
		return ""
	}
	return sensitiveQueryParamPattern.ReplaceAllString(err.Error(), "${1}[redacted]")
}

func (a *adminRoutes) Handle(ctx context.Context, req *pb.HandleHTTPRequest) (*pb.HandleHTTPResponse, error) {
	headers := req.GetHeaders()
	role := headers["X-Vio-User-Role"]
	if role == "" {
		role = headers["X-Silo-User-Role"] // legacy host compat
	}
	if req == nil || !strings.EqualFold(role, "admin") {
		return adminJSON(http.StatusForbidden, map[string]string{"error": "admin access required"})
	}
	path := strings.TrimRight(req.GetPath(), "/")
	switch {
	case path == queuePagePath && req.GetMethod() == http.MethodGet:
		return adminAsset(http.StatusOK, adminPageHTML, "text/html; charset=utf-8")
	case path == queuePagePath+"/queue" && req.GetMethod() == http.MethodGet:
		return a.queueJSON()
	case path == queuePagePath+"/calendar" && req.GetMethod() == http.MethodGet:
		return a.calendarJSON()
	case (path == queuePagePath+"/schedule/refresh" || path == "/admin/schedule/refresh") && req.GetMethod() == http.MethodPost:
		return a.refreshSchedule(ctx)
	case (path == queuePagePath+"/schedule" || path == "/admin/schedule") && req.GetMethod() == http.MethodGet:
		return a.scheduleJSON()
	case path == queuePagePath+"/queue/force" && req.GetMethod() == http.MethodPost:
		return a.forceQueueItem(ctx, req.GetQuery().GetFields())
	case path == queuePagePath+"/queue/search" && req.GetMethod() == http.MethodPost:
		return a.searchQueueItem(ctx, req.GetQuery().GetFields())
	case path == queuePagePath+"/queue/remove" && req.GetMethod() == http.MethodPost:
		return a.removeQueueItem(req.GetQuery().GetFields())
	default:
		return adminJSON(http.StatusNotFound, map[string]string{"error": "route not found"})
	}
}

func (a *adminRoutes) searchQueueItem(ctx context.Context, fields map[string]*structpb.Value) (*pb.HandleHTTPResponse, error) {
	key := queryString(fields, "key")
	item, ok := a.server.monitor.item(key)
	if !ok {
		return adminJSON(http.StatusNotFound, map[string]string{"error": "queue item not found"})
	}
	a.server.monitor.mu.Lock()
	client := a.server.monitor.prowlarr
	quality := a.server.monitor.config.Quality
	a.server.monitor.mu.Unlock()
	if client == nil || client.URL() == "" {
		return adminJSON(http.StatusBadRequest, map[string]string{"error": "Prowlarr is not configured"})
	}
	releases, err := client.SearchItem(ctx, item)
	if err != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(err)})
	}
	sortProwlarrReleases(releases, quality.CustomFormats)
	matched := matchProwlarrReleasesWithQuality(releases, item, quality)
	if matched {
		item.Ready = true
		item.Force = false
		if err := a.server.monitor.register(ctx, item); err != nil {
			return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(err)})
		}
		a.server.monitor.markRegistered(item.Key)
		if err := a.server.monitor.remember(item); err != nil {
			return adminJSON(http.StatusInternalServerError, map[string]string{"error": redactError(err)})
		}
	}
	results := make([]prowlarrReleaseResult, 0, len(releases))
	for _, release := range releases {
		var score int
		var rejected bool
		if release.parsedCandidate != nil && len(quality.CustomFormats) > 0 {
			score, rejected = customFormatScore(*release.parsedCandidate, quality.CustomFormats)
		}
		results = append(results, prowlarrReleaseResult{
			Title: release.Title, Indexer: release.Indexer, Size: release.Size,
			IMDbID: release.IMDbID, TMDBID: release.TMDBID, TVDBID: release.TVDBID,
			PublishDate: release.PublishDate,
			Score: score, Rejected: rejected,
		})
	}
	return adminJSON(http.StatusOK, map[string]any{"matched": matched, "registered": matched, "title": item.Title, "count": len(results), "releases": results})
}

func (a *adminRoutes) queueJSON() (*pb.HandleHTTPResponse, error) {
	a.server.monitor.mu.Lock()
	items := make([]monitoredMedia, 0, len(a.server.monitor.items))
	now := time.Now()
	for _, item := range a.server.monitor.items {
		if item.MediaType == "series" {
			item.Episodes = missingEpisodes(item.Episodes, now)
			if len(item.Episodes) > 0 {
				items = append(items, item)
			}
			continue
		}
		if !item.Ready {
			items = append(items, item)
		}
	}
	a.server.monitor.mu.Unlock()
	sort.Slice(items, func(i, j int) bool {
		return strings.ToLower(items[i].Title) < strings.ToLower(items[j].Title)
	})
	return adminJSON(http.StatusOK, map[string]any{"items": items, "count": len(items), "generated_at": time.Now().UTC()})
}

func (a *adminRoutes) calendarJSON() (*pb.HandleHTTPResponse, error) {
	a.server.monitor.mu.Lock()
	items := make([]monitoredMedia, 0, len(a.server.monitor.items))
	for _, item := range a.server.monitor.items {
		if !item.Ready && !item.Release.IsZero() {
			items = append(items, item)
		}
	}
	a.server.monitor.mu.Unlock()
	sort.Slice(items, func(i, j int) bool { return items[i].Release.Before(items[j].Release) })
	return adminJSON(http.StatusOK, map[string]any{"items": items, "count": len(items), "from": time.Now().UTC().Format("2006-01-02"), "to": time.Now().UTC().AddDate(0, 1, 0).Format("2006-01-02")})
}

func (a *adminRoutes) scheduleJSON() (*pb.HandleHTTPResponse, error) {
	if a.server == nil || a.server.releaseStore == nil {
		return adminJSON(http.StatusOK, map[string]any{
			"count":        0,
			"shows":        []any{},
			"generated_at": time.Now().UTC(),
		})
	}
	return adminJSON(http.StatusOK, a.server.releaseStore.GetScheduleSummary())
}

func (a *adminRoutes) refreshSchedule(ctx context.Context) (*pb.HandleHTTPResponse, error) {
	if a.server == nil || a.server.scheduler == nil || a.server.releaseStore == nil {
		return adminJSON(http.StatusBadRequest, map[string]string{"error": "release scheduler is not configured"})
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := a.server.scheduler.RefreshAll(refreshCtx); err != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(err)})
	}
	summary := a.server.releaseStore.GetScheduleSummary()
	return adminJSON(http.StatusOK, map[string]any{
		"message": "Schedule refresh completed",
		"summary": summary,
	})
}

func (a *adminRoutes) forceQueueItem(ctx context.Context, fields map[string]*structpb.Value) (*pb.HandleHTTPResponse, error) {
	key := queryString(fields, "key")
	item, ok := a.server.monitor.item(key)
	if !ok {
		return adminJSON(http.StatusNotFound, map[string]string{"error": "queue item not found"})
	}
	item.Force = true
	updated, message, evaluationErr := a.server.monitor.evaluate(ctx, item)
	if evaluationErr != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(evaluationErr)})
	}
	if !updated.Ready || strings.TrimSpace(updated.Title) == "" || (updated.MediaType == "series" && len(updated.Episodes) == 0) {
		return adminJSON(http.StatusConflict, map[string]string{"error": "metadata is not ready to register", "message": message})
	}
	if err := a.server.monitor.register(ctx, updated); err != nil {
		return adminJSON(http.StatusBadGateway, map[string]string{"error": redactError(err)})
	}
	a.server.monitor.markRegistered(updated.Key)
	if err := a.server.monitor.remember(updated); err != nil {
		return adminJSON(http.StatusInternalServerError, map[string]string{"error": redactError(err)})
	}
	return adminJSON(http.StatusOK, map[string]any{"item": updated, "message": "Media was force-added to the virtual library"})
}

func (a *adminRoutes) removeQueueItem(fields map[string]*structpb.Value) (*pb.HandleHTTPResponse, error) {
	key := queryString(fields, "key")
	if key == "" {
		return adminJSON(http.StatusBadRequest, map[string]string{"error": "key is required"})
	}
	if err := a.server.monitor.forget(key); err != nil {
		return adminJSON(http.StatusInternalServerError, map[string]string{"error": redactError(err)})
	}
	return adminJSON(http.StatusOK, map[string]string{"message": "Queue item removed"})
}

func queryString(fields map[string]*structpb.Value, key string) string {
	if value, ok := fields[key]; ok {
		return strings.TrimSpace(value.GetStringValue())
	}
	return ""
}

func adminJSON(status int, payload any) (*pb.HandleHTTPResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &pb.HandleHTTPResponse{StatusCode: int32(status), Body: body, Headers: map[string]string{"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store"}}, nil
}

func adminAsset(status int, body []byte, contentType string) (*pb.HandleHTTPResponse, error) {
	return &pb.HandleHTTPResponse{StatusCode: int32(status), Body: body, Headers: map[string]string{"Content-Type": contentType, "Cache-Control": "no-store"}}, nil
}

//go:embed web/admin/index.html
var adminIndexHTML []byte

//go:embed web/admin/styles.css
var adminStylesCSS []byte

//go:embed web/admin/app.js
var adminAppJS []byte

// adminPageHTML is composed once at startup: the shell with styles and
// script inlined. The Release Desk is served through host proxies whose
// mount paths (and trailing slashes) vary, so external asset references
// are never URL-resolvable reliably — a single self-contained document
// is the only robust delivery.
var adminPageHTML = func() []byte {
	page := strings.Replace(string(adminIndexHTML), "__INLINE_CSS__", string(adminStylesCSS), 1)
	return []byte(strings.Replace(page, "__INLINE_JS__", string(adminAppJS), 1))
}()
