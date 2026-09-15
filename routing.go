package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	sdkruntime "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/drondeseries/vio-virtual-library/pkg/release"
	"github.com/hashicorp/go-hclog"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	tmdbBaseURL      = "https://api.themoviedb.org/3"
	cinemetaBaseURL  = "https://v3-cinemeta.strem.io"
	tvmazeBaseURL    = "https://api.tvmaze.com"
	metadataClient   = newRestrictedRedirectHTTPClient(20 * time.Second)
	errNoHomeRelease = errors.New("TMDB metadata has no digital or physical release date")
)

const (
	maxMonitoredItems    = 10000
	maxMonitorStateBytes = 32 << 20
	maxMonitoredEpisodes = 10000
	maxMonitorKeyBytes   = 512
)

type monitorConfig struct {
	TMDBAPIKey        string
	File              string
	ProwlarrIndexFile string
	FilterProwlarr    bool
	Quality           QualityConfig
}
type monitoredMedia struct {
	Key           string           `json:"key"`
	MediaType     string           `json:"media_type"`
	Title         string           `json:"title"`
	StreamID      string           `json:"stream_id,omitempty"`
	IMDbID        string           `json:"imdb_id,omitempty"`
	TMDBID        string           `json:"tmdb_id,omitempty"`
	TVDBID        string           `json:"tvdb_id,omitempty"`
	SourceKey     string           `json:"source_key,omitempty"`
	MediaFolderID int              `json:"media_folder_id,omitempty"`
	Year          int32            `json:"year,omitempty"`
	Runtime       int              `json:"runtime,omitempty"`
	Release       time.Time        `json:"release,omitempty"`
	Ready         bool             `json:"ready"`
	Force         bool             `json:"force"`
	Overview      string           `json:"overview,omitempty"`
	Poster        string           `json:"poster,omitempty"`
	Backdrop      string           `json:"backdrop,omitempty"`
	Genres        []string         `json:"genres,omitempty"`
	Episodes      []virtualEpisode `json:"episodes,omitempty"`
}
type virtualEpisode struct {
	Season    int       `json:"season"`
	Episode   int       `json:"episode"`
	Runtime   int       `json:"runtime,omitempty"`
	Title     string    `json:"title"`
	Overview  string    `json:"overview,omitempty"`
	Thumbnail string    `json:"thumbnail,omitempty"`
	Released  time.Time `json:"released,omitempty"`
	Available bool      `json:"available,omitempty"`
}
type mediaMonitor struct {
	mu           sync.Mutex
	resolver     streamResolver
	logger       hclog.Logger
	config       monitorConfig
	items        map[string]monitoredMedia
	registrar    virtualMediaRegistrar
	prowlarr     *prowlarrSearchClient
	altmount     *altmountStateClient
	registered   map[string]struct{}
	releaseStore *release.ReleaseStore
}

type virtualMediaLister interface {
	ListVirtual(context.Context) ([]monitoredMedia, error)
}

func (m *mediaMonitor) setRegistrar(registrar virtualMediaRegistrar) {
	m.mu.Lock()
	m.registrar = registrar
	m.mu.Unlock()
}

// prowlarrMatch returns true when any configured RSS feed contains a
// release matching the monitored item.
func (m *mediaMonitor) prowlarrMatch(item monitoredMedia) bool {
	m.mu.Lock()
	c := m.prowlarr
	m.mu.Unlock()
	if c == nil {
		return false
	}
	if c.URL() == "" {
		return false
	}
	m.mu.Lock()
	quality := m.config.Quality
	m.mu.Unlock()
	return c.MatchWithQuality(item, quality)
}

// configureProwlarr sets up the Prowlarr search client with the first
// non-empty URL from the list. Multiple URLs / per-indexer discovery are
// no longer needed — /api/v1/search covers all indexers in one request.
func (m *mediaMonitor) configureProwlarr(urls, apiKey string, intervalMinutes int, indexFile string) error {
	firstURL := ""
	for _, u := range strings.FieldsFunc(urls, func(r rune) bool { return r == '\n' || r == ',' }) {
		u = strings.TrimSpace(u)
		if u != "" {
			firstURL = u
			break
		}
	}
	m.mu.Lock()
	if m.prowlarr == nil {
		m.prowlarr = newProwlarrSearchClient(nil)
	}
	m.mu.Unlock()
	m.prowlarr.Configure(firstURL, apiKey, intervalMinutes)
	return m.prowlarr.ConfigureIndexFile(indexFile)
}

// configureAltmount sets up the AltMount completed/failed state client. The
// URL may be blank, in which case the client stays inert and Prowlarr remains
// the known-good fallback.
func (m *mediaMonitor) configureAltmount(baseURL, apiKey string, intervalMinutes int, indexFile string) error {
	m.mu.Lock()
	if m.altmount == nil {
		m.altmount = newAltmountStateClient(nil)
	}
	m.mu.Unlock()
	m.altmount.Configure(baseURL, apiKey, intervalMinutes)
	return m.altmount.ConfigureIndexFile(indexFile)
}

// altmountClient returns the AltMount state client, or a new empty one.
func (m *mediaMonitor) altmountClient() *altmountStateClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.altmount != nil {
		return m.altmount
	}
	return newAltmountStateClient(nil)
}

// prowlarrClient returns the Prowlarr search client, or a new empty one for Validate.
func (m *mediaMonitor) prowlarrClient() *prowlarrSearchClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.prowlarr != nil {
		return m.prowlarr
	}
	return newProwlarrSearchClient(nil)
}

func (m *mediaMonitor) markRegistered(key string) {
	m.mu.Lock()
	m.registered[key] = struct{}{}
	m.mu.Unlock()
}

func (m *mediaMonitor) isRegistered(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.registered[key]
	return ok
}

func (m *mediaMonitor) register(ctx context.Context, item monitoredMedia) error {
	m.mu.Lock()
	registrar := m.registrar
	m.mu.Unlock()
	if registrar == nil {
		return errors.New("Vio virtual catalog service is not configured")
	}
	return registrar.Register(ctx, item)
}

func newMediaMonitor(resolver streamResolver, logger hclog.Logger) *mediaMonitor {
	return &mediaMonitor{
		resolver:   resolver,
		logger:     logger,
		config:     monitorConfig{File: ".vio-virtual-library-monitored.json", ProwlarrIndexFile: ".vio-virtual-library-prowlarr-index.json"},
		items:      map[string]monitoredMedia{},
		prowlarr:   nil,
		registered: map[string]struct{}{},
	}
}
func (m *mediaMonitor) Configure(c monitorConfig) error {
	configured, loaded, err := loadMonitorConfig(c)
	if err != nil {
		return err
	}
	m.applyConfiguration(configured, loaded, nil, false)
	return nil
}

func loadMonitorConfig(c monitorConfig) (monitorConfig, map[string]monitoredMedia, error) {
	if c.File == "" {
		c.File = ".vio-virtual-library-monitored.json"
	}
	if c.ProwlarrIndexFile == "" {
		c.ProwlarrIndexFile = ".vio-virtual-library-prowlarr-index.json"
	}
	loaded := make(map[string]monitoredMedia)
	file, err := os.Open(c.File)
	if err == nil {
		defer file.Close()
		data, readErr := io.ReadAll(io.LimitReader(file, maxMonitorStateBytes+1))
		if readErr != nil {
			return monitorConfig{}, nil, fmt.Errorf("read monitored queue: %w", readErr)
		}
		if len(data) > maxMonitorStateBytes {
			return monitorConfig{}, nil, fmt.Errorf("monitored queue exceeds %d bytes", maxMonitorStateBytes)
		}
		var items []monitoredMedia
		if err := json.Unmarshal(data, &items); err != nil {
			return monitorConfig{}, nil, fmt.Errorf("decode monitored queue: %w", err)
		}
		if len(items) > maxMonitoredItems {
			return monitorConfig{}, nil, fmt.Errorf("monitored queue exceeds %d items", maxMonitoredItems)
		}
		duplicateKeys := make(map[string]struct{}, len(items))
		for _, item := range items {
			if _, exists := duplicateKeys[item.Key]; exists {
				return monitorConfig{}, nil, fmt.Errorf("monitored queue contains duplicate key %q", item.Key)
			}
			duplicateKeys[item.Key] = struct{}{}
		}
		for _, item := range items {
			if err := validateMonitoredMedia(item); err != nil {
				return monitorConfig{}, nil, fmt.Errorf("invalid monitored queue item: %w", err)
			}
			loaded[item.Key] = item
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return monitorConfig{}, nil, fmt.Errorf("open monitored queue: %w", err)
	}
	return c, loaded, nil
}

func (m *mediaMonitor) applyConfiguration(c monitorConfig, items map[string]monitoredMedia, registrar virtualMediaRegistrar, replaceRegistrar bool) {
	m.mu.Lock()
	m.config = c
	m.items = items
	if replaceRegistrar {
		m.registrar = registrar
	}
	m.mu.Unlock()
}
func (m *mediaMonitor) remember(item monitoredMedia) error {
	if err := validateMonitoredMedia(item); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	previous, existed := m.items[item.Key]
	if !existed && len(m.items) >= maxMonitoredItems {
		return fmt.Errorf("monitored queue is at its %d item limit", maxMonitoredItems)
	}
	m.items[item.Key] = item
	if err := m.saveLocked(); err != nil {
		if existed {
			m.items[item.Key] = previous
		} else {
			delete(m.items, item.Key)
		}
		return err
	}
	return nil
}

// rememberSeriesEpisodes merges freshly evaluated episodes into the persisted
// series item without dropping entries that are already registered, so the
// monitor queue can keep tracking upcoming episodes for ongoing series.
func (m *mediaMonitor) rememberSeriesEpisodes(key string, episodes []virtualEpisode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.items[key]
	if !ok {
		return
	}
	item.Episodes = mergeEpisodes(item.Episodes, episodes)
	m.items[key] = item
}

func mergeEpisodes(existing, fresh []virtualEpisode) []virtualEpisode {
	byKey := make(map[string]int, len(existing))
	for i, episode := range existing {
		byKey[episodeKey(episode)] = i
	}
	for _, episode := range fresh {
		key := episodeKey(episode)
		if idx, ok := byKey[key]; ok {
			if !existing[idx].Available && episode.Available {
				existing[idx].Available = true
			}
			if existing[idx].Title == "" {
				existing[idx].Title = episode.Title
			}
			if existing[idx].Released.IsZero() {
				existing[idx].Released = episode.Released
			}
			continue
		}
		byKey[key] = len(existing)
		existing = append(existing, episode)
	}
	return existing
}

func episodeKey(episode virtualEpisode) string {
	return fmt.Sprintf("%d:%d", episode.Season, episode.Episode)
}

func (m *mediaMonitor) forget(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	previous, existed := m.items[key]
	delete(m.items, key)
	if err := m.saveLocked(); err != nil {
		if existed {
			m.items[key] = previous
		}
		return err
	}
	return nil
}
func (m *mediaMonitor) item(key string) (monitoredMedia, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.items[key]
	return v, ok
}
func (m *mediaMonitor) saveLocked() error {
	items := make([]monitoredMedia, 0, len(m.items))
	for _, item := range m.items {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	if len(data)+1 > maxMonitorStateBytes {
		return fmt.Errorf("monitored queue exceeds %d bytes", maxMonitorStateBytes)
	}
	dir := filepath.Dir(m.config.File)
	tmp, err := os.CreateTemp(dir, ".silo-monitor-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(append(data, '\n')); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, m.config.File)
}

func validateMonitoredMedia(item monitoredMedia) error {
	if item.Key == "" || len(item.Key) > maxMonitorKeyBytes || strings.TrimSpace(item.Key) != item.Key {
		return errors.New("monitored media key is empty, invalid, or too long")
	}
	if len(item.Episodes) > maxMonitoredEpisodes {
		return fmt.Errorf("monitored media exceeds %d episodes", maxMonitoredEpisodes)
	}
	if item.MediaType != "movie" && item.MediaType != "series" {
		return errors.New("monitored media type must be movie or series")
	}
	return nil
}

func mediaFromRequest(r *pb.RequestDescriptor) (monitoredMedia, error) {
	if r == nil {
		return monitoredMedia{}, errors.New("request descriptor is required")
	}
	typ := strings.ToLower(strings.TrimSpace(r.GetMediaType()))
	if typ != "movie" && typ != "series" {
		return monitoredMedia{}, errors.New("media type must be movie or series")
	}
	ids := r.GetExternalIds()
	imdb, tmdb, tvdb := strings.TrimSpace(ids["imdb"]), strings.TrimSpace(ids["tmdb"]), strings.TrimSpace(ids["tvdb"])
	streamID := imdb
	if streamID == "" && typ == "series" && tvdb != "" {
		streamID = "tvdb:" + tvdb
	} else if streamID == "" && tmdb != "" {
		streamID = "tmdb:" + tmdb
	}
	if streamID == "" {
		return monitoredMedia{}, errors.New("IMDb, TVDB, or TMDB ID is required")
	}
	return monitoredMedia{Key: typ + ":" + streamID, MediaType: typ, Title: strings.TrimSpace(r.GetTitle()), Year: r.GetYear(), StreamID: streamID, IMDbID: imdb, TMDBID: tmdb, TVDBID: tvdb, SourceKey: "request:" + typ + ":" + streamID}, nil
}

func virtualContentID(item monitoredMedia) string {
	if item.MediaType == "series" && item.TVDBID != "" {
		return "series-tvdb-" + item.TVDBID
	}
	if item.TMDBID != "" {
		return item.MediaType + "-tmdb-" + item.TMDBID
	}
	if item.IMDbID != "" {
		return item.MediaType + "-imdb-" + item.IMDbID
	}
	return ""
}

func appendUniqueString(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func (m *mediaMonitor) evaluate(ctx context.Context, item monitoredMedia) (monitoredMedia, string, error) {
	now := time.Now()
	var metadataErr error
	if enriched, err := m.fetchCinemeta(ctx, item); err == nil && (item.MediaType != "series" || episodeMetadataComplete(enriched.Episodes)) {
		item = enriched
		if item.MediaType == "series" && episodeRuntimeMissing(item.Episodes) {
			if supplemented, supplementErr := m.fetchTVMaze(ctx, item); supplementErr == nil {
				item = supplemented
			}
		}
	} else {
		if item.MediaType == "series" {
			if fallback, fallbackErr := m.fetchTVMaze(ctx, item); fallbackErr == nil {
				item = fallback
			} else {
				m.logger.Warn("fetch series metadata", "key", item.Key, "cinemeta_error", err, "tvmaze_error", fallbackErr)
				metadataErr = errors.Join(err, fallbackErr)
			}
		} else {
			m.logger.Warn("fetch Cinemeta metadata", "key", item.Key, "error", err)
		}
	}
	if item.MediaType == "movie" {
		if runtime, err := m.fetchTMDBMovieRuntime(ctx, item); err == nil && runtime > 0 {
			item.Runtime = runtime
		}
		release, err := m.movieRelease(ctx, item)
		if err != nil {
			if item.Force && (errors.Is(err, errNoHomeRelease) || item.Title != "") {
				item.Ready = true
				return item, "Movie force-added by administrator", nil
			}
			if errors.Is(err, errNoHomeRelease) && m.prowlarrMatch(item) {
				item.Ready = true
				return item, "Movie release confirmed by indexer RSS feed", nil
			}
			item.Ready = false
			if errors.Is(err, errNoHomeRelease) {
				return item, "Movie is theatrical-only; monitoring indexer RSS feed", nil
			}
			return item, "Release metadata unavailable; monitoring will retry", err
		}
		item.Release = release
		if m.releaseStore != nil && item.IMDbID != "" && !release.IsZero() {
			m.releaseStore.SetMovie(item.IMDbID, release)
		}
		if item.Force {
			item.Ready = true
			return item, "Movie force-added by administrator", nil
		}
		if release.After(now) {
			if m.prowlarrMatch(item) {
				item.Ready = true
				return item, "Movie release confirmed by indexer search", nil
			}
			item.Ready = false
			return item, "Movie is not released for home media yet; monitoring indexer search", nil
		}
	}
	if item.MediaType == "series" {
		episodes := episodeList(item.Episodes)
		if item.Force {
			for i := range episodes {
				if episodes[i].Season > 0 && episodes[i].Episode > 0 {
					episodes[i].Available = true
				}
			}
		} else {
			available := appendUniqueEpisodes(airedEpisodes(episodes, now), m.prowlarrMatchedEpisodes(item, episodes))
			for i := range episodes {
				for _, match := range available {
					if episodes[i].Season == match.Season && episodes[i].Episode == match.Episode {
						episodes[i].Available = true
						break
					}
				}
			}
		}
		item.Episodes = episodes
		if m.releaseStore != nil && item.IMDbID != "" {
			epMap := make(map[string]release.EpisodeInfo, len(episodes))
			var nextAir *time.Time
			for _, ep := range episodes {
				key := fmt.Sprintf("%d:%d", ep.Season, ep.Episode)
				epMap[key] = release.EpisodeInfo{
					Season:  ep.Season,
					Episode: ep.Episode,
					AirDate: ep.Released,
					Title:   ep.Title,
				}
				if !ep.Released.IsZero() && ep.Released.After(now) {
					if nextAir == nil || ep.Released.Before(*nextAir) {
						t := ep.Released
						nextAir = &t
					}
				}
			}
			// Seed the store only when the scheduler has not yet written an
			// authoritative TVmaze schedule for this show; never overwrite a
			// real status (e.g. "Ended") with monitor-local guesses.
			m.releaseStore.SetShowIfAbsent(item.IMDbID, &release.ShowSchedule{
				IMDBID:      item.IMDbID,
				Title:       item.Title,
				Status:      "Monitoring",
				NextAirDate: nextAir,
				Episodes:    epMap,
			})
		}
		available := 0
		for _, episode := range episodes {
			if episode.Available {
				available++
			}
		}
		item.Ready = available > 0
		if !item.Ready {
			return item, "Series registered; monitoring for released episodes", metadataErr
		}
		return item, fmt.Sprintf("%d episodes registered for on-demand playback", available), metadataErr
	}
	item.Ready = true
	return item, "Movie is available for home media", nil
}

func (m *mediaMonitor) prowlarrMatchedEpisodes(item monitoredMedia, episodes []virtualEpisode) []virtualEpisode {
	m.mu.Lock()
	c := m.prowlarr
	quality := m.config.Quality
	m.mu.Unlock()
	if c == nil {
		return nil
	}
	matched := make([]virtualEpisode, 0, len(episodes))
	for _, episode := range episodes {
		if episode.Season <= 0 || episode.Episode <= 0 {
			continue
		}
		if c.MatchEpisodeWithQuality(item, episode, quality) {
			matched = append(matched, episode)
		}
	}
	return matched
}

func appendUniqueEpisodes(existing, additions []virtualEpisode) []virtualEpisode {
	seen := make(map[string]struct{}, len(existing)+len(additions))
	for _, episode := range existing {
		seen[fmt.Sprintf("%d:%d", episode.Season, episode.Episode)] = struct{}{}
	}
	for _, episode := range additions {
		key := fmt.Sprintf("%d:%d", episode.Season, episode.Episode)
		if _, ok := seen[key]; !ok {
			existing = append(existing, episode)
			seen[key] = struct{}{}
		}
	}
	return existing
}

func episodeList(episodes []virtualEpisode) []virtualEpisode {
	if episodes == nil {
		return []virtualEpisode{}
	}
	return episodes
}

func airedEpisodes(episodes []virtualEpisode, now time.Time) []virtualEpisode {
	aired := make([]virtualEpisode, 0, len(episodes))
	for _, episode := range episodeList(episodes) {
		if episode.Season <= 0 || episode.Episode <= 0 || episode.Released.IsZero() || episode.Released.After(now) {
			continue
		}
		aired = append(aired, episode)
	}
	return aired
}

func missingEpisodes(episodes []virtualEpisode, _ time.Time) []virtualEpisode {
	missing := make([]virtualEpisode, 0, len(episodes))
	for _, episode := range episodeList(episodes) {
		if episode.Season > 0 && episode.Episode > 0 && !episode.Released.IsZero() && !episode.Available {
			missing = append(missing, episode)
		}
	}
	return missing
}

func episodeMetadataComplete(episodes []virtualEpisode) bool {
	if len(episodes) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(episodes))
	for _, episode := range episodes {
		if episode.Season <= 0 || episode.Episode <= 0 || strings.TrimSpace(episode.Title) == "" {
			return false
		}
		key := episodeKey(episode)
		if _, exists := seen[key]; exists {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}

func mergeSeriesEpisodeMetadata(existing, fresh []virtualEpisode) []virtualEpisode {
	byKey := make(map[string]virtualEpisode, len(existing)+len(fresh))
	order := make([]string, 0, len(existing)+len(fresh))
	add := func(episode virtualEpisode) {
		if episode.Season <= 0 || episode.Episode <= 0 {
			return
		}
		key := episodeKey(episode)
		if _, exists := byKey[key]; !exists {
			order = append(order, key)
			byKey[key] = episode
			return
		}
		current := byKey[key]
		if current.Title == "" {
			current.Title = episode.Title
		}
		if current.Overview == "" {
			current.Overview = episode.Overview
		}
		if current.Thumbnail == "" {
			current.Thumbnail = episode.Thumbnail
		}
		if current.Runtime <= 0 {
			current.Runtime = episode.Runtime
		}
		if current.Released.IsZero() {
			current.Released = episode.Released
		}
		byKey[key] = current
	}
	for _, episode := range existing {
		add(episode)
	}
	for _, episode := range fresh {
		add(episode)
	}
	result := make([]virtualEpisode, 0, len(order))
	for _, key := range order {
		result = append(result, byKey[key])
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Season != result[j].Season {
			return result[i].Season < result[j].Season
		}
		return result[i].Episode < result[j].Episode
	})
	return result
}

func episodeRuntimeMissing(episodes []virtualEpisode) bool {
	for _, episode := range episodes {
		if episode.Season > 0 && episode.Episode > 0 && episode.Runtime <= 0 {
			return true
		}
	}
	return false
}

func (s *runtimeServer) Fulfill(ctx context.Context, req *pb.FulfillRequest) (resp *pb.FulfillResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.monitor.logger.Error("panic in Fulfill", "error", r)
			err = fmt.Errorf("plugin fulfill panic: %v", r)
		}
	}()
	item, err := mediaFromRequest(req.GetRequest())
	if err != nil {
		return nil, err
	}
	item, message, _ := s.monitor.evaluate(ctx, item)
	if len(req.GetConnections()) > 0 {
		folderID, folderErr := configuredFolderID(req.GetConnections()[0].GetConfig().AsMap()["media_folder_id"])
		if folderErr != nil {
			return nil, folderErr
		}
		item.MediaFolderID = folderID
	}
	// If media is available but still has no title or is a series with no
	// episodes (Silo did not include episodes in the request descriptor and
	// metadata lookup hasn't resolved them yet), keep it queued so the
	// scheduled monitor can retry once episode metadata arrives.
	if item.Ready && (strings.TrimSpace(item.Title) == "" || (item.MediaType == "series" && len(item.Episodes) == 0)) {
		item.Ready = false
		if item.MediaType == "series" && len(item.Episodes) == 0 {
			message = "Queued; waiting for episode metadata"
		} else {
			message = "Queued; waiting for metadata before registering"
		}
	}
	if item.Ready {
		if err := s.monitor.register(ctx, item); err != nil {
			return nil, fmt.Errorf("register virtual media: %w", err)
		}
		s.monitor.markRegistered(item.Key)
		message = "Virtual media registered in Vio library"
		if item.MediaType == "series" {
			s.monitor.rememberSeriesEpisodes(item.Key, item.Episodes)
		}
	}
	if err := s.monitor.remember(item); err != nil {
		return nil, fmt.Errorf("persist monitored media: %w", err)
	}
	status, external := "queued", "monitored"
	if item.Ready {
		status, external = "completed", "registered"
	}
	targets := make([]*pb.FulfillmentTarget, 0, len(req.GetQualities()))
	conn := ""
	if len(req.GetConnections()) > 0 {
		conn = req.GetConnections()[0].GetId()
	}
	for _, q := range req.GetQualities() {
		targets = append(targets, &pb.FulfillmentTarget{Quality: q.GetId(), ConnectionId: conn, ExternalId: item.Key, Status: status, ExternalStatus: external, Message: message})
	}
	return &pb.FulfillResponse{Targets: targets, Message: message}, nil
}

func (s *runtimeServer) CheckStatus(ctx context.Context, req *pb.CheckStatusRequest) (*pb.CheckStatusResponse, error) {
	base, err := mediaFromRequest(req.GetRequest())
	if err != nil {
		return nil, err
	}
	statuses := make([]*pb.TargetStatus, 0, len(req.GetTargets()))
	for _, target := range req.GetTargets() {
		item, ok := s.monitor.item(target.GetExternalId())
		if !ok {
			item = base
		}
		item, message, _ := s.monitor.evaluate(ctx, item)
		// If media is available but still has no title or is a series with no
		// episodes, keep it queued rather than failing the RPC with an SDK validation error.
		if item.Ready && (strings.TrimSpace(item.Title) == "" || (item.MediaType == "series" && len(item.Episodes) == 0)) {
			item.Ready = false
			if item.MediaType == "series" && len(item.Episodes) == 0 {
				message = "Queued; waiting for episode metadata"
			} else {
				message = "Queued; waiting for metadata before registering"
			}
		}
		if item.Ready {
			if err := s.monitor.register(ctx, item); err != nil {
				return nil, fmt.Errorf("register virtual media: %w", err)
			}
			s.monitor.markRegistered(item.Key)
			message = "Virtual media registered in Vio library"
			if item.MediaType == "series" {
				s.monitor.rememberSeriesEpisodes(item.Key, item.Episodes)
			}
		}
		if err := s.monitor.remember(item); err != nil {
			return nil, fmt.Errorf("persist monitored media: %w", err)
		}
		status, external := "queued", "monitored"
		if item.Ready {
			status, external = "completed", "registered"
		}
		statuses = append(statuses, &pb.TargetStatus{Quality: target.GetQuality(), ConnectionId: target.GetConnectionId(), Status: status, ExternalStatus: external, Message: message})
	}
	return &pb.CheckStatusResponse{Statuses: statuses}, nil
}
func (s *runtimeServer) ListConfigOptions(ctx context.Context, _ *pb.ListConfigOptionsRequest) (*pb.ListConfigOptionsResponse, error) {
	host := sdkruntime.Host()
	if host == nil {
		return &pb.ListConfigOptionsResponse{}, nil
	}
	libs, err := host.ListLibraries(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list host libraries for config options: %w", err)
	}
	s.monitor.logger.Info("ListConfigOptions: host returned libraries", "count", len(libs))
	movies := make([]*pb.ConfigOption, 0)
	series := make([]*pb.ConfigOption, 0)
	for _, lib := range libs {
		if lib == nil {
			continue
		}
		mediaType := strings.ToLower(strings.TrimSpace(lib.GetMediaType()))
		s.monitor.logger.Info("ListConfigOptions: library", "id", lib.GetId(), "name", lib.GetName(), "media_type", mediaType)
		switch mediaType {
		case "movie", "movies":
			movies = append(movies, &pb.ConfigOption{Value: lib.GetId(), Label: lib.GetName()})
		case "tv", "show", "shows", "series":
			series = append(series, &pb.ConfigOption{Value: lib.GetId(), Label: lib.GetName()})
		case "mixed":
			movies = append(movies, &pb.ConfigOption{Value: lib.GetId(), Label: lib.GetName()})
			series = append(series, &pb.ConfigOption{Value: lib.GetId(), Label: lib.GetName()})
		default:
			s.monitor.logger.Warn("ListConfigOptions: unrecognized library media_type", "id", lib.GetId(), "name", lib.GetName(), "media_type", mediaType)
		}
	}
	optionsByField := map[string]*pb.ConfigOptionList{}
	if len(movies) > 0 {
		optionsByField["movie_library_id"] = &pb.ConfigOptionList{Options: movies}
	}
	if len(series) > 0 {
		optionsByField["series_library_id"] = &pb.ConfigOptionList{Options: series}
	}
	return &pb.ListConfigOptionsResponse{OptionsByField: optionsByField}, nil
}
func (s *runtimeServer) Validate(context.Context, *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	return &pb.ValidateResponse{FieldErrors: map[string]string{}}, nil
}
func (s *runtimeServer) TestConnection(ctx context.Context, _ *pb.TestConnectionRequest) (*pb.TestConnectionResponse, error) {
	// Wrap everything in a hard 8s deadline so the SDK gRPC context
	// never races against slow provider or indexer responses.
	testCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	totalStart := time.Now()
	start := totalStart
	err := s.resolver.ValidateConnection(testCtx)
	s.monitor.logger.Info("TestConnection phase", "phase", "provider", "duration_ms", time.Since(start).Milliseconds())
	if err != nil {
		return &pb.TestConnectionResponse{Ok: false, Message: err.Error()}, nil
	}
	msg := "Connected to streaming provider"
	client := s.monitor.prowlarrClient()
	prowlarrURL := client.URL()
	s.monitor.logger.Info("TestConnection phase", "phase", "prowlarr-check", "url_configured", prowlarrURL != "")
	if prowlarrURL != "" {
		start = time.Now()
		searchMsg, searchErr := client.Validate(testCtx)
		s.monitor.logger.Info("TestConnection phase", "phase", "prowlarr", "duration_ms", time.Since(start).Milliseconds())
		if searchErr != nil {
			msg += fmt.Sprintf("\nProwlarr search: %s", searchErr.Error())
		} else {
			msg += fmt.Sprintf("\n%s", searchMsg)
		}
	}
	if altmount := s.monitor.altmountClient(); altmount.URL() != "" {
		start = time.Now()
		stateMsg, stateErr := altmount.Validate(testCtx)
		s.monitor.logger.Info("TestConnection phase", "phase", "altmount", "duration_ms", time.Since(start).Milliseconds())
		if stateErr != nil {
			msg += fmt.Sprintf("\nAltMount state: %s", stateErr.Error())
		} else {
			msg += fmt.Sprintf("\n%s", stateMsg)
		}
	}
	s.monitor.logger.Info("TestConnection phase", "phase", "done", "total_ms", time.Since(totalStart).Milliseconds())
	return &pb.TestConnectionResponse{Ok: true, Message: msg}, nil
}
func (s *runtimeServer) Run(ctx context.Context, req *pb.RunScheduledTaskRequest) (*pb.RunScheduledTaskResponse, error) {
	if req.GetTaskKey() != "" && req.GetTaskKey() != "monitor-media" {
		return nil, fmt.Errorf("unknown task key %q", req.GetTaskKey())
	}
	s.monitor.mu.Lock()
	itemsByKey := make(map[string]monitoredMedia, len(s.monitor.items))
	for _, v := range s.monitor.items {
		itemsByKey[v.Key] = v
	}
	registrar := s.monitor.registrar
	s.monitor.mu.Unlock()
	if lister, ok := registrar.(virtualMediaLister); ok {
		existing, err := lister.ListVirtual(ctx)
		if err != nil {
			return nil, fmt.Errorf("list existing virtual media: %w", err)
		}
		for _, item := range existing {
			itemsByKey[item.Key] = item
			s.monitor.markRegistered(item.Key)
		}
	}
	items := make([]monitoredMedia, 0, len(itemsByKey))
	for _, item := range itemsByKey {
		items = append(items, item)
	}
	s.monitor.mu.Lock()
	client := s.monitor.prowlarr
	altmount := s.monitor.altmount
	s.monitor.mu.Unlock()
	if client != nil {
		if err := client.refreshIfStale(ctx); err != nil {
			s.monitor.logger.Warn("refresh Prowlarr search", "error", err)
		}
	}
	if altmount != nil && altmount.URL() != "" {
		if err := altmount.refreshIfStale(ctx); err != nil {
			s.monitor.logger.Warn("refresh AltMount state", "error", err)
		}
	}
	ready, pending := 0, 0
	keepBySource := make(map[string][]string)
	reconcileSafeBySource := make(map[string]bool)
	for _, item := range items {
		if ctx.Err() != nil {
			// Host deadline (2 minutes for scheduled tasks) hit mid-pass:
			// stop evaluating and report what was completed so the run
			// degrades gracefully instead of dying inside a fetch.
			s.monitor.logger.Warn("monitor run stopped by context deadline", "completed", ready+pending, "total", len(items))
			break
		}
		source := item.SourceKey
		if source == "" {
			source = "monitor"
		}
		if _, exists := keepBySource[source]; !exists {
			keepBySource[source] = nil
			reconcileSafeBySource[source] = true
		}
		// Keep the previously known content unless an authoritative successful
		// pass proves it should be absent. Metadata and host failures must never
		// turn an empty keep-set into destructive reconciliation.
		if contentID := virtualContentID(item); contentID != "" {
			keepBySource[source] = appendUniqueString(keepBySource[source], contentID)
		}
		updated, _, evaluationErr := s.monitor.evaluate(ctx, item)
		if evaluationErr != nil {
			pending++
			reconcileSafeBySource[source] = false
			if ctx.Err() != nil || errors.Is(evaluationErr, context.Canceled) || errors.Is(evaluationErr, context.DeadlineExceeded) || strings.Contains(evaluationErr.Error(), "context canceled") || strings.Contains(evaluationErr.Error(), "deadline exceeded") {
				s.monitor.logger.Warn("monitor run stopped by context deadline", "completed", ready+pending, "total", len(items))
				break
			}
			s.monitor.logger.Warn("evaluate virtual media", "key", item.Key, "error", evaluationErr)
			continue
		}
		if updated.Ready && (strings.TrimSpace(updated.Title) == "" || (updated.MediaType == "series" && len(updated.Episodes) == 0)) {
			updated.Ready = false
			pending++
			if err := s.monitor.remember(updated); err != nil {
				return nil, err
			}
			continue
		}
		if updated.Ready {
			if updated.MediaType == "movie" && s.monitor.isRegistered(updated.Key) {
				pending++
				continue
			}
			if err := s.monitor.register(ctx, updated); err != nil {
				pending++
				reconcileSafeBySource[source] = false
				if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "deadline exceeded") {
					s.monitor.logger.Warn("monitor run stopped by context deadline", "completed", ready+pending, "total", len(items))
					break
				}
				s.monitor.logger.Error("register virtual media", "key", updated.Key, "error", err)
				continue
			}
			s.monitor.markRegistered(updated.Key)
			ready++
			source := updated.SourceKey
			if source == "" {
				source = "monitor"
			}
			if contentID := virtualContentID(updated); contentID != "" {
				keepBySource[source] = appendUniqueString(keepBySource[source], contentID)
			}
			if err := s.monitor.remember(updated); err != nil {
				return nil, err
			}
		} else {
			pending++
			if err := s.monitor.remember(updated); err != nil {
				return nil, err
			}
		}
	}
	if reconciler, ok := registrar.(interface {
		Reconcile(context.Context, string, []string) error
	}); ok {
		for source, keep := range keepBySource {
			if !reconcileSafeBySource[source] {
				continue
			}
			if err := reconciler.Reconcile(ctx, source, keep); err != nil {
				return nil, fmt.Errorf("reconcile virtual source %q: %w", source, err)
			}
		}
	}
	out, _ := structpb.NewStruct(map[string]any{"media_checked": len(items), "ready": ready, "pending": pending})
	return &pb.RunScheduledTaskResponse{Output: out}, nil
}

func (m *mediaMonitor) fetchCinemeta(ctx context.Context, item monitoredMedia) (monitoredMedia, error) {
	if item.IMDbID == "" {
		// Try to resolve IMDb ID from TMDB when available.
		if item.TMDBID != "" && m.config.TMDBAPIKey != "" {
			if ids, err := fetchTMDBExternalIDs(ctx, item.MediaType, item.TMDBID, m.config.TMDBAPIKey); err == nil && ids.IMDbID != "" {
				item.IMDbID = ids.IMDbID
			}
		}
	}
	if item.IMDbID == "" {
		return item, errors.New("IMDb ID required for Cinemeta metadata")
	}
	endpoint := strings.TrimRight(cinemetaBaseURL, "/") + "/meta/" + item.MediaType + "/" + url.PathEscape(item.IMDbID) + ".json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return item, err
	}
	resp, err := metadataClient.Do(req)
	if err != nil {
		return item, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return item, fmt.Errorf("Cinemeta HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Meta struct {
			Name, Description, Poster, Background string
			Runtime                               string `json:"runtime"`
			Genres                                []string
			Videos                                []struct {
				ID, Title, Overview, Thumbnail string
				Season, Episode                int
				Released                       time.Time
			} `json:"videos"`
		} `json:"meta"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return item, err
	}
	if payload.Meta.Name != "" {
		item.Title = payload.Meta.Name
	}
	item.Overview, item.Poster, item.Backdrop, item.Genres = payload.Meta.Description, payload.Meta.Poster, payload.Meta.Background, payload.Meta.Genres
	if runtime := parseRuntimeMinutes(payload.Meta.Runtime); runtime > 0 {
		item.Runtime = runtime
	}
	freshEpisodes := make([]virtualEpisode, 0, len(payload.Meta.Videos))
	for _, video := range payload.Meta.Videos {
		freshEpisodes = append(freshEpisodes, virtualEpisode{Season: video.Season, Episode: video.Episode, Title: video.Title, Overview: video.Overview, Thumbnail: video.Thumbnail, Released: video.Released})
	}
	item.Episodes = mergeSeriesEpisodeMetadata(item.Episodes, freshEpisodes)
	return item, nil
}

var htmlTagPattern = regexp.MustCompile(`<[^>]*>`)
var runtimeHourPattern = regexp.MustCompile(`(?i)(\d+)\s*h`)
var runtimeMinutePattern = regexp.MustCompile(`(?i)(\d+)\s*m`)
var runtimeNumberPattern = regexp.MustCompile(`\d+`)

func parseRuntimeMinutes(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	hours, minutes := 0, 0
	if match := runtimeHourPattern.FindStringSubmatch(value); len(match) == 2 {
		hours, _ = strconv.Atoi(match[1])
	}
	if match := runtimeMinutePattern.FindStringSubmatch(value); len(match) == 2 {
		minutes, _ = strconv.Atoi(match[1])
	}
	if hours > 0 || minutes > 0 {
		return hours*60 + minutes
	}
	if match := runtimeNumberPattern.FindString(value); match != "" {
		minutes, _ = strconv.Atoi(match)
	}
	return minutes
}

func cleanTVMazeSummary(value string) string {
	return strings.TrimSpace(html.UnescapeString(htmlTagPattern.ReplaceAllString(value, "")))
}

func (m *mediaMonitor) fetchTVMaze(ctx context.Context, item monitoredMedia) (monitoredMedia, error) {
	if item.IMDbID == "" && item.TVDBID == "" && item.TMDBID != "" {
		m.mu.Lock()
		apiKey := m.config.TMDBAPIKey
		m.mu.Unlock()
		if apiKey != "" {
			if externalIDs, err := fetchTMDBExternalIDs(ctx, item.MediaType, item.TMDBID, apiKey); err == nil {
				if externalIDs.IMDbID != "" {
					item.IMDbID = externalIDs.IMDbID
				}
				if externalIDs.TVDBID > 0 {
					item.TVDBID = strconv.Itoa(externalIDs.TVDBID)
				}
			}
		}
	}
	lookup, err := url.Parse(strings.TrimRight(tvmazeBaseURL, "/") + "/lookup/shows")
	if err != nil {
		return item, err
	}
	query := lookup.Query()
	if item.IMDbID != "" {
		query.Set("imdb", item.IMDbID)
	} else if item.TVDBID != "" {
		query.Set("thetvdb", item.TVDBID)
	} else if item.Title != "" {
		lookup, _ = url.Parse(strings.TrimRight(tvmazeBaseURL, "/") + "/singlesearch/shows")
		query = lookup.Query()
		query.Set("q", item.Title)
	} else {
		return item, errors.New("IMDb, TVDB, or Title required for TVMaze")
	}
	lookup.RawQuery = query.Encode()
	client := metadataClient
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, lookup.String(), nil)
	resp, err := client.Do(req)
	if err != nil {
		return item, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return item, fmt.Errorf("TVMaze lookup HTTP %d", resp.StatusCode)
	}
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return item, err
	}
	var show struct {
		ID            int `json:"id"`
		Name, Summary string
		Genres        []string
		Premiered     string
		Image         struct{ Medium, Original string }
	}
	if err := json.Unmarshal(bodyBytes, &show); err != nil || show.ID <= 0 {
		var wrapped struct {
			Show struct {
				ID            int `json:"id"`
				Name, Summary string
				Genres        []string
				Premiered     string
				Image         struct{ Medium, Original string }
			} `json:"show"`
		}
		if err := json.Unmarshal(bodyBytes, &wrapped); err == nil {
			show = wrapped.Show
		}
	}
	if show.ID <= 0 {
		return item, errors.New("TVMaze returned no show ID")
	}
	episodesURL := fmt.Sprintf("%s/shows/%d/episodes", strings.TrimRight(tvmazeBaseURL, "/"), show.ID)
	episodeReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, episodesURL, nil)
	episodeResp, err := client.Do(episodeReq)
	if err != nil {
		return item, err
	}
	defer episodeResp.Body.Close()
	if episodeResp.StatusCode != http.StatusOK {
		return item, fmt.Errorf("TVMaze episodes HTTP %d", episodeResp.StatusCode)
	}
	var episodes []struct {
		Name, Summary, Airdate, Airstamp string
		Season, Number                   int
		Runtime                          int
		Image                            struct{ Medium, Original string }
	}
	if err := json.NewDecoder(io.LimitReader(episodeResp.Body, maxResponseBytes)).Decode(&episodes); err != nil {
		return item, err
	}
	if show.Name != "" {
		item.Title = show.Name
	}
	item.Overview, item.Genres = cleanTVMazeSummary(show.Summary), show.Genres
	if show.Image.Original != "" {
		item.Poster = show.Image.Original
	} else {
		item.Poster = show.Image.Medium
	}
	freshEpisodes := make([]virtualEpisode, 0, len(episodes))
	for _, episode := range episodes {
		released, parseErr := time.Parse(time.RFC3339, episode.Airstamp)
		if parseErr != nil && episode.Airdate != "" {
			released, _ = time.Parse("2006-01-02", episode.Airdate)
		}
		thumbnail := episode.Image.Original
		if thumbnail == "" {
			thumbnail = episode.Image.Medium
		}
		freshEpisodes = append(freshEpisodes, virtualEpisode{Season: episode.Season, Episode: episode.Number, Runtime: episode.Runtime, Title: episode.Name, Overview: cleanTVMazeSummary(episode.Summary), Thumbnail: thumbnail, Released: released})
	}
	item.Episodes = mergeSeriesEpisodeMetadata(item.Episodes, freshEpisodes)
	return item, nil
}

type tmdbReleaseDates struct {
	Results []struct {
		Country string `json:"iso_3166_1"`
		Dates   []struct {
			Date time.Time `json:"release_date"`
			Type int       `json:"type"`
		} `json:"release_dates"`
	} `json:"results"`
}

func (m *mediaMonitor) movieRelease(ctx context.Context, item monitoredMedia) (time.Time, error) {
	m.mu.Lock()
	cfg := m.config
	m.mu.Unlock()
	if cfg.TMDBAPIKey != "" && item.TMDBID != "" {
		release, err := fetchTMDBRelease(ctx, item.TMDBID, cfg.TMDBAPIKey)
		if err == nil {
			return release, nil
		}
	}
	if item.IMDbID == "" && item.TMDBID != "" && cfg.TMDBAPIKey != "" {
		if ext, err := fetchTMDBExternalIDs(ctx, item.MediaType, item.TMDBID, cfg.TMDBAPIKey); err == nil && ext.IMDbID != "" {
			item.IMDbID = ext.IMDbID
		}
	}
	if item.IMDbID == "" {
		return time.Time{}, errors.New("IMDb ID required for Cinemeta fallback")
	}
	endpoint := strings.TrimRight(cinemetaBaseURL, "/") + "/meta/movie/" + url.PathEscape(item.IMDbID) + ".json"
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	client := metadataClient
	resp, err := client.Do(request)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return time.Time{}, fmt.Errorf("Cinemeta HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Meta struct {
			Released    time.Time `json:"released"`
			ReleaseInfo string    `json:"releaseInfo"`
			Year        string    `json:"year"`
		} `json:"meta"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return time.Time{}, err
	}
	if !payload.Meta.Released.IsZero() {
		// Cinemeta exposes the theatrical/premiere date, not a verified home
		// release. Do not let a newly opened theatrical title bypass the gate.
		// For older catalog titles, a conservative 90-day window keeps the
		// no-TMDB fallback useful without claiming day-one home availability.
		presumedHomeRelease := payload.Meta.Released.AddDate(0, 0, 90)
		if presumedHomeRelease.After(time.Now()) {
			return time.Time{}, errNoHomeRelease
		}
		return presumedHomeRelease, nil
	}
	for _, v := range []string{payload.Meta.ReleaseInfo, payload.Meta.Year} {
		if len(strings.TrimSpace(v)) >= 4 {
			if y, e := strconv.Atoi(strings.TrimSpace(v)[:4]); e == nil {
				if y < time.Now().Year() {
					// Previous catalog years can be presumed released on Jan 1 of that past year.
					return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC), nil
				}
				// Current-year or future-year titles without an explicit home release date
				// must remain gated as theatrical/unreleased until verified.
				return time.Time{}, errNoHomeRelease
			}
		}
	}
	if item.Year > 0 && int(item.Year) < time.Now().Year() {
		return time.Date(int(item.Year), 1, 1, 0, 0, 0, 0, time.UTC), nil
	}
	return time.Time{}, errNoHomeRelease
}

func (m *mediaMonitor) fetchTMDBMovieRuntime(ctx context.Context, item monitoredMedia) (int, error) {
	m.mu.Lock()
	key := m.config.TMDBAPIKey
	m.mu.Unlock()
	if key == "" || item.TMDBID == "" {
		return 0, nil
	}
	endpoint := strings.TrimRight(tmdbBaseURL, "/") + "/movie/" + url.PathEscape(item.TMDBID)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if strings.Count(key, ".") == 2 {
		req.Header.Set("Authorization", "Bearer "+key)
	} else {
		query := req.URL.Query()
		query.Set("api_key", key)
		req.URL.RawQuery = query.Encode()
	}
	resp, err := metadataClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("TMDB movie details HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Runtime int `json:"runtime"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&payload); err != nil {
		return 0, err
	}
	return payload.Runtime, nil
}
func fetchTMDBRelease(ctx context.Context, id, key string) (time.Time, error) {
	endpoint := strings.TrimRight(tmdbBaseURL, "/") + "/movie/" + url.PathEscape(id) + "/release_dates"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if strings.Count(key, ".") == 2 {
		req.Header.Set("Authorization", "Bearer "+key)
	} else {
		q := req.URL.Query()
		q.Set("api_key", key)
		req.URL.RawQuery = q.Encode()
	}
	resp, err := metadataClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return time.Time{}, fmt.Errorf("TMDB HTTP %d", resp.StatusCode)
	}
	var data tmdbReleaseDates
	if err = json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&data); err != nil {
		return time.Time{}, err
	}
	results := data.Results
	// TMDB release types:
	// Type 4: Digital (VOD, iTunes)
	// Type 5: Physical (Blu-ray, DVD)
	// Type 6: TV / Streaming Originals (Apple TV+, Netflix, Disney+)
	for _, typ := range []int{4, 5, 6} {
		var earliest time.Time
		for _, r := range results {
			for _, d := range r.Dates {
				if d.Type == typ && !d.Date.IsZero() && (earliest.IsZero() || d.Date.Before(earliest)) {
					earliest = d.Date
				}
			}
		}
		if !earliest.IsZero() {
			return earliest, nil
		}
	}
	// No Digital/Physical/TV date on record means the movie is still
	// theatrical-only: keep it queued. The earlier "presume home release 90
	// days after the earliest date" fallback admitted premiere and theatrical
	// dates into that calculation, registering titles that were still only in
	// theaters. Indexer RSS / Prowlarr confirmation remains the escape hatch
	// for titles whose home release is real but not yet on TMDB.
	return time.Time{}, errNoHomeRelease
}

type tmdbExternalIDs struct {
	IMDbID string `json:"imdb_id"`
	TVDBID int    `json:"tvdb_id"`
}

func fetchTMDBExternalIDs(ctx context.Context, mediaType, tmdbID, key string) (tmdbExternalIDs, error) {
	kind := "tv"
	if mediaType == "movie" {
		kind = "movie"
	}
	endpoint := strings.TrimRight(tmdbBaseURL, "/") + "/" + kind + "/" + url.PathEscape(tmdbID) + "/external_ids"
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if strings.Count(key, ".") == 2 {
		req.Header.Set("Authorization", "Bearer "+key)
	} else {
		q := req.URL.Query()
		q.Set("api_key", key)
		req.URL.RawQuery = q.Encode()
	}
	resp, err := metadataClient.Do(req)
	if err != nil {
		return tmdbExternalIDs{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return tmdbExternalIDs{}, fmt.Errorf("TMDB external_ids HTTP %d", resp.StatusCode)
	}
	var out tmdbExternalIDs
	if err = json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil {
		return tmdbExternalIDs{}, err
	}
	return out, nil
}
