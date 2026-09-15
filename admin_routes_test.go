package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/drondeseries/vio-virtual-library/pkg/release"
	"github.com/hashicorp/go-hclog"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestQueueJSONShowsOnlyMissingMoviesAndEpisodes(t *testing.T) {
	now := time.Now()
	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	monitor.items = map[string]monitoredMedia{
		"movie:missing": {Key: "movie:missing", MediaType: "movie", Title: "Missing Movie"},
		"movie:ready":   {Key: "movie:ready", MediaType: "movie", Title: "Ready Movie", Ready: true},
		"series:ongoing": {
			Key: "series:ongoing", MediaType: "series", Title: "Ongoing Show", Ready: true,
			Episodes: []virtualEpisode{
				{Season: 1, Episode: 1, Title: "Available", Released: now.Add(-2 * time.Hour), Available: true},
				{Season: 1, Episode: 2, Title: "Missing", Released: now.Add(-time.Hour)},
				{Season: 1, Episode: 3, Title: "Future", Released: now.Add(time.Hour)},
			},
		},
		"series:complete": {
			Key: "series:complete", MediaType: "series", Title: "Complete Show", Ready: true,
			Episodes: []virtualEpisode{{Season: 1, Episode: 1, Released: now.Add(-time.Hour), Available: true}},
		},
	}

	response, err := (&adminRoutes{server: &runtimeServer{monitor: monitor}}).queueJSON()
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Items []monitoredMedia `json:"items"`
	}
	if err := json.Unmarshal(response.Body, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 2 {
		t.Fatalf("items = %#v, want missing movie and ongoing show", payload.Items)
	}
	if payload.Items[0].Key != "movie:missing" || payload.Items[1].Key != "series:ongoing" {
		t.Fatalf("item keys = %q, %q", payload.Items[0].Key, payload.Items[1].Key)
	}
	if len(payload.Items[1].Episodes) != 2 || payload.Items[1].Episodes[0].Episode != 2 || payload.Items[1].Episodes[1].Episode != 3 {
		t.Fatalf("missing episodes = %#v, want S01E02 and S01E03", payload.Items[1].Episodes)
	}
}

func TestAdminScheduleJSONAndRefresh(t *testing.T) {
	store := release.NewReleaseStore()
	store.SetShow("tt1234567", &release.ShowSchedule{
		IMDBID: "tt1234567",
		Status: "Running",
		Episodes: map[string]release.EpisodeInfo{
			"1:1": {Season: 1, Episode: 1, Title: "Pilot", AirDate: time.Now().Add(-24 * time.Hour)},
		},
	})

	server := &runtimeServer{
		releaseStore: store,
		scheduler:    release.NewScheduler(store, release.NewMetadataClient(nil), time.Hour),
	}
	admin := newAdminRoutes(server)

	// Test schedule GET
	getReq := &pb.HandleHTTPRequest{
		Path:    "/admin/virtual-library/schedule",
		Method:  "GET",
		Headers: map[string]string{"X-Vio-User-Role": "admin"},
	}
	resp, err := admin.Handle(context.Background(), getReq)
	if err != nil {
		t.Fatalf("Handle schedule GET error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("schedule GET statusCode = %d, want 200", resp.StatusCode)
	}
	var schedulePayload struct {
		Count int                     `json:"count"`
		Shows []*release.ShowSchedule `json:"shows"`
	}
	if err := json.Unmarshal(resp.Body, &schedulePayload); err != nil {
		t.Fatalf("unmarshal schedule response: %v", err)
	}
	if schedulePayload.Count != 1 || len(schedulePayload.Shows) != 1 || schedulePayload.Shows[0].IMDBID != "tt1234567" {
		t.Fatalf("unexpected schedule payload: %#v", schedulePayload)
	}

	// Test forbidden for non-admin
	nonAdminReq := &pb.HandleHTTPRequest{
		Path:   "/admin/virtual-library/schedule",
		Method: "GET",
	}
	forbiddenResp, err := admin.Handle(context.Background(), nonAdminReq)
	if err != nil || forbiddenResp.StatusCode != 403 {
		t.Fatalf("expected 403 for non-admin, got resp=%v, err=%v", forbiddenResp, err)
	}
}

func TestSearchQueueItemSortsReleasesByCustomFormats(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		releases := []prowlarrRelease{
			{Title: "Inception.2010.1080p.BluRay.German.Dubbed.x264", Indexer: "altHUB"},
			{Title: "Inception.2010.1080p.BluRay.Preferred.x264", Indexer: "altHUB"},
			{Title: "Inception.2010.1080p.BluRay.x264", Indexer: "altHUB"},
		}
		json.NewEncoder(w).Encode(releases)
	}))
	defer srv.Close()

	prowlarr := newProwlarrSearchClient(srv.Client())
	prowlarr.Configure(srv.URL, "key", 15)

	formats := []CustomFormat{
		{Name: "Preferred", Regex: `(?i)\bPreferred\b`, Score: 600},
		{Name: "Penalty", Regex: `(?i)\bGerman\.Dubbed\b`, Score: -650},
	}
	tempDir := t.TempDir()
	monitor := newMediaMonitor(nil, hclog.NewNullLogger())
	monitor.setRegistrar(registrarFunc(func(context.Context, monitoredMedia) error { return nil }))
	monitor.prowlarr = prowlarr
	monitor.config = monitorConfig{
		File:              filepath.Join(tempDir, "monitor.json"),
		ProwlarrIndexFile: filepath.Join(tempDir, "prowlarr.json"),
		Quality: QualityConfig{
			CustomFormats: formats,
		},
	}
	monitor.items = map[string]monitoredMedia{
		"movie:inception": {Key: "movie:inception", MediaType: "movie", Title: "Inception", Year: 2010},
	}

	server := &runtimeServer{monitor: monitor}
	admin := newAdminRoutes(server)

	req := &pb.HandleHTTPRequest{
		Path:    "/admin/virtual-library/queue/search",
		Method:  "POST",
		Headers: map[string]string{"X-Vio-User-Role": "admin"},
		Query: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"key": structpb.NewStringValue("movie:inception"),
			},
		},
	}

	resp, err := admin.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle search error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(resp.Body))
	}

	var result struct {
		Matched  bool                    `json:"matched"`
		Count    int                     `json:"count"`
		Releases []prowlarrReleaseResult `json:"releases"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(result.Releases) != 3 {
		t.Fatalf("got %d releases, want 3", len(result.Releases))
	}
	if result.Releases[0].Title != "Inception.2010.1080p.BluRay.Preferred.x264" || result.Releases[0].Score != 600 {
		t.Fatalf("expected first release to be Preferred (score 600), got %+v", result.Releases[0])
	}
	if result.Releases[2].Title != "Inception.2010.1080p.BluRay.German.Dubbed.x264" || result.Releases[2].Score != -650 {
		t.Fatalf("expected last release to be German dub (score -650), got %+v", result.Releases[2])
	}
}
