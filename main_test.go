package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	publicmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtimehost"
	"github.com/drondeseries/vio-virtual-library/pkg/release"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestManifestStreamResolverResolvesFirstHTTPStream(t *testing.T) {
	var gotPath string
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.Path
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"url": "magnet:?xt=urn:btih:ignored"},
			{"url": "https://stream.example/movie.mkv?token=secret"},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}

	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/configured/manifest.json"})
	streamURL, err := resolver.Resolve(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if streamURL != "https://stream.example/movie.mkv?token=secret" {
		t.Fatalf("Resolve() = %q", streamURL)
	}
	if gotPath != "/configured/stream/movie/tt0133093.json" {
		t.Fatalf("request path = %q", gotPath)
	}
}

func TestGetCandidatesFreshBypassesCache(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{{
			"url": fmt.Sprintf("https://stream.example/movie.mkv?token=%d", calls),
		}}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json"})
	first, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("GetCandidates() error = %v", err)
	}
	cached, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("cached GetCandidates() error = %v", err)
	}
	// A forced lookup still serves entries younger than freshServeFloor so a
	// single playback start cannot stack several provider round-trips.
	freshWithinFloor, _, _, err := resolver.GetCandidatesFresh(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("GetCandidatesFresh() within floor error = %v", err)
	}
	if calls != 1 || first[0].URL != cached[0].URL || first[0].URL != freshWithinFloor[0].URL {
		t.Fatalf("within floor: calls=%d first=%q cached=%q fresh=%q", calls, first[0].URL, cached[0].URL, freshWithinFloor[0].URL)
	}

	// Past the floor the forced lookup takes the provider path again.
	backdateFetchedAt(t, resolver, "movie|tt0133093", freshServeFloor+time.Second)
	fresh, _, _, err := resolver.GetCandidatesFresh(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("GetCandidatesFresh() past floor error = %v", err)
	}
	if calls != 2 || first[0].URL == fresh[0].URL {
		t.Fatalf("past floor: calls=%d first=%q fresh=%q", calls, first[0].URL, fresh[0].URL)
	}
}

// backdateFetchedAt ages a cache entry's fetch time without touching its TTL,
// simulating an entry old enough that forced lookups must refetch.
func backdateFetchedAt(t *testing.T, resolver *manifestStreamResolver, cacheKey string, age time.Duration) {
	t.Helper()
	resolver.cacheMu.Lock()
	defer resolver.cacheMu.Unlock()
	entry, ok := resolver.cache[cacheKey]
	if !ok {
		t.Fatalf("cache key %q not present", cacheKey)
	}
	entry.fetchedAt = entry.fetchedAt.Add(-age)
	resolver.cache[cacheKey] = entry
}

// An empty provider answer must be cached: a title the provider cannot serve
// otherwise re-paid a full round-trip on every resolve — four inside one
// playback start in production.
func TestNegativeCacheServesEmptyResultsWithoutRefetching(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"streams":[]}`)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json"})

	for range 3 {
		candidates, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0000000")
		if err != nil {
			t.Fatalf("GetCandidates() error = %v", err)
		}
		if len(candidates) != 0 {
			t.Fatalf("candidates = %d, want empty", len(candidates))
		}
		fresh, _, _, err := resolver.GetCandidatesFresh(context.Background(), "virtual://movie/tt0000000")
		if err != nil {
			t.Fatalf("GetCandidatesFresh() error = %v", err)
		}
		if len(fresh) != 0 {
			t.Fatalf("fresh candidates = %d, want empty", len(fresh))
		}
	}
	if calls != 1 {
		t.Fatalf("provider calls = %d, want 1 (negative cache must absorb repeats)", calls)
	}

	// Once the negative TTL lapses the plugin re-checks upstream so newly
	// added sources are noticed without operator action.
	negativeKey := "movie|tt0000000"
	resolver.cacheMu.Lock()
	entry := resolver.cache[negativeKey]
	entry.fetchedAt = entry.fetchedAt.Add(-negativeCacheTTL - time.Second)
	entry.expiresAt = entry.expiresAt.Add(-negativeCacheTTL - time.Second)
	resolver.cache[negativeKey] = entry
	resolver.cacheMu.Unlock()

	if _, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0000000"); err != nil {
		t.Fatalf("GetCandidates() after negative expiry error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("provider calls after expiry = %d, want 2", calls)
	}
}

// The upstream provider flaps between a full list and an empty answer. An
// empty answer must not evict a still-servable positive entry: doing so makes
// every resolve inside the negative TTL report "no streams" even though a good
// list was cached moments earlier.
func TestEmptyAnswerKeepsFreshPositiveCandidateCache(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.mu.RLock()
	generation := resolver.generation
	resolver.mu.RUnlock()
	key := "movie|tt3000001"
	now := time.Now()
	resolver.storeCandidateCache(key, []StreamCandidate{{Name: "good-1080p", URL: "https://provider.example/a.mkv"}},
		now.Add(time.Hour), now, generation)

	resolver.storeCandidateCache(key, nil, now.Add(time.Hour), now.Add(time.Second), generation)

	resolver.cacheMu.Lock()
	entry, ok := resolver.cache[key]
	resolver.cacheMu.Unlock()
	if !ok {
		t.Fatal("positive cache entry disappeared after an empty answer")
	}
	if len(entry.candidates) != 1 || entry.candidates[0].Name != "good-1080p" {
		t.Fatalf("empty answer replaced the positive entry: %#v", entry.candidates)
	}
	if !entry.expiresAt.After(now) {
		t.Fatalf("positive entry expiry = %v, want a live TTL", entry.expiresAt)
	}
}

// Once the positive entry leaves stale grace it is no longer servable, so the
// empty answer installs the short negative entry as before.
func TestEmptyAnswerReplacesPositiveCachePastStaleGrace(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.mu.RLock()
	generation := resolver.generation
	resolver.mu.RUnlock()
	key := "movie|tt3000002"
	now := time.Now()
	resolver.storeCandidateCache(key, []StreamCandidate{{Name: "ancient", URL: "https://provider.example/old.mkv"}},
		now.Add(-candidateStaleGrace-time.Minute), now.Add(-time.Hour), generation)

	resolver.storeCandidateCache(key, nil, now.Add(time.Hour), now, generation)

	resolver.cacheMu.Lock()
	entry, ok := resolver.cache[key]
	resolver.cacheMu.Unlock()
	if !ok {
		t.Fatal("negative cache entry missing")
	}
	if len(entry.candidates) != 0 {
		t.Fatalf("past-grace positive entry not replaced: %#v", entry.candidates)
	}
	if got, want := entry.expiresAt.Sub(now), negativeCacheTTL; got != want {
		t.Fatalf("negative expiry = %v, want %v", got, want)
	}
}

func TestConfigureDiscardsInFlightCandidateCacheStore(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		if call == 1 {
			close(started)
			<-release
		}
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{{
			"url": "https://stream.example/" + r.URL.Host + ".mkv",
		}}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}
	resolver := &manifestStreamResolver{client: client}
	resolver.Configure(resolverConfig{ManifestURL: "https://old.example/manifest.json"})
	done := make(chan error, 1)
	go func() {
		_, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0133093")
		done <- err
	}()
	<-started
	resolver.Configure(resolverConfig{ManifestURL: "https://new.example/manifest.json"})
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight GetCandidates() error = %v", err)
	}
	candidates, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt0133093")
	if err != nil {
		t.Fatalf("GetCandidates() after Configure error = %v", err)
	}
	if calls.Load() < 2 || len(candidates) != 1 || !strings.Contains(candidates[0].URL, "new.example") {
		t.Fatalf("calls=%d candidates=%#v, want a fresh new-provider response", calls.Load(), candidates)
	}
}

func TestCandidateCacheEnforcesAggregateByteLimit(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{})
	resolver.mu.RLock()
	generation := resolver.generation
	resolver.mu.RUnlock()
	now := time.Now()
	for i := 0; i < 32; i++ {
		resolver.storeCandidateCache(fmt.Sprintf("movie|tt%d", i), []StreamCandidate{{
			URL:  fmt.Sprintf("https://stream.example/%d.mkv", i),
			Name: strings.Repeat("x", 1<<20),
		}}, now.Add(time.Hour), now.Add(time.Duration(i)*time.Second), generation)
	}
	resolver.cacheMu.Lock()
	defer resolver.cacheMu.Unlock()
	if resolver.cacheBytes > maxCandidateCacheBytes {
		t.Fatalf("cache bytes = %d, limit = %d", resolver.cacheBytes, maxCandidateCacheBytes)
	}
	if len(resolver.cache) >= 32 {
		t.Fatalf("cache retained %d oversized aggregate entries", len(resolver.cache))
	}
}

func TestValidateConnectionRequiresStremioStreamManifest(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "valid string resource", body: `{"id":"provider","resources":["stream"],"types":["movie","series"]}`},
		{name: "valid object resource", body: `{"id":"provider","resources":[{"name":"stream"}],"types":["movie"]}`},
		{name: "html", body: `<html>not a manifest</html>`, wantErr: true},
		{name: "missing stream", body: `{"id":"provider","resources":["catalog"],"types":["movie"]}`, wantErr: true},
		{name: "unsupported types", body: `{"id":"provider","resources":["stream"],"types":["music"]}`, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &manifestStreamResolver{client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(test.body)), Header: make(http.Header)}, nil
			})}}
			resolver.Configure(resolverConfig{ManifestURL: "https://provider.example/manifest.json"})
			err := resolver.ValidateConnection(context.Background())
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateConnection() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestEmbeddedManifestUsesTypedVirtualStreamDescriptor(t *testing.T) {
	manifest, err := publicmanifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("Load(manifestJSON) error = %v", err)
	}
	for _, descriptor := range manifest.GetCapabilities() {
		if descriptor.GetType() == "virtual_stream_provider.v1" {
			if descriptor.GetVirtualStreamProvider() == nil {
				t.Fatal("virtual stream provider is missing its typed descriptor")
			}
			return
		}
	}
	t.Fatal("virtual stream provider capability is missing")
}

func TestRuntimeConfigureDoesNotPartiallyApplyOnFailure(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{ManifestURL: "https://old.example/manifest.json"})
	monitor := newMediaMonitor(resolver, nil)
	server := &runtimeServer{resolver: resolver, monitor: monitor}
	value, err := structpb.NewStruct(map[string]any{
		"manifest_url":      "https://new.example/manifest.json",
		"movie_library_id":  1,
		"series_library_id": 2,
		"monitor_file":      t.TempDir() + "/queue.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.Configure(context.Background(), &pb.ConfigureRequest{Config: []*pb.ConfigEntry{{Key: configKey, Value: value}}})
	if err == nil {
		t.Fatal("Configure() succeeded without a bound RuntimeHost")
	}
	resolver.mu.RLock()
	got := resolver.config.ManifestURL
	resolver.mu.RUnlock()
	if got != "https://old.example/manifest.json" {
		t.Fatalf("resolver config = %q after rejected Configure, want old config", got)
	}
}

func TestParseVirtualPath(t *testing.T) {
	tests := []struct {
		path, mediaType, mediaID string
		wantErr                  bool
	}{
		{"virtual://movie/tt0133093", "movie", "tt0133093", false},
		{"virtual://series/tt0944947/1/2", "series", "tt0944947:1:2", false},
		{"virtual://anime/kitsu/12/1", "anime", "kitsu:12:1", false},
		{"/movie/tt0133093", "", "", true},
		{"virtual://book/1", "", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			mediaType, mediaID, err := parseVirtualPath(tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if mediaType != tc.mediaType || mediaID != tc.mediaID {
				t.Fatalf("got (%q, %q), want (%q, %q)", mediaType, mediaID, tc.mediaType, tc.mediaID)
			}
		})
	}
}

func TestVirtualPathMetadataPreservesSelectionAndBindsIdentity(t *testing.T) {
	request := &pb.ResolveVirtualStreamRequest{MediaType: "movie", ExternalIds: map[string]string{"imdb": "tt0133093"}}
	metadata, err := structpb.NewStruct(map[string]any{"virtual_uri": "virtual://movie/tt0133093?profile=1080p&results=all"})
	if err != nil {
		t.Fatal(err)
	}
	request.Metadata = metadata
	path, err := virtualPathForResolveRequest(request)
	if err != nil || path != "virtual://movie/tt0133093?profile=1080p&results=all" {
		t.Fatalf("path=%q err=%v", path, err)
	}
	metadata, _ = structpb.NewStruct(map[string]any{"virtual_uri": "virtual://movie/tt9999999?results=all"})
	request.Metadata = metadata
	if _, err := virtualPathForResolveRequest(request); err == nil {
		t.Fatal("mismatched metadata virtual_uri accepted")
	}
	metadata, _ = structpb.NewStruct(map[string]any{"virtual_uri": "not-a-virtual-uri"})
	request.Metadata = metadata
	if _, err := virtualPathForResolveRequest(request); err == nil {
		t.Fatal("malformed metadata virtual_uri accepted")
	}
}

func TestStreamEndpointAllowsPrivateHTTPOnlyWhenOptedIn(t *testing.T) {
	if _, err := streamEndpoint("http://streaming:8080/token/manifest.json", "movie", "tt0133093"); err == nil {
		t.Fatal("streamEndpoint accepted HTTP without explicit opt-in")
	}
	endpoint, err := streamEndpointWithPolicy("http://streaming:8080/token/manifest.json", "movie", "tt0133093", true)
	if err != nil {
		t.Fatalf("private HTTP endpoint rejected: %v", err)
	}
	if endpoint != "http://streaming:8080/token/stream/movie/tt0133093.json" {
		t.Fatalf("endpoint = %q", endpoint)
	}
	if _, err := streamEndpointWithPolicy("http://altmount:8080/token/manifest.json", "movie", "tt0133093", true); err != nil {
		t.Fatalf("single-label service endpoint rejected: %v", err)
	}
	if _, err := streamEndpointWithPolicy("http://public.example/token/manifest.json", "movie", "tt0133093", true); err == nil {
		t.Fatal("HTTP public endpoint accepted")
	}
}

type resolverFunc func(context.Context, string) (string, error)

func (f resolverFunc) Resolve(ctx context.Context, path string) (string, error) { return f(ctx, path) }
func (f resolverFunc) GetVariants(ctx context.Context, path string) []runtimehost.VirtualMediaVariant {
	return nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestQualityProfiles(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := json.Marshal(map[string]any{"streams": []map[string]string{
			{"url": "https://stream.example/movie1.mkv", "title": "720p HDTV"},
			{"url": "https://stream.example/movie2.mkv", "title": "1080p WEB-DL"},
			{"url": "https://stream.example/movie3.mkv", "title": "2160p REMUX Dolby Vision", "name": "4K Stream"},
			{"url": "https://stream.example/movie4.mkv", "title": "2160p HDR10 WEB-DL"},
		}})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
	})}

	resolver := &manifestStreamResolver{client: client}

	qc := QualityConfig{
		EnableProfiles: true,
		Profiles: []QualityProfile{
			{Label: "4K DV", Resolution: "2160p", HDR: "dv"},
			{Label: "1080p", Resolution: "1080p"},
			{Label: "Exclude Remux", ExcludeRegex: "(?i)remux"},
			{Label: "Include HDR10", IncludeRegex: "(?i)hdr10"},
		},
		FallbackToAnyStream: true,
	}
	qc.Validate()

	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json", Quality: qc})

	ctx := context.Background()

	url1, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=4K+DV")
	if err != nil || url1 != "https://stream.example/movie3.mkv" {
		t.Fatalf("Expected movie3.mkv, got %v %v", url1, err)
	}

	url2, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=1080p")
	if err != nil || url2 != "https://stream.example/movie2.mkv" {
		t.Fatalf("Expected movie2.mkv, got %v %v", url2, err)
	}

	url3, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=Exclude+Remux")
	if err != nil || url3 != "https://stream.example/movie4.mkv" {
		t.Fatalf("Expected movie4.mkv (best without remux), got %v %v", url3, err)
	}

	url4, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=Include+HDR10")
	if err != nil || url4 != "https://stream.example/movie4.mkv" {
		t.Fatalf("Expected movie4.mkv, got %v %v", url4, err)
	}

	url5, err := resolver.Resolve(ctx, "virtual://movie/tt0133093")
	if err != nil || url5 != "https://stream.example/movie3.mkv" {
		t.Fatalf("Expected ranked movie3.mkv (fallback any stream), got %v %v", url5, err)
	}

	variants := resolver.GetVariants(ctx, "virtual://movie/tt0133093")
	if len(variants) != 4 {
		t.Fatalf("Expected one version per profile, got %d", len(variants))
	}
	if strings.Contains(variants[0].VirtualURI, "result=") {
		t.Fatalf("Expected provider-neutral variant URI: %s", variants[0].VirtualURI)
	}
	selected, err := resolver.Resolve(ctx, variants[0].VirtualURI)
	if err != nil || selected != "https://stream.example/movie3.mkv" {
		t.Fatalf("Expected variant to resolve movie3.mkv, got %v %v", selected, err)
	}

	qc.FallbackToAnyStream = false
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json", Quality: qc})
	_, err = resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=NonExistent")
	if err == nil {
		t.Fatalf("Expected error for non-existent profile when fallback is false")
	}

	qc.EnableProfiles = false
	resolver.Configure(resolverConfig{ManifestURL: "https://aio.example/manifest.json", Quality: qc})
	url6, err := resolver.Resolve(ctx, "virtual://movie/tt0133093?profile=4K+DV")
	if err != nil || url6 != "https://stream.example/movie3.mkv" {
		t.Fatalf("Expected ranked movie3.mkv when profiles are disabled, got %v %v", url6, err)
	}
}

func TestCustomFormatPresets(t *testing.T) {
	for _, preset := range []string{
		"english-original", "english-strict", "original-or-english",
		"clean-quality", "audio-hd", "repack-proper", "top-web-sources", "anime-enhanced",
		"web-tier-01", "web-tier-02", "remux-tier-01", "remux-tier-02", "trash-recommended",
	} {
		config := QualityConfig{CustomFormats: customFormatPresets(preset)}
		if len(config.CustomFormats) == 0 {
			t.Fatalf("expected custom format preset %s to return rules", preset)
		}
		if err := config.Validate(); err != nil {
			t.Fatalf("preset %s failed validation: %v", preset, err)
		}
	}
	strictConfig := QualityConfig{CustomFormats: customFormatPresets("english-strict")}
	_ = strictConfig.Validate()
	formats := strictConfig.CustomFormats
	english := StreamCandidate{Title: "1080p WEB-DL ENG"}
	german := StreamCandidate{Title: "2160p WEB-DL German"}
	if _, rejected := customFormatScore(english, formats); rejected {
		t.Fatal("English release was rejected")
	}
	if _, rejected := customFormatScore(german, formats); !rejected {
		t.Fatal("German release was not rejected")
	}
}

func TestCustomFormatsScoringAndSorting(t *testing.T) {
	formats := []CustomFormat{
		{Name: "Tier 1", Regex: `(?i)\bTIER1\b`, Score: 600},
		{Name: "Tier 2", Regex: `(?i)\bTIER2\b`, Score: 400},
		{Name: "Penalty", Regex: `(?i)\bPENALTY\b`, Score: -500},
		{Name: "Banned", Regex: `(?i)\bBANNED\b`, Reject: true},
	}
	config := QualityConfig{CustomFormats: formats}
	if err := config.Validate(); err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	candTier1 := StreamCandidate{Title: "Release.1080p.TIER1.x264"}
	candTier2 := StreamCandidate{Title: "Release.1080p.TIER2.x264"}
	candNeutral := StreamCandidate{Title: "Release.1080p.x264"}
	candPenalty := StreamCandidate{Title: "Release.1080p.PENALTY.x264"}
	candBanned := StreamCandidate{Title: "Release.1080p.BANNED.x264"}

	score1, rej1 := customFormatScore(candTier1, config.CustomFormats)
	if rej1 || score1 != 600 {
		t.Fatalf("Tier 1 score = %d, rej = %t, want 600, false", score1, rej1)
	}

	score2, rej2 := customFormatScore(candTier2, config.CustomFormats)
	if rej2 || score2 != 400 {
		t.Fatalf("Tier 2 score = %d, rej = %t, want 400, false", score2, rej2)
	}

	scoreN, rejN := customFormatScore(candNeutral, config.CustomFormats)
	if rejN || scoreN != 0 {
		t.Fatalf("Neutral score = %d, rej = %t, want 0, false", scoreN, rejN)
	}

	scoreP, rejP := customFormatScore(candPenalty, config.CustomFormats)
	if rejP || scoreP != -500 {
		t.Fatalf("Penalty score = %d, rej = %t, want -500, false", scoreP, rejP)
	}

	scoreB, rejB := customFormatScore(candBanned, config.CustomFormats)
	if !rejB {
		t.Fatalf("Banned score = %d, rej = %t, want rejected", scoreB, rejB)
	}

	candidates := []StreamCandidate{candPenalty, candNeutral, candTier2, candBanned, candTier1}
	sortCandidatesForProfile(candidates, QualityProfile{}, config.CustomFormats)
	if candidates[0].Title != candTier1.Title {
		t.Fatalf("top candidate = %q, want %q", candidates[0].Title, candTier1.Title)
	}
	if candidates[1].Title != candTier2.Title {
		t.Fatalf("second candidate = %q, want %q", candidates[1].Title, candTier2.Title)
	}
	lastIdx := len(candidates) - 1
	if candidates[lastIdx].Title != candBanned.Title {
		t.Fatalf("bottom candidate = %q, want %q (rejected)", candidates[lastIdx].Title, candBanned.Title)
	}
}

func TestCustomFormatsRankAndReject(t *testing.T) {
	formats := []CustomFormat{
		{Name: "English", Regex: `(?i)\b(?:eng|english)\b`, Score: 100},
		{Name: "German", Regex: `(?i)\b(?:deu|ger|german|deutsch)\b`, Reject: true},
	}
	if err := (&QualityConfig{CustomFormats: formats}).Validate(); err != nil {
		t.Fatal(err)
	}
	english := StreamCandidate{Title: "1080p WEB-DL ENG"}
	german := StreamCandidate{Title: "2160p WEB-DL German"}
	if score, rejected := customFormatScore(english, formats); rejected || score != 100 {
		t.Fatalf("english score = %d, rejected = %t", score, rejected)
	}
	if _, rejected := customFormatScore(german, formats); !rejected {
		t.Fatal("expected German release to be rejected")
	}
}

func TestCustomFormatsURLEncodingAndBehaviorHints(t *testing.T) {
	formats := []CustomFormat{
		{Name: "VOSTFR", Regex: `(?i)\bVOSTFR\b`, Score: 300},
	}
	candEncoded := StreamCandidate{URL: "https://provider.example/The%20Matrix%201999%20VOSTFR%201080p.mkv"}
	score, rej := customFormatScore(candEncoded, formats)
	if rej || score != 300 {
		t.Fatalf("candEncoded score = %d, rej = %t, want 300, false", score, rej)
	}

	var candHint StreamCandidate
	candHint.URL = "https://provider.example/stream/uuid-1234"
	candHint.BehaviorHints.Filename = "The.Matrix.1999.VOSTFR.1080p.mkv"
	scoreH, rejH := customFormatScore(candHint, formats)
	if rejH || scoreH != 300 {
		t.Fatalf("candHint score = %d, rej = %t, want 300, false", scoreH, rejH)
	}

	var candHintEncoded StreamCandidate
	candHintEncoded.URL = "https://provider.example/stream/uuid-1234"
	candHintEncoded.BehaviorHints.Filename = "The%20Matrix%201999%20VOSTFR%201080p.mkv"
	scoreHE, rejHE := customFormatScore(candHintEncoded, formats)
	if rejHE || scoreHE != 300 {
		t.Fatalf("candHintEncoded score = %d, rej = %t, want 300, false", scoreHE, rejHE)
	}
}

func TestSortCandidatesForProfileSingleCandidateQualityScore(t *testing.T) {
	formats := []CustomFormat{
		{Name: "Tier 1", Regex: `(?i)\bTIER1\b`, Score: 600},
	}
	candidates := []StreamCandidate{
		{Title: "Release.1080p.TIER1.x264"},
	}
	sortCandidatesForProfile(candidates, QualityProfile{}, formats)
	if candidates[0].QualityScore != 600 {
		t.Fatalf("single candidate QualityScore = %d, want 600", candidates[0].QualityScore)
	}
}

func TestQualityConfigValidation(t *testing.T) {
	qc := QualityConfig{
		EnableProfiles: true,
		Profiles: []QualityProfile{
			{Label: "P1", IncludeRegex: "["},
		},
	}
	err := qc.Validate()
	if err == nil {
		t.Fatalf("Expected error for invalid regex")
	}

	qc2 := QualityConfig{
		EnableProfiles: true,
		Profiles: []QualityProfile{
			{Label: "P1"}, {Label: "p1"},
		},
	}
	err2 := qc2.Validate()
	if err2 == nil {
		t.Fatalf("Expected error for duplicate label")
	}

	ordered := QualityConfig{EnableProfiles: true, Profiles: []QualityProfile{
		{Label: "Unspecified"}, {Label: "Second", PreferredOrder: 2}, {Label: "First", PreferredOrder: 1},
	}}
	if err := ordered.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := []string{ordered.Profiles[0].Label, ordered.Profiles[1].Label, ordered.Profiles[2].Label}; got[0] != "First" || got[1] != "Second" || got[2] != "Unspecified" {
		t.Fatalf("profile order = %v", got)
	}
}

func TestQualityPresetProfiles(t *testing.T) {
	q := QualityConfig{Preset: "4k-dolby-vision"}
	q.ApplyPreset()
	if len(q.Profiles) != 2 || q.Profiles[0].Label != "4K Dolby Vision" || q.Profiles[0].HDR != "dv" {
		t.Fatalf("preset profiles = %#v", q.Profiles)
	}
	custom := QualityConfig{Preset: defaultQualityPreset, Profiles: []QualityProfile{{Label: "My Profile"}}}
	custom.ApplyPreset()
	if len(custom.Profiles) != 1 || custom.Profiles[0].Label != "My Profile" {
		t.Fatalf("custom profiles changed: %#v", custom.Profiles)
	}
	noDV := QualityConfig{Preset: "no-dolby-vision"}
	noDV.ApplyPreset()
	if len(noDV.Profiles) != 2 || noDV.Profiles[0].Label != "4K HDR10" || noDV.Profiles[0].ExcludeHDR != "dv" {
		t.Fatalf("no-DV preset profiles = %#v", noDV.Profiles)
	}
	if err := noDV.Validate(); err != nil {
		t.Fatalf("no-DV preset validation: %v", err)
	}
	noHDR := QualityConfig{Preset: "no-hdr"}
	noHDR.ApplyPreset()
	if len(noHDR.Profiles) != 2 || noHDR.Profiles[0].Label != "4K SDR" || noHDR.Profiles[0].ExcludeHDR != "*" {
		t.Fatalf("no-HDR preset profiles = %#v", noHDR.Profiles)
	}
	if err := noHDR.Validate(); err != nil {
		t.Fatalf("no-HDR preset validation: %v", err)
	}
}

func TestQualityProfilesAlwaysMaterializeConfiguredVariants(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{Quality: QualityConfig{
		EnableProfiles: true,
		Profiles:       []QualityProfile{{Label: "1080p"}},
	}})
	if got := resolver.GetConfiguredVariants("virtual://movie/tt0133093"); len(got) != 1 {
		t.Fatalf("configured variants = %d, want 1 profile", len(got))
	}
}

func TestProfilesDisabledExposeOnlyCanonicalURL(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{Quality: QualityConfig{EnableProfiles: false}})
	variants := resolver.GetConfiguredVariants("virtual://movie/tt0133093")
	if len(variants) != 0 {
		t.Fatalf("configured variants = %d, want no variants when profiles are disabled", len(variants))
	}
}

func TestDecodeQualityProfilesRequiresTypedArray(t *testing.T) {
	raw := []any{map[string]any{"label": "1080p", "resolution": "1080p", "preferred_order": float64(2)}}
	profiles, err := decodeQualityProfiles(raw)
	if err != nil {
		t.Fatalf("decodeQualityProfiles() error = %v", err)
	}
	if len(profiles) != 1 || profiles[0].Label != "1080p" || profiles[0].PreferredOrder != 2 {
		t.Fatalf("profiles = %#v", profiles)
	}
	if _, err := decodeQualityProfiles(`[{"label":"1080p"}]`); err == nil {
		t.Fatal("JSON string quality profiles accepted")
	}
}

func TestProviderHTTPClientRejectsCrossOriginRedirect(t *testing.T) {
	client := newProviderHTTPClient()
	origin, _ := http.NewRequest(http.MethodGet, "https://provider.example/token/manifest.json", nil)
	target, _ := http.NewRequest(http.MethodGet, "https://attacker.example/token/manifest.json", nil)
	if err := client.CheckRedirect(target, []*http.Request{origin}); err == nil {
		t.Fatal("cross-origin redirect accepted")
	}
	sameOrigin, _ := http.NewRequest(http.MethodGet, "https://provider.example/token/next.json", nil)
	if err := client.CheckRedirect(sameOrigin, []*http.Request{origin}); err != nil {
		t.Fatalf("same-origin redirect rejected: %v", err)
	}
}

func TestDecodeBoundedJSONRejectsOversizedResponse(t *testing.T) {
	var payload stremioResponse
	if err := decodeBoundedJSON(strings.NewReader(`{"streams":[]}`), 64, &payload); err != nil {
		t.Fatalf("small response rejected: %v", err)
	}
	if err := decodeBoundedJSON(strings.NewReader(`{"streams":[]}`+strings.Repeat(" ", 64)), 64, &payload); err == nil {
		t.Fatal("oversized response accepted")
	}
}

func TestCandidateDisplayNameIncludesProviderSize(t *testing.T) {
	name := candidateDisplayName(StreamCandidate{Name: "AltMount FHD", Title: "Movie 1080p WEB-DL 5.69 GB"})
	if !strings.Contains(name, "5.69 GB") {
		t.Fatalf("candidate display name = %q, want provider size", name)
	}
}

func TestCandidateVariantIDIgnoresRotatingQueryCredentials(t *testing.T) {
	base := StreamCandidate{
		Name: "AltMount 4K", Title: "Movie 2160p WEB-DL", Resolution: "2160p",
		SourceType: "web-dl", OriginalIndex: 0,
		URL: "https://provider.example/play/movie.mkv?token=old&expires=100",
	}
	rotated := base
	rotated.URL = "https://provider.example/play/movie.mkv?token=new&expires=200"
	if got, want := candidateVariantID(rotated), candidateVariantID(base); got != want {
		t.Fatalf("rotating URL credentials changed candidate ID: got %q want %q", got, want)
	}
}

func TestCandidateVariantIDIgnoresProviderOrder(t *testing.T) {
	first := StreamCandidate{Name: "1080p WEB-DL", Resolution: "1080p", OriginalIndex: 0, URL: "https://provider.example/a.mkv?token=one"}
	second := first
	second.OriginalIndex = 1
	if got, want := candidateVariantID(second), candidateVariantID(first); got != want {
		t.Fatalf("provider reorder changed candidate ID: got %q want %q", got, want)
	}
}

func TestCandidateVariantIDDifferentiatesStableSources(t *testing.T) {
	first := StreamCandidate{Name: "1080p WEB-DL", Resolution: "1080p", URL: "https://provider.example/a.mkv?token=one"}
	second := first
	second.URL = "https://provider.example/b.mkv?token=one"
	if got, want := candidateVariantID(first), candidateVariantID(second); got == want {
		t.Fatalf("distinct stable sources share candidate ID %q", got)
	}
}

func TestSelectCandidatesRanksResultsAndProfiles(t *testing.T) {
	resolver := &manifestStreamResolver{}
	resolver.Configure(resolverConfig{Quality: QualityConfig{EnableProfiles: true, Profiles: []QualityProfile{
		{Label: "4K", Resolution: "2160p"},
		{Label: "SD", Resolution: "480p"},
	}, FallbackToAnyStream: true}})
	candidates := []StreamCandidate{
		{Title: "720p HDTV", Resolution: "720p", SourceType: "hdtv", OriginalIndex: 0, URL: "https://stream.example/720.mkv"},
		{Title: "2160p WEB-DL", Resolution: "2160p", SourceType: "web-dl", OriginalIndex: 1, URL: "https://stream.example/2160.mkv"},
		{Title: "1080p REMUX", Resolution: "1080p", SourceType: "remux", OriginalIndex: 2, URL: "https://stream.example/1080.mkv"},
	}
	profile := resolver.SelectCandidates("virtual://movie/tt1?profile=4K", candidates)
	if len(profile) != 1 || profile[0].Resolution != "2160p" {
		t.Fatalf("profile candidates = %#v, want one 2160p candidate", profile)
	}
	all := resolver.SelectCandidates("virtual://movie/tt1?results=all", candidates)
	if len(all) != 3 || all[0].Resolution != "2160p" || all[1].Resolution != "1080p" || all[2].Resolution != "720p" {
		t.Fatalf("all candidates order = %#v, want 2160p,1080p,720p", all)
	}
	defaultResult := resolver.SelectCandidates("virtual://movie/tt1", candidates)
	if len(defaultResult) != 3 || defaultResult[0].Resolution != "2160p" {
		t.Fatalf("default candidates = %#v, want ranked failover candidates", defaultResult)
	}
	fallback := resolver.SelectCandidates("virtual://movie/tt1?profile=Unknown", candidates)
	if len(fallback) != 3 || fallback[0].Resolution != "2160p" {
		t.Fatalf("fallback candidates = %#v, want ranked failover candidates", fallback)
	}
	knownFallback := resolver.SelectCandidates("virtual://movie/tt1?profile=SD", candidates)
	if len(knownFallback) != 3 || knownFallback[0].Resolution != "2160p" {
		t.Fatalf("known-profile fallback candidates = %#v, want ranked failover candidates", knownFallback)
	}
	explicitID := candidateVariantID(candidates[0])
	explicit := resolver.SelectCandidates("virtual://movie/tt1?result="+url.QueryEscape(explicitID), candidates)
	if len(explicit) != 1 || explicit[0].Resolution != "720p" {
		t.Fatalf("explicit candidates = %#v, want exact 720p candidate", explicit)
	}
}

func TestParseStreamDetailsPrefersTitleResolutionOverURLTokens(t *testing.T) {
	candidate := StreamCandidate{
		Title: "Sin City A Dame to Kill For [720p]",
		URL:   "https://provider.example/play/4k opaque-token",
	}
	parseStreamDetails(&candidate)
	if candidate.Resolution != "720p" {
		t.Fatalf("resolution = %q, want 720p", candidate.Resolution)
	}
}

func TestParseStreamDetailsUsesBoundedAudioAndHDRMarkers(t *testing.T) {
	falsePositive := StreamCandidate{Title: "Added Value Adventure", URL: "https://provider.example/video.mkv"}
	parseStreamDetails(&falsePositive)
	if falsePositive.CodecAudio != "" || falsePositive.HDR != "" {
		t.Fatalf("false-positive metadata = audio %q HDR %q", falsePositive.CodecAudio, falsePositive.HDR)
	}

	actual := StreamCandidate{Title: "2160p DV E-AC-3 Atmos"}
	parseStreamDetails(&actual)
	if actual.HDR != "dv" || actual.CodecAudio != "eac3" || !actual.HasAtmos {
		t.Fatalf("parsed metadata = audio %q (atmos: %v) HDR %q", actual.CodecAudio, actual.HasAtmos, actual.HDR)
	}
}

func TestManifestStreamResolver_UnreleasedShortCircuit(t *testing.T) {
	upstreamCalled := false
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		upstreamCalled = true
		return nil, fmt.Errorf("upstream should not be contacted for unreleased media")
	})}

	store := release.NewReleaseStore()
	futureAirDate := time.Now().Add(48 * time.Hour)
	store.SetShow("tt1234567", &release.ShowSchedule{
		IMDBID: "tt1234567",
		Status: "Running",
		Episodes: map[string]release.EpisodeInfo{
			"1:5": {Season: 1, Episode: 5, AirDate: futureAirDate, Title: "Future Episode"},
		},
	})
	store.SetMovie("tt7777777", futureAirDate)

	resolver := &manifestStreamResolver{
		client:       client,
		releaseStore: store,
	}
	resolver.Configure(resolverConfig{ManifestURL: "https://stream.example/manifest.json"})

	// 1. Unreleased episode yields a typed error, no candidates, no upstream call
	candidates, mediaType, mediaID, err := resolver.GetCandidates(context.Background(), "virtual://series/tt1234567/1/5")
	if err == nil {
		t.Fatalf("expected unreleased error for future episode")
	}
	var unreleased *unreleasedError
	if !errors.As(err, &unreleased) {
		t.Fatalf("error %v is not an unreleasedError", err)
	}
	if upstreamCalled {
		t.Fatalf("upstream provider was called for unreleased episode")
	}
	if len(candidates) != 0 {
		t.Fatalf("expected zero candidates for unreleased episode, got: %#v", candidates)
	}
	if !strings.Contains(err.Error(), "airs") || !strings.Contains(err.Error(), "episode") {
		t.Fatalf("unreleased message should name the episode and air date, got: %q", err.Error())
	}
	if mediaType != "series" || mediaID != "tt1234567:1:5" {
		t.Fatalf("unexpected mediaType/ID: %s, %s", mediaType, mediaID)
	}

	// 2. Resolve surfaces the typed error rather than a playable URL
	if _, resolveErr := resolver.Resolve(context.Background(), "virtual://series/tt1234567/1/5"); !errors.As(resolveErr, &unreleased) {
		t.Fatalf("Resolve should propagate the unreleased error, got: %v", resolveErr)
	}

	// 3. Movie unreleased check
	candidatesMovie, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt7777777")
	if !errors.As(err, &unreleased) {
		t.Fatalf("expected unreleased movie error, got: %v", err)
	}
	if upstreamCalled {
		t.Fatalf("upstream provider was called for unreleased movie")
	}
	if len(candidatesMovie) != 0 {
		t.Fatalf("expected zero candidates for unreleased movie, got: %#v", candidatesMovie)
	}

	// 4. Released titles still pass through to the provider path untouched
	releasedStore := release.NewReleaseStore()
	releasedStore.SetMovie("tt0100002", time.Now().Add(-24*time.Hour))
	passResolver := &manifestStreamResolver{
		client: &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("provider reached as expected")
		})},
		releaseStore: releasedStore,
	}
	passResolver.Configure(resolverConfig{ManifestURL: "https://stream.example/manifest.json"})
	if _, _, _, passErr := passResolver.GetCandidates(context.Background(), "virtual://movie/tt0100002"); passErr == nil || strings.Contains(passErr.Error(), "airs") {
		t.Fatalf("released movie should reach provider path, got: %v", passErr)
	}
}

func swrTestResolver(t *testing.T, transport roundTripperFunc) (*manifestStreamResolver, *int32) {
	t.Helper()
	var calls int32
	resolver := &manifestStreamResolver{
		client: &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			atomic.AddInt32(&calls, 1)
			return transport(r)
		})},
	}
	resolver.Configure(resolverConfig{ManifestURL: "https://stream.example/manifest.json", CacheTTLMinutes: 1})
	return resolver, &calls
}

func stremioBody() *http.Response {
	body := `{"streams":[{"name":"1080p","title":"Test","url":"https://provider.example/a.mkv"}]}`
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestCandidateCacheServesStaleWithinGraceAndRefreshesInBackground(t *testing.T) {
	resolver, calls := swrTestResolver(t, func(*http.Request) (*http.Response, error) {
		return stremioBody(), nil
	})
	// Seed an entry that expired two seconds ago (inside the grace window).
	key := "movie|tt1000001"
	cached := []StreamCandidate{{Name: "cached-1080p", URL: "https://provider.example/old.mkv"}}
	resolver.storeCandidateCache(key, cached, time.Now().Add(-2*time.Second), time.Now().Add(-time.Minute), resolver.cacheGeneration)

	done := make(chan struct{})
	go func() {
		candidates, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt1000001")
		if err != nil {
			t.Errorf("stale serve error: %v", err)
		}
		if len(candidates) != 1 || candidates[0].Name != "cached-1080p" {
			t.Errorf("expected stale cached candidate served instantly, got %#v", candidates)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stale serve blocked; grace path must return immediately")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resolver.cacheMu.Lock()
		entry, ok := resolver.cache[key]
		genOK := resolver.cacheGeneration == resolver.cacheGeneration
		resolver.cacheMu.Unlock()
		if ok && genOK && time.Now().Before(entry.expiresAt) && entry.candidates[0].Name == "1080p" {
			if atomic.LoadInt32(calls) != 1 {
				t.Fatalf("background refresh fetch count = %d, want 1", atomic.LoadInt32(calls))
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("background refresh did not replace the stale entry in time")
}

func TestCandidateCacheBeyondGraceBlocksOnProvider(t *testing.T) {
	resolver, calls := swrTestResolver(t, func(*http.Request) (*http.Response, error) {
		return stremioBody(), nil
	})
	key := "movie|tt1000002"
	resolver.storeCandidateCache(key, []StreamCandidate{{Name: "ancient", URL: "https://provider.example/x.mkv"}},
		time.Now().Add(-candidateStaleGrace-time.Minute), time.Now().Add(-30*time.Minute), resolver.cacheGeneration)

	candidates, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt1000002")
	if err != nil {
		t.Fatalf("beyond-grace lookup error: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Name != "1080p" {
		t.Fatalf("expected fresh provider candidate, got %#v", candidates)
	}
	if atomic.LoadInt32(calls) != 1 {
		t.Fatalf("synchronous fetch count = %d, want 1", atomic.LoadInt32(calls))
	}
}

func TestForceRefreshBypassesStaleGrace(t *testing.T) {
	resolver, calls := swrTestResolver(t, func(*http.Request) (*http.Response, error) {
		return stremioBody(), nil
	})
	key := "movie|tt1000003"
	resolver.storeCandidateCache(key, []StreamCandidate{{Name: "stale", URL: "https://provider.example/s.mkv"}},
		time.Now().Add(-2*time.Second), time.Now().Add(-time.Minute), resolver.cacheGeneration)

	candidates, _, _, err := resolver.GetCandidatesFresh(context.Background(), "virtual://movie/tt1000003")
	if err != nil {
		t.Fatalf("force refresh error: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Name != "1080p" {
		t.Fatalf("expected fresh fetch to bypass stale entry, got %#v", candidates)
	}
	if atomic.LoadInt32(calls) != 1 {
		t.Fatalf("forced synchronous fetch count = %d, want 1", atomic.LoadInt32(calls))
	}
}

// A forced re-list that hits the provider's empty flap must not fail while a
// servable positive entry is still cached: the cache retained that positive
// through the flap, so callers should get it instead of an empty answer.
func TestForcedRefreshFallsBackToCachedPositiveOnEmptyFlap(t *testing.T) {
	var served atomic.Int32
	resolver, calls := swrTestResolver(t, func(*http.Request) (*http.Response, error) {
		if served.Add(1) == 1 {
			return stremioBody(), nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"streams":[]}`)), Header: make(http.Header)}, nil
	})
	key := "movie|tt1000005"
	path := "virtual://movie/tt1000005"

	first, _, _, err := resolver.GetCandidates(context.Background(), path)
	if err != nil || len(first) != 1 {
		t.Fatalf("initial GetCandidates = %#v, err = %v; want one positive candidate", first, err)
	}
	// Age the entry past the forced-lookup floor so the forced call below
	// actually re-lists from the provider, but keep its TTL live.
	backdateFetchedAt(t, resolver, key, freshServeFloor+time.Second)

	candidates, _, _, err := resolver.GetCandidatesFresh(context.Background(), path)
	if err != nil {
		t.Fatalf("forced refresh error: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Name != "1080p" {
		t.Fatalf("forced refresh on empty flap = %#v, want the cached positive", candidates)
	}
	if got := atomic.LoadInt32(calls); got != 2 {
		t.Fatalf("provider calls = %d, want 2 (initial + forced)", got)
	}
}

// Once the cached positive is past stale grace it is no longer servable, so a
// forced re-list must return the provider's empty answer — the escape-from-
// dead-candidates semantics a forced refresh exists for.
func TestForcedRefreshUnboundedPastGraceReturnsEmptyFlap(t *testing.T) {
	resolver, calls := swrTestResolver(t, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"streams":[]}`)), Header: make(http.Header)}, nil
	})
	key := "movie|tt1000006"
	resolver.storeCandidateCache(key, []StreamCandidate{{Name: "ancient", URL: "https://provider.example/old.mkv"}},
		time.Now().Add(-candidateStaleGrace-time.Minute), time.Now().Add(-30*time.Minute), resolver.cacheGeneration)

	candidates, _, _, err := resolver.GetCandidatesFreshUnbounded(context.Background(), "virtual://movie/tt1000006")
	if err != nil {
		t.Fatalf("forced unbounded refresh error: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("forced refresh past grace = %#v, want the empty provider answer", candidates)
	}
	if got := atomic.LoadInt32(calls); got != 1 {
		t.Fatalf("provider calls = %d, want 1", got)
	}
}

func TestConcurrentStaleLookupsTriggerSingleRefresh(t *testing.T) {
	resolver, calls := swrTestResolver(t, func(*http.Request) (*http.Response, error) {
		time.Sleep(80 * time.Millisecond)
		return stremioBody(), nil
	})
	key := "movie|tt1000004"
	resolver.storeCandidateCache(key, []StreamCandidate{{Name: "stale", URL: "https://provider.example/c.mkv"}},
		time.Now().Add(-2*time.Second), time.Now().Add(-time.Minute), resolver.cacheGeneration)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, _ = resolver.GetCandidates(context.Background(), "virtual://movie/tt1000004")
		}()
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond) // allow background refresh to land
	if got := atomic.LoadInt32(calls); got > 1 {
		t.Fatalf("concurrent stale lookups caused %d provider fetches, want at most 1", got)
	}
}

func TestProviderStubCandidatesFilteredOut(t *testing.T) {
	body := `{"streams":[
		{"name":"No streams available","description":"Try another addon","url":"https://provider.example/stub?result=abc"},
		{"name":"1080p","title":"Real stream","url":"https://provider.example/real.mkv"},
		{"name":"4K","title":"No results for this query","url":"https://provider.example/stub2"}
	]}`
	resolver := &manifestStreamResolver{
		client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})},
	}
	resolver.Configure(resolverConfig{ManifestURL: "https://stream.example/manifest.json", CacheTTLMinutes: 1})

	candidates, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt1000009")
	if err != nil {
		t.Fatalf("GetCandidates error: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Name != "1080p" {
		t.Fatalf("expected only the real stream, got %#v", candidates)
	}
}

func TestConcurrentColdMissesDeduplicate(t *testing.T) {
	var calls int32
	resolver := &manifestStreamResolver{
		client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			atomic.AddInt32(&calls, 1)
			time.Sleep(100 * time.Millisecond) // simulate slow provider
			return stremioBody(), nil
		})},
	}
	resolver.Configure(resolverConfig{ManifestURL: "https://stream.example/manifest.json", CacheTTLMinutes: 1})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidates, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt2000001")
			if err != nil {
				t.Errorf("GetCandidates error: %v", err)
			}
			if len(candidates) == 0 {
				t.Error("expected non-empty candidates")
			}
		}()
	}
	wg.Wait()
	got := atomic.LoadInt32(&calls)
	if got != 1 {
		t.Fatalf("expected exactly 1 provider fetch for concurrent cold misses, got %d", got)
	}
}

func TestHardExpiredSingleflightDeduplicates(t *testing.T) {
	var calls int32
	resolver := &manifestStreamResolver{
		client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			atomic.AddInt32(&calls, 1)
			time.Sleep(80 * time.Millisecond)
			return stremioBody(), nil
		})},
	}
	resolver.Configure(resolverConfig{ManifestURL: "https://stream.example/manifest.json", CacheTTLMinutes: 1})
	key := "movie|tt2000002"
	// Store an entry far past grace (expired > 3 min ago).
	resolver.storeCandidateCache(key, []StreamCandidate{{Name: "old", URL: "https://provider.example/old.mkv"}},
		time.Now().Add(-candidateStaleGrace-time.Minute), time.Now().Add(-30*time.Minute), resolver.cacheGeneration)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidates, _, _, err := resolver.GetCandidates(context.Background(), "virtual://movie/tt2000002")
			if err != nil {
				t.Errorf("GetCandidates error: %v", err)
			}
			if len(candidates) == 0 {
				t.Error("expected candidates from blocking fetch")
			}
		}()
	}
	wg.Wait()
	got := atomic.LoadInt32(&calls)
	if got != 1 {
		t.Fatalf("hard-expired dedup fetch count = %d, want 1", got)
	}
}

func TestCandidateExpiryAndHeadersExtraction(t *testing.T) {
	futureUnix := time.Now().Add(2 * time.Hour).Unix()
	urlWithExp := fmt.Sprintf("https://provider.example/stream.mp4?token=xyz&expires=%d", futureUnix)
	exp := parseURLExpiration(urlWithExp)
	if exp.IsZero() || !exp.After(time.Now()) {
		t.Fatalf("parseURLExpiration failed: got %v", exp)
	}

	// Test AWS SigV4 signed URL expiration parsing
	amzDateStr := time.Now().UTC().Format("20060102T150405Z")
	amzURL := fmt.Sprintf("https://s3.example.com/bucket/video.mp4?X-Amz-Date=%s&X-Amz-Expires=3600", amzDateStr)
	amzExp := parseURLExpiration(amzURL)
	if amzExp.IsZero() || !amzExp.After(time.Now().Add(3500*time.Second)) {
		t.Fatalf("parseURLExpiration for AWS SigV4 failed: got %v", amzExp)
	}

	// Malformed or missing X-Amz-Date must not fabricate an expiry
	amzMissingDateURL := "https://s3.example.com/bucket/video.mp4?X-Amz-Expires=3600"
	if exp := parseURLExpiration(amzMissingDateURL); !exp.IsZero() {
		t.Fatalf("expected zero expiry for missing X-Amz-Date, got %v", exp)
	}
	amzMalformedDateURL := "https://s3.example.com/bucket/video.mp4?X-Amz-Date=invalid-date&X-Amz-Expires=3600"
	if exp := parseURLExpiration(amzMalformedDateURL); !exp.IsZero() {
		t.Fatalf("expected zero expiry for malformed X-Amz-Date, got %v", exp)
	}

	headers := extractProxyRequestHeaders(map[string]any{
		"request": map[string]any{
			"Referer":    "https://stremio.example/",
			"User-Agent": "Mozilla/5.0",
			"Secret":     "should-be-dropped",
		},
	})
	if headers["Referer"] != "https://stremio.example/" || headers["User-Agent"] != "Mozilla/5.0" {
		t.Fatalf("extracted headers incorrect: %#v", headers)
	}
	if _, secretFound := headers["Secret"]; secretFound {
		t.Fatalf("unallowed header 'Secret' was extracted: %#v", headers)
	}
}

func TestResolveVirtualStreamHonorsExclusionsAndPreferred(t *testing.T) {
	body := `{"streams":[
		{"name":"1080p","title":"Candidate A","url":"https://provider.example/a.mkv"},
		{"name":"1080p","title":"Candidate B","url":"https://provider.example/b.mkv"},
		{"name":"1080p","title":"Candidate C","url":"https://provider.example/c.mkv"}
	]}`
	resolver := &manifestStreamResolver{
		client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})},
	}
	resolver.Configure(resolverConfig{ManifestURL: "https://stream.example/manifest.json"})
	provider := &virtualStreamProvider{resolver: resolver}

	candidates, _, _, _ := resolver.GetCandidates(context.Background(), "virtual://movie/tt3000001")
	if len(candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(candidates))
	}
	candA_ID := candidateVariantID(candidates[0])
	candB_ID := candidateVariantID(candidates[1])
	candC_ID := candidateVariantID(candidates[2])

	// Request with candA excluded, candC preferred
	resp, err := provider.ResolveVirtualStream(context.Background(), &pb.ResolveVirtualStreamRequest{
		CapabilityId:         "stremio-stream-resolver",
		MediaType:            "movie",
		ExternalIds:          map[string]string{"imdb": "tt3000001"},
		ExcludedCandidateIds: []string{candA_ID},
		PreferredCandidateId: candC_ID,
	})
	if err != nil {
		t.Fatalf("ResolveVirtualStream error: %v", err)
	}
	resCandidates := resp.GetResult().GetCandidates()
	if len(resCandidates) != 2 {
		t.Fatalf("expected 2 candidates after exclusion, got %d", len(resCandidates))
	}
	// First candidate should be candC because it was preferred and not excluded
	if resCandidates[0].GetCandidateId() != candC_ID {
		t.Fatalf("first candidate = %q, want preferred %q", resCandidates[0].GetCandidateId(), candC_ID)
	}
	if resCandidates[1].GetCandidateId() != candB_ID {
		t.Fatalf("second candidate = %q, want %q", resCandidates[1].GetCandidateId(), candB_ID)
	}
}

func TestParseStreamMetadataPopulatesSubtitleLanguages(t *testing.T) {
	// A release that carries subtitle markers should advertise subtitle
	// languages so Silo can list and serve them.
	withSubtitles := StreamCandidate{Name: "Movie.2026.1080p.WEB-DL.DDP5.1.H.264-MULTi", Description: "Subtitles: eng / fra", Title: ""}
	parseStreamMetadata(&withSubtitles)
	if len(withSubtitles.SubtitleLanguages) == 0 {
		t.Fatalf("expected subtitle languages from subtitle-marked release, got %v", withSubtitles.SubtitleLanguages)
	}
	found := false
	for _, lang := range withSubtitles.SubtitleLanguages {
		if lang == "ENG" {
			found = true
		}
	}
	if !found {
		t.Fatalf("subtitle languages = %v, want ENG among them", withSubtitles.SubtitleLanguages)
	}
	if len(withSubtitles.SubtitleLanguages) != 2 {
		t.Fatalf("subtitle languages = %v, want [ENG FRA]", withSubtitles.SubtitleLanguages)
	}

	// An audio-only language marker without any subtitle hint must not be
	// advertised as a subtitle track.
	audioOnly := StreamCandidate{Name: "Movie.2026.1080p.WEB-DL.DDP5.1.H.264", Description: "English audio", Title: ""}
	parseStreamMetadata(&audioOnly)
	if len(audioOnly.SubtitleLanguages) != 0 {
		t.Fatalf("audio-only release wrongly advertised subtitle languages: %v", audioOnly.SubtitleLanguages)
	}
}
