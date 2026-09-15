package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultSearchCheckMinutes = 15
	minSearchCheckMinutes     = 15
	maxSearchCheckMinutes     = 10080
	maxSearchBodyBytes        = 8 << 20
	maxProwlarrIndexBytes     = 64 << 20
	maxProwlarrIndexReleases  = 20000
	prowlarrIndexRetention    = 14 * 24 * time.Hour
)

var searchHTTPClient = newRestrictedRedirectHTTPClient(20 * time.Second)

// prowlarrRelease is one result from Prowlarr's /api/v1/search endpoint.
type prowlarrRelease struct {
	GUID            string              `json:"guid"`
	Title           string              `json:"title"`
	Size            int64               `json:"size"`
	Indexer         string              `json:"indexer"`
	IndexerID       int                 `json:"indexerId"`
	IMDbID          int64               `json:"imdbId"`
	TMDBID          int64               `json:"tmdbId"`
	TVDBID          int64               `json:"tvdbId"`
	PublishDate     string              `json:"publishDate"`
	DownloadURL     string              `json:"downloadUrl"`
	parsedCandidate *StreamCandidate    `json:"-"`
	episodeKeys     []episodeReleaseKey `json:"-"`
	normalizedTitle string              `json:"-"`
}

func prepareProwlarrRelease(release *prowlarrRelease) {
	if release == nil {
		return
	}
	candidate := StreamCandidate{Name: release.Title, Title: release.Title, URL: release.DownloadURL}
	parseStreamDetails(&candidate)
	release.parsedCandidate = &candidate
	release.episodeKeys = episodeReleaseKeys(release.Title)
	release.normalizedTitle, _ = normalizeReleaseTitle(release.Title)
}

// prowlarrSearchClient holds a cached snapshot of Prowlarr's recent releases
// search and refreshes it on a configurable interval (min 15 minutes). One
// request covers every enabled indexer.
type prowlarrSearchClient struct {
	mu        sync.Mutex
	url       string
	apiKey    string
	interval  time.Duration
	client    *http.Client
	lastFetch time.Time
	lastErr   error
	releases  []prowlarrRelease
	indexFile string
}

func newProwlarrSearchClient(client *http.Client) *prowlarrSearchClient {
	if client == nil {
		client = searchHTTPClient
	}
	return &prowlarrSearchClient{client: client}
}

// URL returns the configured Prowlarr base URL, or empty string.
func (c *prowlarrSearchClient) URL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.url
}

// Configure sets the Prowlarr base URL, API key, and check interval.
func (c *prowlarrSearchClient) Configure(baseURL, apiKey string, intervalMinutes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newURL := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	newKey := strings.TrimSpace(apiKey)
	if intervalMinutes < minSearchCheckMinutes {
		intervalMinutes = defaultSearchCheckMinutes
	}
	if intervalMinutes > maxSearchCheckMinutes {
		intervalMinutes = maxSearchCheckMinutes
	}
	newInterval := time.Duration(intervalMinutes) * time.Minute
	if newURL != c.url || newKey != c.apiKey || newInterval != c.interval {
		c.releases = nil
		c.lastFetch = time.Time{}
		c.lastErr = nil
	}
	c.url = newURL
	c.apiKey = newKey
	c.interval = newInterval
}

func (c *prowlarrSearchClient) ConfigureIndexFile(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	path = strings.TrimSpace(path)
	if path == "" {
		path = ".vio-virtual-library-prowlarr-index.json"
	}
	c.indexFile = path
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open Prowlarr index: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxProwlarrIndexBytes+1))
	if err != nil {
		return fmt.Errorf("read Prowlarr index: %w", err)
	}
	if len(data) > maxProwlarrIndexBytes {
		return fmt.Errorf("Prowlarr index exceeds %d bytes", maxProwlarrIndexBytes)
	}
	var releases []prowlarrRelease
	if err := json.Unmarshal(data, &releases); err != nil {
		return fmt.Errorf("decode Prowlarr index: %w", err)
	}
	for i := range releases {
		prepareProwlarrRelease(&releases[i])
	}
	c.releases = pruneProwlarrReleases(releases, time.Now())
	return nil
}

func (c *prowlarrSearchClient) searchURL() (string, error) {
	return c.searchURLForQuery("")
}

// searchURLForQuery builds the Prowlarr search endpoint URL without any
// credentials in it. The API key travels in the X-Api-Key request header so
// it can never leak through error strings or logs that embed URLs.
func (c *prowlarrSearchClient) searchURLForQuery(query string) (string, error) {
	c.mu.Lock()
	raw := c.url
	c.mu.Unlock()
	if strings.TrimSpace(raw) == "" {
		return "", errors.New("Prowlarr search URL is not configured")
	}
	u, err := url.Parse(raw + "/api/v1/search")
	if err != nil {
		return "", fmt.Errorf("invalid Prowlarr URL: %w", err)
	}
	q := u.Query()
	q.Set("limit", "1000")
	q.Del("categories")
	q.Add("categories", "2000")
	q.Add("categories", "5000")
	if strings.TrimSpace(query) != "" {
		q.Set("query", query)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *prowlarrSearchClient) newSearchRequest(ctx context.Context, searchURL string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, searchURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	c.mu.Lock()
	key := c.apiKey
	c.mu.Unlock()
	if key != "" {
		req.Header.Set("X-Api-Key", key)
	}
	return req, nil
}

func (c *prowlarrSearchClient) search(ctx context.Context, item monitoredMedia) ([]prowlarrRelease, error) {
	searchURL, err := c.searchURLForQuery(item.Title)
	if err != nil {
		return nil, err
	}
	req, err := c.newSearchRequest(ctx, searchURL)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.New("Prowlarr search request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Prowlarr search returned status %d", resp.StatusCode)
	}
	releases, err := parseProwlarrSearch(io.LimitReader(resp.Body, maxSearchBodyBytes+1))
	if err != nil {
		return nil, err
	}
	for i := range releases {
		prepareProwlarrRelease(&releases[i])
	}
	return releases, nil
}

func (c *prowlarrSearchClient) Stale() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.url == "" || c.lastFetch.IsZero() || time.Since(c.lastFetch) >= c.interval
}

func (c *prowlarrSearchClient) refreshIfStale(ctx context.Context) error {
	if !c.Stale() {
		return nil
	}
	return c.refresh(ctx)
}

func (c *prowlarrSearchClient) refresh(ctx context.Context) error {
	searchURL, err := c.searchURL()
	if err != nil {
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}
	req, err := c.newSearchRequest(ctx, searchURL)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		err = errors.New("Prowlarr search request failed")
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("Prowlarr search returned status %d", resp.StatusCode)
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}
	releases, err := parseProwlarrSearch(io.LimitReader(resp.Body, maxSearchBodyBytes+1))
	if err != nil {
		c.mu.Lock()
		c.lastErr = err
		c.mu.Unlock()
		return err
	}
	c.mu.Lock()
	merged := mergeProwlarrReleases(c.releases, releases, time.Now())
	c.releases = merged
	c.lastFetch = time.Now()
	c.lastErr = nil
	indexFile := c.indexFile
	c.mu.Unlock()
	if indexFile != "" {
		if err := saveProwlarrIndex(indexFile, merged); err != nil {
			return err
		}
	}
	return nil
}

func releaseKey(release prowlarrRelease) string {
	if release.GUID != "" {
		return "guid:" + release.GUID
	}
	if release.DownloadURL != "" {
		return "download:" + release.DownloadURL
	}
	return release.Indexer + "\x00" + release.Title + "\x00" + release.PublishDate
}

func mergeProwlarrReleases(existing, incoming []prowlarrRelease, now time.Time) []prowlarrRelease {
	byKey := make(map[string]prowlarrRelease, len(existing)+len(incoming))
	for _, release := range append(append([]prowlarrRelease(nil), existing...), incoming...) {
		prepareProwlarrRelease(&release)
		byKey[releaseKey(release)] = release
	}
	merged := make([]prowlarrRelease, 0, len(byKey))
	for _, release := range byKey {
		if release.PublishDate != "" {
			if published, err := time.Parse(time.RFC3339, release.PublishDate); err == nil && now.Sub(published) > prowlarrIndexRetention {
				continue
			}
		}
		merged = append(merged, release)
	}
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].PublishDate > merged[j].PublishDate })
	if len(merged) > maxProwlarrIndexReleases {
		merged = merged[:maxProwlarrIndexReleases]
	}
	return merged
}

func pruneProwlarrReleases(releases []prowlarrRelease, now time.Time) []prowlarrRelease {
	return mergeProwlarrReleases(nil, releases, now)
}

func saveProwlarrIndex(path string, releases []prowlarrRelease) error {
	data, kept := marshalProwlarrIndex(releases)
	if len(data) > maxProwlarrIndexBytes {
		return fmt.Errorf("Prowlarr index exceeds %d bytes after retaining %d releases", maxProwlarrIndexBytes, kept)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".silo-prowlarr-index-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(data); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}

func marshalProwlarrIndex(releases []prowlarrRelease) ([]byte, int) {
	for len(releases) > 0 {
		data, err := json.Marshal(releases)
		if err != nil {
			return nil, len(releases)
		}
		data = append(data, '\n')
		if len(data) <= maxProwlarrIndexBytes {
			return data, len(releases)
		}
		releases = releases[:len(releases)-1]
	}
	return []byte("[]\n"), 0
}

func parseProwlarrSearch(r io.Reader) ([]prowlarrRelease, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxSearchBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Prowlarr search: %w", err)
	}
	if int64(len(data)) > maxSearchBodyBytes {
		return nil, fmt.Errorf("Prowlarr search response exceeds %d bytes", maxSearchBodyBytes)
	}
	var releases []prowlarrRelease
	if err := json.Unmarshal(data, &releases); err != nil {
		return nil, fmt.Errorf("decode Prowlarr search: %w", err)
	}
	return releases, nil
}

// --- Title normalization for fallback matching ---

var prowlarrSepPattern = regexp.MustCompile(`[._\-/()\[\]{}]`)
var prowlarrTagPattern = regexp.MustCompile(`(?i)\b(2160p|1080p|720p|480p|4k|uhd|hdr|dolby|dolby.?vision|dv|hdr10|hdr10\+|ddp|dd|dts|dts-?hd|truehd|flac|aac|ac3|atmos|x264|x265|hevc|avc|av1|web-?dl|web-?rip|web|blu-?ray|brrip|bdrip|hdtv|dvdrip|remux|proper|repack|extended|imax|directors.?cut|unrated|multi|dual|10bit|8bit|10-?bit|h\.?264|h\.?265|s[0-9]{2}e[0-9]{2}|season[0-9]+|episode[0-9]+|amzn|amazon|nf|netflix|dsnp|disney|hbo|max|appletv|itunes|vudu|prime|red|group|post|subbed|dubbed|dual-?audio|hc|hdcam|cam|ts|telesync|telecine|screener|scr|complete|1080|720|2160)\b.*`)
var prowlarrCleanPattern = regexp.MustCompile(`[^a-z0-9]+`)
var prowlarrYearPattern = regexp.MustCompile(`\b(19|20)\d{2}\b`)

var episodeReleasePattern = regexp.MustCompile(`(?i)(?:s([0-9]{1,3})e([0-9]{1,3})|([0-9]{1,3})x([0-9]{1,3})|season[ ._-]*([0-9]{1,3})[ ._-]*episode[ ._-]*([0-9]{1,3}))`)

type episodeReleaseKey struct {
	season  int
	episode int
}

func episodeReleaseKeys(title string) []episodeReleaseKey {
	matches := episodeReleasePattern.FindAllStringSubmatch(title, -1)
	keys := make([]episodeReleaseKey, 0, len(matches))
	for _, match := range matches {
		values := [][2]int{{1, 2}, {3, 4}, {5, 6}}
		for _, pair := range values {
			if match[pair[0]] == "" || match[pair[1]] == "" {
				continue
			}
			season, seasonErr := strconv.Atoi(match[pair[0]])
			episode, episodeErr := strconv.Atoi(match[pair[1]])
			if seasonErr == nil && episodeErr == nil && season > 0 && episode > 0 {
				keys = append(keys, episodeReleaseKey{season: season, episode: episode})
			}
		}
	}
	return keys
}

func normalizeReleaseTitle(raw string) (title string, year int) {
	s := prowlarrSepPattern.ReplaceAllString(raw, " ")
	s = prowlarrTagPattern.ReplaceAllString(s, "")
	parts := strings.Fields(strings.ToLower(s))
	var kept []string
	for _, p := range parts {
		if m := prowlarrYearPattern.FindString(p); m != "" {
			if y, err := strconv.Atoi(m); err == nil {
				year = y
			}
			continue
		}
		if len(p) > 1 {
			kept = append(kept, p)
		}
	}
	cleaned := prowlarrCleanPattern.ReplaceAllString(strings.Join(kept, " "), " ")
	return strings.TrimSpace(cleaned), year
}

func normalizedReleaseTitle(raw string) string {
	title, _ := normalizeReleaseTitle(raw)
	return title
}

// releaseNameKey reduces a release or provider filename to a comparable
// identity: lowercase, extension stripped, all non-alphanumerics removed. Two
// postings of the same scene release normalize to the same key even when one
// uses dots and the other spaces.
func releaseNameKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	if idx := strings.IndexAny(value, "?#"); idx != -1 {
		value = value[:idx]
	}
	for _, ext := range []string{".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mov", ".webm"} {
		value = strings.TrimSuffix(value, ext)
	}
	return prowlarrCleanPattern.ReplaceAllString(value, "")
}

// candidateReleaseName returns the most release-like identity carried by a
// provider candidate. AltMount-style providers put the release name on the
// first line of the stream title (the remaining lines carry size and indexer
// badges), so only the first line is considered.
func candidateReleaseName(candidate StreamCandidate) string {
	for _, value := range []string{
		candidate.BehaviorHints.Filename,
		firstReleaseLine(candidate.Title),
		firstReleaseLine(candidate.Name),
	} {
		if key := releaseNameKey(value); key != "" {
			return key
		}
	}
	if parsed, err := url.Parse(strings.TrimSpace(candidate.URL)); err == nil {
		if key := releaseNameKey(path.Base(parsed.Path)); key != "" {
			return key
		}
	}
	return ""
}

func firstReleaseLine(value string) string {
	if idx := strings.IndexAny(value, "\r\n"); idx >= 0 {
		return value[:idx]
	}
	return value
}

// releaseSizesMatch reports whether two byte sizes plausibly describe the same
// file. Provider display sizes are rounded (and parsed as decimal GB), so the
// tolerance is wider than a byte-exact comparison. Unknown sizes never reject a
// match.
func releaseSizesMatch(a, b int64) bool {
	if a <= 0 || b <= 0 {
		return true
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	largest := a
	if b > largest {
		largest = b
	}
	return float64(diff)/float64(largest) <= 0.10
}

// prowlarrReleaseConfirmsCandidate reports whether a provider candidate is the
// same release that Prowlarr already indexed. Exact release-name identity is
// the primary signal; a normalized title+year match additionally requires
// compatible sizes so a different quality tier is not treated as confirmed.
func prowlarrReleaseConfirmsCandidate(release prowlarrRelease, candidate StreamCandidate) bool {
	candidateKey := candidateReleaseName(candidate)
	if candidateKey == "" {
		return false
	}
	releaseKeyValue := releaseNameKey(release.Title)
	if releaseKeyValue != "" && releaseKeyValue == candidateKey {
		return true
	}
	if release.normalizedTitle == "" {
		prepareProwlarrRelease(&release)
	}
	if release.normalizedTitle == "" {
		return false
	}
	releaseTitle, releaseYear := normalizeReleaseTitle(release.Title)
	candidateTitle, candidateYear := normalizeReleaseTitle(candidate.Title + " " + candidate.Name)
	if releaseTitle == "" || releaseTitle != candidateTitle {
		return false
	}
	if releaseYear != 0 && candidateYear != 0 && releaseYear != candidateYear {
		return false
	}
	// A title-only fallback is too weak on its own: Prowlarr returns many
	// quality tiers of the same movie. Require both sizes to corroborate the
	// match rather than confirming whichever tier the provider happened to
	// return. (AltMount's own classification may treat unknown sizes as
	// neutral; here a false confirmation would reorder playback.)
	if release.Size <= 0 || candidate.FileSize <= 0 {
		return false
	}
	return releaseSizesMatch(release.Size, candidate.FileSize)
}

// ClassifyCandidates sets SourceConfirmed on each candidate whose release the
// cached Prowlarr snapshot already carries. This is the fallback signal used
// when AltMount is not configured. The snapshot is a durable, operator-visible
// record (persisted to the index file and refreshed by the scheduled monitor),
// so this is not a live lookup and adds no provider round-trip to playback.
func (c *prowlarrSearchClient) ClassifyCandidates(candidates []StreamCandidate) {
	if c == nil || len(candidates) == 0 {
		return
	}
	c.mu.Lock()
	releases := append([]prowlarrRelease(nil), c.releases...)
	c.mu.Unlock()
	if len(releases) == 0 {
		return
	}
	for i := range candidates {
		for j := range releases {
			if prowlarrReleaseConfirmsCandidate(releases[j], candidates[i]) {
				candidates[i].SourceConfirmed = true
				candidates[i].SourceGUID = releases[j].GUID
				break
			}
		}
	}
}

// Match returns true when any release in the cached search results
// corresponds to the monitored media. Prefers IMDb/TMDB/TVDB IDs,
// falling back to normalized title+year comparison.
func (c *prowlarrSearchClient) Match(item monitoredMedia) bool {
	return c.MatchWithQuality(item, QualityConfig{})
}

func (c *prowlarrSearchClient) MatchWithQuality(item monitoredMedia, quality QualityConfig) bool {
	c.mu.Lock()
	releases := append([]prowlarrRelease(nil), c.releases...)
	c.mu.Unlock()
	if len(releases) == 0 {
		return false
	}
	return matchProwlarrReleasesWithQuality(releases, item, quality)
}

func (c *prowlarrSearchClient) SearchItem(ctx context.Context, item monitoredMedia) ([]prowlarrRelease, error) {
	return c.search(ctx, item)
}

func (c *prowlarrSearchClient) MatchEpisodeWithQuality(item monitoredMedia, episode virtualEpisode, quality QualityConfig) bool {
	c.mu.Lock()
	if c.url == "" {
		c.mu.Unlock()
		return false
	}
	releases := append([]prowlarrRelease(nil), c.releases...)
	c.mu.Unlock()
	wantTitle, _ := normalizeReleaseTitle(item.Title)
	if wantTitle == "" {
		return false
	}
	wantIMDb, _ := strconv.ParseInt(strings.TrimPrefix(strings.TrimSpace(item.IMDbID), "tt"), 10, 64)
	wantTMDB, _ := strconv.ParseInt(item.TMDBID, 10, 64)
	wantTVDB, _ := strconv.ParseInt(item.TVDBID, 10, 64)
	for i := range releases {
		release := &releases[i]
		if quality.EnableProfiles && !releaseMatchesQuality(release, quality.Profiles) {
			continue
		}
		if len(quality.CustomFormats) > 0 {
			if release.parsedCandidate == nil {
				prepareProwlarrRelease(release)
			}
			if release.parsedCandidate != nil {
				if _, rejected := customFormatScore(*release.parsedCandidate, quality.CustomFormats); rejected {
					continue
				}
			}
		}
		if wantIMDb > 0 && release.IMDbID > 0 && wantIMDb != release.IMDbID || wantTMDB > 0 && release.TMDBID > 0 && wantTMDB != release.TMDBID || wantTVDB > 0 && release.TVDBID > 0 && wantTVDB != release.TVDBID {
			continue
		}
		if release.normalizedTitle == "" {
			prepareProwlarrRelease(release)
		}
		if (strings.Contains(release.normalizedTitle, wantTitle) || strings.Contains(wantTitle, release.normalizedTitle)) && hasEpisodeReleaseKey(release.episodeKeys, episode.Season, episode.Episode) {
			return true
		}
	}
	return false
}

func containsEpisodeReleaseKey(title string, season, episode int) bool {
	return hasEpisodeReleaseKey(episodeReleaseKeys(title), season, episode)
}

func hasEpisodeReleaseKey(keys []episodeReleaseKey, season, episode int) bool {
	for _, key := range keys {
		if key.season == season && key.episode == episode {
			return true
		}
	}
	return false
}

func matchProwlarrReleases(releases []prowlarrRelease, item monitoredMedia) bool {
	return matchProwlarrReleasesWithQuality(releases, item, QualityConfig{})
}

func matchProwlarrReleasesWithQuality(releases []prowlarrRelease, item monitoredMedia, quality QualityConfig) bool {
	wantTitle, titleYear := normalizeReleaseTitle(item.Title)
	wantYear := item.Year
	if wantYear == 0 && titleYear != 0 {
		wantYear = int32(titleYear)
	}
	if wantTitle == "" {
		return false
	}
	wantIMDb, _ := strconv.ParseInt(strings.TrimPrefix(strings.TrimSpace(item.IMDbID), "tt"), 10, 64)
	wantTMDB, _ := strconv.ParseInt(item.TMDBID, 10, 64)
	wantTVDB, _ := strconv.ParseInt(item.TVDBID, 10, 64)

	for i := range releases {
		r := &releases[i]
		if quality.EnableProfiles && !releaseMatchesQuality(r, quality.Profiles) {
			continue
		}
		if len(quality.CustomFormats) > 0 {
			if r.parsedCandidate == nil {
				prepareProwlarrRelease(r)
			}
			if r.parsedCandidate != nil {
				if _, rejected := customFormatScore(*r.parsedCandidate, quality.CustomFormats); rejected {
					continue
				}
			}
		}
		// Fast path: direct ID match.
		if wantIMDb > 0 && r.IMDbID > 0 && wantIMDb == r.IMDbID {
			return true
		}
		if wantTMDB > 0 && r.TMDBID > 0 && wantTMDB == r.TMDBID {
			return true
		}
		if wantTVDB > 0 && r.TVDBID > 0 && wantTVDB == r.TVDBID {
			return true
		}

		// Fallback: title+year matching.
		if r.normalizedTitle == "" {
			prepareProwlarrRelease(r)
		}
		gotTitle, gotYear := r.normalizedTitle, 0
		if r.normalizedTitle != "" {
			_, gotYear = normalizeReleaseTitle(r.Title)
		}
		if gotTitle == "" {
			continue
		}
		titleMatch := strings.Contains(gotTitle, wantTitle) || strings.Contains(wantTitle, gotTitle)
		if !titleMatch {
			continue
		}
		if wantYear != 0 && gotYear != 0 && int(wantYear) != gotYear {
			continue
		}
		if wantYear == 0 && len(strings.Fields(wantTitle)) < 2 {
			continue
		}
		return true
	}
	return false
}

func releaseMatchesQuality(release *prowlarrRelease, profiles []QualityProfile) bool {
	if release == nil || len(profiles) == 0 {
		return false
	}
	candidate := release.parsedCandidate
	if candidate == nil {
		prepareProwlarrRelease(release)
		candidate = release.parsedCandidate
	}
	if candidate == nil {
		return false
	}
	return slicesContainsFunc(profiles, func(profile QualityProfile) bool {
		return matchProfile(*candidate, profile)
	})
}

func slicesContainsFunc[T any](values []T, predicate func(T) bool) bool {
	for _, value := range values {
		if predicate(value) {
			return true
		}
	}
	return false
}

type scoredProwlarrRelease struct {
	release     prowlarrRelease
	score       int
	rejected    bool
	index       int
	publishTime time.Time
	hasTime     bool
}

func sortProwlarrReleases(releases []prowlarrRelease, formats []CustomFormat) {
	if len(releases) == 0 {
		return
	}
	if len(releases) == 1 {
		if releases[0].parsedCandidate == nil {
			prepareProwlarrRelease(&releases[0])
		}
		if releases[0].parsedCandidate != nil {
			releases[0].parsedCandidate.QualityScore, _ = customFormatScore(*releases[0].parsedCandidate, formats)
		}
		return
	}
	scored := make([]scoredProwlarrRelease, len(releases))
	for i := range releases {
		if releases[i].parsedCandidate == nil {
			prepareProwlarrRelease(&releases[i])
		}
		score, rejected := 0, false
		if releases[i].parsedCandidate != nil {
			score, rejected = customFormatScore(*releases[i].parsedCandidate, formats)
			releases[i].parsedCandidate.QualityScore = score
		}
		pubStr := strings.TrimSpace(releases[i].PublishDate)
		var pubTime time.Time
		var hasTime bool
		if t, err := time.Parse(time.RFC3339, pubStr); err == nil {
			pubTime = t
			hasTime = true
		} else if t, err := time.Parse(time.RFC3339Nano, pubStr); err == nil {
			pubTime = t
			hasTime = true
		}
		scored[i] = scoredProwlarrRelease{
			release:     releases[i],
			score:       score,
			rejected:    rejected,
			index:       i,
			publishTime: pubTime,
			hasTime:     hasTime,
		}
	}
	sort.SliceStable(scored, func(i, j int) bool {
		s1, s2 := scored[i], scored[j]
		if s1.rejected != s2.rejected {
			return !s1.rejected
		}
		if s1.score != s2.score {
			return s1.score > s2.score
		}
		c1, c2 := s1.release.parsedCandidate, s2.release.parsedCandidate
		if c1 != nil && c2 != nil {
			if r1, r2 := resolutionScore(c1.Resolution), resolutionScore(c2.Resolution); r1 != r2 {
				return r1 > r2
			}
			if src1, src2 := sourceScore(c1.SourceType), sourceScore(c2.SourceType); src1 != src2 {
				return src1 > src2
			}
		} else if (c1 != nil) != (c2 != nil) {
			return c1 != nil
		}
		if s1.hasTime && s2.hasTime {
			if !s1.publishTime.Equal(s2.publishTime) {
				return s1.publishTime.After(s2.publishTime)
			}
		} else if s1.hasTime != s2.hasTime {
			return s1.hasTime
		} else if s1.release.PublishDate != s2.release.PublishDate {
			return s1.release.PublishDate > s2.release.PublishDate
		}
		return s1.index < s2.index
	})
	for i := range scored {
		releases[i] = scored[i].release
	}
}

// Validate performs a one-shot search and returns a human-readable status.
func (c *prowlarrSearchClient) Validate(ctx context.Context) (string, error) {
	searchURL, err := c.searchURL()
	if err != nil {
		return "", err
	}
	req, err := c.newSearchRequest(ctx, searchURL)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	// Dedicated short-timeout client so a stale Prowlarr config can't exhaust
	// the SDK's gRPC deadline during TestConnection.
	validateClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := validateClient.Do(req)
	if err != nil {
		return "", errors.New("connect to Prowlarr failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("Prowlarr returned HTTP %d", resp.StatusCode)
	}
	releases, err := parseProwlarrSearch(io.LimitReader(resp.Body, maxSearchBodyBytes+1))
	if err != nil {
		return "", fmt.Errorf("parse search: %w", err)
	}
	return fmt.Sprintf("Prowlarr search OK: %d recent releases across all indexers", len(releases)), nil
}
