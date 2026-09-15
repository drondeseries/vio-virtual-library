# Vio Virtual Library

Vio Virtual Library is a zero-storage playback plugin for Vio Server. Vio stores lightweight virtual media references such as `virtual://movie/tt0133093`; no video files, manifests, or segments are persisted by the plugin. At playback time, the plugin asks the configured streaming provider instance for a current upstream streaming URL.

## Install in Vio

In Vio, open **Admin → Plugins → Catalog**, add the following custom repository URL, and install **Vio Virtual Library**:

```text
https://raw.githubusercontent.com/drondeseries/vio-virtual-library/main/catalog.json
```


## How it works

1. A Vio library item registered through the authenticated plugin control plane points at an `virtual://` virtual path.
2. Vio delegates the path to the plugin over gRPC.
3. The playback handler recognizes the scheme and asks an `streamResolver` for a stream.
4. The resolver returns a time-limited HLS URL for immediate playback.
5. Vio or its client streams from that URL; this plugin stores no media locally.

After installation, you need two virtual libraries before the plugin can register media:

1. **Create virtual libraries**: Go to **Settings → Libraries → Add Library**. On the **Folders** tab, click **Add Virtual** — this inserts a `virtual://` path that tells Vio the library holds only virtual media with no local storage. Create one Movies library (`virtual://movies`) and one Series library (`virtual://series`).

2. **Configure the plugin**: Open the plugin settings and enter the **streaming provider Manifest URL** (the tokenized Stremio addon URL ending in `/manifest.json`). The Movies and Series library dropdowns will show your newly created virtual libraries — select them. HTTPS is required by default; toggle **Allow local HTTP** for private hosts (localhost, `.local`, Docker service names, private IPs).

The plugin derives the Stremio stream endpoint from that URL, requests streams for the IMDb identifier, and returns the first valid HTTP or HTTPS source. Manifest credentials are held in Vio-managed secret configuration and must not be committed to the repository.

When Virtual Library is selected as a request connection, compatible Vio builds automatically populate the generic Base URL and API key fields with plugin-managed values.

## Requests and monitored media

The `request_router.v1` capability checks release availability before reporting a request complete. Movies use TMDB digital/physical release dates across every returned market when a TMDB token and ID are available; theatrical-only titles remain queued. Cinemeta supplies the conservative fallback release date, including a 90-day theatrical window for catalog titles without explicit home-media dates. Metadata-provider outages fail closed: affected titles stay queued and are rechecked on the next monitor pass rather than being registered on a guessed date. Once home-media availability is established, Vio registers the item immediately without waiting for streaming provider discovery.

Titles whose tracked release date is still in the future resolve as explicitly unavailable: playback returns a result-level "airs <date>" status instead of a source, so nothing broken appears in the player. Streams appear automatically once the air date passes.

Items that are upcoming or theatrical-only are persisted in the configured monitored queue file. The `monitor-media` scheduled task rechecks release metadata. Vio's subsequent request-status poll observes `completed` once the title has a digital or physical release. Configure a writable absolute queue path for deployments whose plugin working directory is ephemeral.

Registration and collection sync never contact the streaming provider. When the user presses Play, the plugin makes one stream request for that movie or episode, caches the complete response briefly, and Vio exposes the matching profile plus additional results without repeating the provider request. Future episodes of an ongoing series are added on schedule without prewarming.

Provider candidates are cached for 10 minutes by default. Set **Candidate Cache TTL (minutes)** from 1 to 10080 (seven days) to tune freshness versus provider load. A playback request can explicitly set `force_refresh` to bypass this cache; no provider URLs or credentials are persisted.

Expired caches are not a stall: within a 10-minute grace window the previous candidates are served instantly while one background refresh repopulates them, so warm playback starts never wait on the provider. Beyond the grace window the lookup blocks on a fresh fetch, as before.

Series release schedules are refreshed from TVmaze in the background. **Schedule Refresh Interval (minutes)** (30–10080, default 360) controls the cadence; changes apply after plugin restart. The Release Desk admin page shows upcoming air dates per tracked show and a manual refresh button. TVmaze requests are rate-limited internally, and sync passes are capped at 90 seconds so they never exceed Vio's scheduled-task deadline.

Movie runtimes come from TMDB movie details when configured, with Cinemeta as fallback. Episode runtimes come from Cinemeta or TVMaze. Vio stores the canonical runtime before playback so growing HLS playlists do not make the seek bar expand second by second.

When an item becomes playable, the plugin submits a typed virtual-media registration to Vio's authenticated RuntimeHost service. Vio validates the selected library and transactionally owns all catalog, episode, virtual-file, cache-invalidation, and metadata-refresh behavior. The plugin never receives database credentials, executes SQL, or creates `.strm` files.

The server administrator configures the streaming provider manifest URL, TMDB token, Movies library ID, and Series library ID in the plugin settings. Normal users only interact with Request and Play.

## streaming provider Quality Profiles

You can enable **Quality Profiles** in the plugin settings to automatically match and register multiple quality versions (e.g. `4K HDR`, `1080p`) per movie or episode.

1. Toggle **Enable Quality Profiles** to `true`.
2. Paste single-line JSON configuration into the **Quality Profiles** field.

### Ready-to-use Single-Line JSON Configurations

**4K HDR + 1080p (Recommended)**:
```json
[{"label":"4K HDR","resolution":"2160p","include_regex":"(?i)(2160p|4k)","exclude_regex":"(?i)(cam|ts|telecine)","preferred_order":1},{"label":"1080p","resolution":"1080p","include_regex":"(?i)1080p","exclude_regex":"(?i)(cam|ts|telecine)","preferred_order":2}]
```

**4K + 1080p + 720p**:
```json
[{"label":"4K Ultra HD","resolution":"2160p","exclude_regex":"(?i)(cam|ts)","preferred_order":1},{"label":"1080p Full HD","resolution":"1080p","exclude_regex":"(?i)(cam|ts)","preferred_order":2},{"label":"720p HD","resolution":"720p","exclude_regex":"(?i)(cam|ts)","preferred_order":3}]
```

**Simple Resolution Only**:
```json
[{"label":"4K HDR","resolution":"2160p"},{"label":"1080p","resolution":"1080p"}]
```

Vio Server collection imports can also enable **Zero-storage virtual playback**. When enabled on
an MDBList, TMDB, or Trakt collection, the server materializes unmatched entries as virtual catalog
items and uses this plugin's existing `virtual-playback` route for resolution. Future collection
syncs add newly discovered entries without creating `.strm` or video files.

## SDK compatibility

This project targets the additive RuntimeHost extension in `github.com/drondeseries/silo-plugin-sdk` v0.10.1-virtual.2. Generated protobuf types are published at `pkg/pluginproto/silo/plugin/v1` (imported as `pb`) and request interception is provided by the `HttpRoutes` gRPC service. The extension remains wire-compatible with the v0.10 RuntimeHost contract while adding canonical virtual-media runtime metadata.

## Development

The module requires Go 1.26 or newer.

```sh
go mod download
go test ./...
go build -o bin/vio-virtual-library .
```

To inspect the manifest emitted by the compiled plugin:

```sh
./bin/vio-virtual-library manifest
```

The binary runs as a HashiCorp go-plugin subprocess and is normally started by Vio Server, not launched directly from an interactive shell.

## Production considerations

- Validate virtual identifiers and authorize playback against the requesting Vio user.
- Apply resolver timeouts, retry limits, and structured error mapping.
- Avoid logging resolver credentials, signed URLs, or access tokens.
- Return URLs with short expirations and scope them to one media item or playback session.
- Confirm streaming provider usage and content access comply with applicable provider terms and law.

## License

No license has been selected for this starter project.

## Quick-start checklist

1. **Create virtual libraries** — Admin → Libraries → Add Library → Folders → Add Virtual → `virtual://movies` and `virtual://tv`
2. **Configure plugin** — paste your streaming provider manifest URL, select the virtual libraries, save
3. **Create a collection** — Admin → Collections → TMDB Collection → pick a preset (Trending, Popular, etc.) → toggle **Zero-storage virtual playback** ON
4. **Sync** — the collection populates with metadata-only entries; no files are written to disk

## Common issues

### 502 on /playback/start

The SSRF guard rejects streams on private addresses. If your provider is local (`localhost`, `192.168.x.x`, `.local`, Docker service names), enable **Allow local HTTP** in plugin settings.

If using a reverse proxy with a public domain but local DNS rewrites: Vio must resolve the stream host to a public IP. Check `/etc/hosts` and DNS inside the Vio container.

### "unsafe stream URL: remote stream host resolves to a non-public address"

Same cause — enable **Allow local HTTP**.

### Play button visible but no sources

```bash
# Check virtual files exist for the item
docker exec postgres_vio psql -U vio -d vio -c \
  "SELECT file_path FROM media_files WHERE content_id='movie-tmdb-XXXXX' AND container='virtual'"

# If empty, trigger a catalog sync or re-add the media
```

### Virtual files disappeared overnight

Make sure your Vio Server build includes the virtual file retention fix. Run `scan_libraries` and verify the count doesn't drop:

```bash
docker exec postgres_vio psql -U vio -d vio -c \
  "SELECT COUNT(*) FROM media_files WHERE container='virtual'"
```

### Audio tracks or subtitles not showing

Virtual files are probed on **first playback**. Play the file once — ffprobe discovers real tracks and persists them. After that, the detail page shows all available audio and subtitle options.

## Diagnostic commands

```bash
# All virtual files
docker exec postgres_vio psql -U vio -d vio -c \
  "SELECT COUNT(*) FROM media_files WHERE container='virtual'"

# Items with most virtual files
docker exec postgres_vio psql -U vio -d vio -c \
  "SELECT mi.content_id, mi.title, COUNT(mf.id)
   FROM media_items mi JOIN media_files mf ON mf.content_id=mi.content_id
   WHERE mf.container='virtual'
   GROUP BY mi.content_id, mi.title ORDER BY 3 DESC LIMIT 20"

# Check one item's virtual files
docker exec postgres_vio psql -U vio -d vio -c \
  "SELECT file_path, resolution, codec_video, codec_audio, hdr
   FROM media_files WHERE content_id='movie-tmdb-XXXXX' AND container='virtual'"

# Test a virtual URI resolves
docker exec -it vio ffmpeg -i "virtual://movie/tt1234567" -f null /dev/null

# Recent playback errors
docker logs vio --tail 200 | grep -i "error\|virtual.*fail\|502"
```
