# MeTube API Reference

This document lists the HTTP and Socket.IO interfaces exposed by MeTube.

Base URL
- All routes are prefixed by `URL_PREFIX` from the config. Default is `/`.
- Examples below assume `URL_PREFIX=/`.

## HTTP API

### `POST /add`
Queue a new download.

Request body (JSON):
```json
{
  "url": "https://music.youtube.com/watch?v=abc123def45",
  "quality": "best",
  "format": "mp3",
  "folder": "",
  "custom_name_prefix": "",
  "playlist_item_limit": 0,
  "auto_start": true,
  "split_by_chapters": false,
  "chapter_template": "%(title)s - %(section_number)02d - %(section_title)s.%(ext)s",
  "entry": {
    "_type": "video",
    "id": "abc123def45",
    "url": "https://music.youtube.com/watch?v=abc123def45",
    "webpage_url": "https://music.youtube.com/watch?v=abc123def45",
    "playlist_id": "PL123...",
    "playlist_title": "My Playlist"
  }
}
```

Response:
```json
{ "status": "ok" }
```
or
```json
{ "status": "error", "msg": "..." }
```

Notes:
- `entry` is optional; if present it is used as a metadata override (useful for retries).
- `quality` is required.

### `POST /delete`
Delete queued or completed items.

Request body (JSON):
```json
{ "ids": ["<download_id_or_url>"], "where": "queue" }
```
`where` must be `queue` or `done`.

Response:
```json
{ "status": "ok" }
```

### `POST /start`
Start pending downloads by id.

Request body (JSON):
```json
{ "ids": ["<download_id_or_url>"] }
```

Response:
```json
{ "status": "ok" }
```

### `GET /history`
Returns download history and current queue state.

Response:
```json
{
  "queue": [ { "download_id": "...", "status": "pending", "url": "...", "...": "..." } ],
  "pending": [ ... ],
  "done": [ ... ]
}
```

### `GET /version`
Returns MeTube + yt-dlp versions.

Response:
```json
{ "yt-dlp": "2025.12.08", "version": "dev" }
```

### `GET /db`
Serves the DB browser UI.

### Static file routes
- `GET /download/...` serves files from `DOWNLOAD_DIR`.
- `GET /audio_download/...` serves files from `AUDIO_DOWNLOAD_DIR`.

## Socket.IO API

Socket.IO is served at:
- Path: `/socket.io` (prefixed by `URL_PREFIX`).

Events emitted by the server:
- `added`: download added to queue.
- `updated`: download status update.
- `completed`: download finished.
- `canceled`: download canceled.
- `cleared`: download cleared from history.
- `all`: full queue snapshot on connect.
- `configuration`: current config object on connect.
- `custom_dirs`: available download dirs when `CUSTOM_DIRS` is enabled.
- `ytdl_options_changed`: emitted when the options file changes (when `YTDL_OPTIONS_FILE` is set).

All event payloads are JSON-serialized objects.

## CORS

If the request includes an `Origin` header, the server mirrors it as `Access-Control-Allow-Origin`. An `OPTIONS` handler exists for `/add`.
