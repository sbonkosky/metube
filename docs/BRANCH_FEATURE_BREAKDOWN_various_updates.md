# Branch Feature Breakdown: `various_updates`

Generated: 2026-02-21  
Comparison baseline: `upstream/master...various_updates`  
Branch-unique commits: 9

## Scope

This analysis covers only custom additions unique to this branch (not upstream MeTube changes already present in `upstream/master`).

Changed files in scope:

- `README.md`
- `app/db.html` (new)
- `app/main.py`
- `app/ytdl.py`
- `docs/API.md` (new)
- `ui/src/app/app.html`
- `ui/src/app/app.ts`
- `ui/src/app/interfaces/download.ts`
- `ui/src/app/services/downloads.service.ts`

## Commit Timeline (Branch-Unique)

1. `45175f5` Add `PLAYLIST_ITEMS_OLDEST_FIRST` env flag for playlist ordering
2. `c04dbd7` multiple updates
3. `bd1175e` added `COOKIEFILE_ON_NO_FORMATS_ONLY`
4. `f27ecb1` retrying song now reuses existing entry metadata
5. `855ea19` status filter for `/db`
6. `82045bd` retry button for `/db`
7. `4f9d7a1` redirect tracking + DB column controls + API docs
8. `f3d5768` cookie behavior for liked music playlist + `/db` colors
9. `c749409` add paging

## Feature Breakdown

## 1) New Config Flags and Runtime Behavior

Added environment variables:

- `PLAYLIST_ITEMS_OLDEST_FIRST` (default `false`)
- `SKIP_EXISTING_DOWNLOADS` (default `false`)
- `RETRY_403_MAX` (default `0`)
- `COOKIEFILE_ON_NO_FORMATS_ONLY` (default `false`)

Behavior added:

- Playlist entry processing can be reversed so oldest items are enqueued first.
- Existing files can be detected and treated as completed without redownloading.
- 403 errors can retry with backoff, controlled by `RETRY_403_MAX`.
- Cookiefile usage can be delayed until a “No video formats found” path is hit.

Primary code:

- `app/main.py` (config defaults + boolean parsing)
- `app/ytdl.py` (download/extract behavior)
- `README.md` (docs)

## 2) Queue Identity Refactor (`download_id`)

Added stable queue identity via `download_id`:

- Each download gets a UUID-like `download_id`.
- Persistent queue keys now use `download_id` instead of raw URL.
- Legacy persisted entries are upgraded on load with `_ensure_download_id(...)`.
- Cancel/start/delete paths now operate on IDs that can be `download_id` (or URL fallback where needed).

Why this matters:

- Avoids collisions when multiple queue items share the same URL shape.
- Enables safer row-level operations from `/db` and UI actions.

Primary code:

- `app/ytdl.py` (`DownloadInfo`, `PersistentQueue`, queue ops)
- `ui/src/app/services/downloads.service.ts`
- `ui/src/app/interfaces/download.ts`
- `ui/src/app/app.html`

## 3) Retry Flow with Entry Metadata Preservation

Retry behavior was upgraded to preserve source metadata:

- `POST /add` now accepts optional `entry` object override.
- Queue add path can enqueue directly from entry metadata (`entry_override`) instead of re-extracting only from URL.
- Main Angular retry flow now deletes old done-row first, then re-adds with preserved `entry`.
- `/db` retry also reconstructs and passes entry metadata to retain playlist context.

Net result:

- Better retry fidelity for playlist/channel items.
- Reduced chance of losing playlist attribution during retry.

Primary code:

- `app/main.py` (`/add` endpoint accepts/validates `entry`)
- `app/ytdl.py` (`add(..., entry_override=...)`)
- `ui/src/app/app.ts` (`retryDownload`)
- `ui/src/app/services/downloads.service.ts` (optional `entry` in add payload)
- `app/db.html` (`retrySelectedErrors`)

## 4) Download Robustness: Redirects, Cookies, and 403 Retries

### A) Redirect recovery for “No video formats found”

On certain failures, download logic now:

- Attempts to resolve a better redirected watch URL by inspecting final response URL + page content signals.
- Retries with discovered redirect URL.
- Persists `redirect_url` to status for visibility.

### B) Cookie fallback strategy

When enabled via `COOKIEFILE_ON_NO_FORMATS_ONLY`:

- Cookiefile is removed from normal first attempt.
- If “No video formats found” occurs, retry can add cookiefile.
- Extract-info stage has similar fallback logic.
- Special-case: liked music playlists (`list=LM`) keep cookies during extract-info path.

### C) 403 retry strategy

- HTTP 403 failures can retry with increasing delays (bounded by `RETRY_403_MAX`).

### D) Cookie usage visibility

- Download status now tracks and exposes `used_cookies`.

Primary code:

- `app/ytdl.py`
- `app/db.html` (shows `redirect_url`, `used_cookies`)

## 5) Playlist/Collection Enhancements

### A) Oldest-first playlist processing

- Playlist/channel entries can be reversed before enqueueing.

### B) Duplicate playlist-entry suppression

- Branch adds duplicate detection by `(playlist_id, entry_id)` across queue/pending/done.
- Prevents re-enqueuing the same playlist item repeatedly.

### C) Audio playlist `.m3u8` generation

On completed audio downloads with playlist metadata:

- Writes/updates playlist files under `AUDIO_DOWNLOAD_DIR/<folder>/_playlists/` when a `folder` is set, falling back to `AUDIO_DOWNLOAD_DIR/_playlists/` when no folder is provided.
- Relative paths inside the `.m3u8` are computed from the `_playlists/` directory to the downloaded file, so they remain correct regardless of folder depth.
- Sanitizes playlist names for filesystem safety.
- Maintains `#EXTM3U` header.
- Moves latest item to top and de-duplicates existing path entries.

Primary code:

- `app/main.py` (`_update_audio_playlist_file(...)`, notifier hook)
- `app/ytdl.py` (playlist handling + duplicate logic)

## 6) New DB Browser at `/db`

A full DB/queue browser UI was added (`app/db.html`) and routed in backend (`/db`, `/db/`).

Core capabilities:

- Loads rows from `/history` and merges `queue`, `pending`, `done`.
- Multi-column sortable table.
- Playlist-title filter and status filter.
- Toggle visible columns (persisted in `localStorage`).
- Pagination with configurable page size (persisted in `localStorage`).
- Bulk row selection with select-all.
- Bulk delete grouped by queue vs done via `/delete`.
- Bulk retry for selected error rows (delete old row + re-add via `/add`).
- Modal viewer for full `entry` JSON payload.
- URL/redirect URL rendered as clickable links.
- Status-based row highlighting (e.g., `error`, `pending`).

Primary code:

- `app/db.html`
- `app/main.py` (`db_browser` route)

## 7) Main UI Compatibility Updates

Angular updates to match backend ID/entry changes:

- Queue/done maps keyed by `download_id` when available, fallback to URL.
- Socket event handlers now consistently use computed key helper.
- `@for` tracking updated to track by map key.
- Download interface extended with optional `download_id` and `entry`.
- Add API call can include `entry` payload.

Primary code:

- `ui/src/app/services/downloads.service.ts`
- `ui/src/app/app.html`
- `ui/src/app/app.ts`
- `ui/src/app/interfaces/download.ts`

## 8) Documentation Expansion

### README updates

- Documents new env vars:
  - `PLAYLIST_ITEMS_OLDEST_FIRST`
  - `SKIP_EXISTING_DOWNLOADS`
  - `RETRY_403_MAX`
  - `COOKIEFILE_ON_NO_FORMATS_ONLY`
- Adds `/db` browser section.

### New API reference doc

- Adds `docs/API.md` covering:
  - `/add`, `/delete`, `/start`, `/history`, `/version`, `/db`
  - static file routes
  - Socket.IO events
  - CORS behavior

## Practical Impact Summary

This branch primarily adds:

- Better reliability for difficult media fetches (redirect/cookie/403 handling).
- Better operational control and observability (`download_id`, `redirect_url`, `used_cookies`).
- Better retry correctness (preserved entry metadata).
- A substantial new admin/operator surface via `/db` for filtering, paging, deleting, and retrying queue records.
- Audio playlist artifact generation (`.m3u8`) from completed playlist audio items.
