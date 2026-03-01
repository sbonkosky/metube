import os
import shutil
import errno
import yt_dlp
import yt_dlp.utils
from collections import OrderedDict
import shelve
import time
import asyncio
import multiprocessing
import logging
import re
import types
import dbm
import subprocess
from typing import Any
from functools import lru_cache
import uuid
import json
from urllib.parse import urlparse, parse_qs
from mutagen import File as MutagenFile

import yt_dlp.networking.impersonate
from yt_dlp.utils import STR_FORMAT_RE_TMPL, STR_FORMAT_TYPES
from dl_formats import get_format, get_opts, AUDIO_FORMATS
from datetime import datetime

log = logging.getLogger('ytdl')


@lru_cache(maxsize=None)
def _compile_outtmpl_pattern(field: str) -> re.Pattern:
    """Compile a regex pattern to match a specific field in an output template, including optional format specifiers."""
    conversion_types = f"[{re.escape(STR_FORMAT_TYPES)}]"
    return re.compile(STR_FORMAT_RE_TMPL.format(re.escape(field), conversion_types))


def _outtmpl_substitute_field(template: str, field: str, value: Any) -> str:
    """Substitute a single field in an output template, applying any format specifiers to the value."""
    pattern = _compile_outtmpl_pattern(field)

    def replacement(match: re.Match) -> str:
        if match.group("has_key") is None:
            return match.group(0)

        prefix = match.group("prefix") or ""
        format_spec = match.group("format")

        if not format_spec:
            return f"{prefix}{value}"

        conversion_type = format_spec[-1]
        try:
            if conversion_type in "diouxX":
                coerced_value = int(value)
            elif conversion_type in "eEfFgG":
                coerced_value = float(value)
            else:
                coerced_value = value

            return f"{prefix}{('%' + format_spec) % coerced_value}"
        except (ValueError, TypeError):
            return f"{prefix}{value}"

    return pattern.sub(replacement, template)

def _convert_generators_to_lists(obj):
    """Recursively convert generators to lists in a dictionary to make it pickleable."""
    if isinstance(obj, types.GeneratorType):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: _convert_generators_to_lists(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return type(obj)(_convert_generators_to_lists(item) for item in obj)
    else:
        return obj

def _ensure_download_id(info, fallback=None):
    if getattr(info, 'download_id', None):
        return info.download_id
    info.download_id = fallback or uuid.uuid4().hex
    return info.download_id

def _is_within_root(path, root):
    if path == root:
        return True
    return path.startswith(root + os.sep)

def _safe_unlink(path, label='file'):
    if not path:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except OSError as exc:
        log.warning(f"Failed to remove {label} {path}: {exc}")

class DownloadQueueNotifier:
    async def added(self, dl):
        raise NotImplementedError

    async def updated(self, dl):
        raise NotImplementedError

    async def completed(self, dl):
        raise NotImplementedError

    async def canceled(self, id):
        raise NotImplementedError

    async def cleared(self, id):
        raise NotImplementedError

class DownloadInfo:
    def __init__(self, id, title, url, quality, format, folder, custom_name_prefix, error, entry, playlist_item_limit, split_by_chapters, chapter_template, skip_existing_downloads=False, retry_403_max=0, cookiefile_fallback=None):
        self.id = id if len(custom_name_prefix) == 0 else f'{custom_name_prefix}.{id}'
        self.title = title if len(custom_name_prefix) == 0 else f'{custom_name_prefix}.{title}'
        self.url = url
        self.redirect_url = None
        self.quality = quality
        self.format = format
        self.folder = folder
        self.custom_name_prefix = custom_name_prefix
        self.msg = self.percent = self.speed = self.eta = None
        self.status = "pending"
        self.size = None
        self.timestamp = time.time_ns()
        self.download_id = uuid.uuid4().hex
        self.skip_existing_downloads = skip_existing_downloads
        self.retry_403_max = retry_403_max
        self.cookiefile_fallback = cookiefile_fallback
        self.used_cookies = False
        self.error = error
        # Convert generators to lists to make entry pickleable
        self.entry = _convert_generators_to_lists(entry) if entry is not None else None
        if isinstance(self.entry, dict):
            self.entry_playlist_title = self.entry.get('playlist_title')
            self.entry_playlist_id = self.entry.get('playlist_id')
            self.entry_playlist = self.entry.get('playlist')
        else:
            self.entry_playlist_title = None
            self.entry_playlist_id = None
            self.entry_playlist = None
        self.playlist_item_limit = playlist_item_limit
        self.split_by_chapters = split_by_chapters
        self.chapter_template = chapter_template

class Download:
    manager = None

    def __init__(self, download_dir, temp_dir, output_template, output_template_chapter, quality, format, ytdl_opts, info, audio_download_dir=None, state_dir=None, cookiefile_fallback=None, request_cookiefile=None):
        self.download_dir = download_dir
        self.temp_dir = temp_dir
        self.output_template = output_template
        self.output_template_chapter = output_template_chapter
        self.format = get_format(format, quality)
        self.ytdl_opts = get_opts(format, quality, ytdl_opts)
        if getattr(info, 'skip_existing_downloads', False) and 'overwrites' not in self.ytdl_opts and 'nooverwrites' not in self.ytdl_opts:
            self.ytdl_opts['overwrites'] = False
        if "impersonate" in self.ytdl_opts:
            self.ytdl_opts["impersonate"] = yt_dlp.networking.impersonate.ImpersonateTarget.from_str(self.ytdl_opts["impersonate"])
        self.info = info
        if not hasattr(self.info, 'redirect_url'):
            self.info.redirect_url = None
        if not hasattr(self.info, 'used_cookies'):
            self.info.used_cookies = False
        self.cookiefile_fallback = cookiefile_fallback if cookiefile_fallback is not None else getattr(info, 'cookiefile_fallback', None)
        self.request_cookiefile = request_cookiefile
        self.audio_download_dir = audio_download_dir
        self.state_dir = state_dir
        self.canceled = False
        self.tmpfilename = None
        self.status_queue = None
        self.proc = None
        self.loop = None
        self.notifier = None

    def _download(self):
        download_url = getattr(self.info, 'redirect_url', None) or self.info.url
        log.info(f"Starting download for: {self.info.title} ({download_url})")
        try:
            debug_logging = logging.getLogger().isEnabledFor(logging.DEBUG)
            def put_status(st):
                self.status_queue.put({k: v for k, v in st.items() if k in (
                    'tmpfilename',
                    'filename',
                    'status',
                    'msg',
                    'total_bytes',
                    'total_bytes_estimate',
                    'downloaded_bytes',
                    'speed',
                    'eta',
                )})

            def put_status_postprocessor(d):
                if d['postprocessor'] == 'MoveFiles' and d['status'] == 'finished':
                    filepath = d['info_dict']['filepath']
                    if '__finaldir' in d['info_dict']:
                        finaldir = d['info_dict']['__finaldir']
                        # Compute relative path from temp dir to preserve
                        # subdirectory structure from the output template.
                        try:
                            rel_path = os.path.relpath(filepath, self.temp_dir)
                        except ValueError:
                            rel_path = os.path.basename(filepath)
                        if rel_path.startswith('..'):
                            # filepath is not under temp_dir, fall back to basename
                            rel_path = os.path.basename(filepath)
                        filename = os.path.join(finaldir, rel_path)
                    else:
                        filename = filepath
                    self.status_queue.put({'status': 'finished', 'filename': filename})

                # Capture all chapter files when SplitChapters finishes
                elif d.get('postprocessor') == 'SplitChapters' and d.get('status') == 'finished':
                    chapters = d.get('info_dict', {}).get('chapters', [])
                    if chapters:
                        for chapter in chapters:
                            if isinstance(chapter, dict) and 'filepath' in chapter:
                                log.info(f"Captured chapter file: {chapter['filepath']}")
                                self.status_queue.put({'chapter_file': chapter['filepath']})
                    else:
                        log.warning("SplitChapters finished but no chapter files found in info_dict")

            ytdl_params = {
                'quiet': not debug_logging,
                'verbose': debug_logging,
                'no_color': True,
                'paths': {"home": self.download_dir, "temp": self.temp_dir},
                'outtmpl': { "default": self.output_template, "chapter": self.output_template_chapter },
                'format': self.format,
                'socket_timeout': 30,
                'ignore_no_formats_error': True,
                'progress_hooks': [put_status],
                'postprocessor_hooks': [put_status_postprocessor],
                **self.ytdl_opts,
            }
            if ytdl_params.get('cookiefile') and self.status_queue is not None:
                self.status_queue.put({'used_cookies': True})

            # Add chapter splitting options if enabled
            if self.info.split_by_chapters:
                ytdl_params['outtmpl']['chapter'] = self.info.chapter_template
                if 'postprocessors' not in ytdl_params:
                    ytdl_params['postprocessors'] = []
                ytdl_params['postprocessors'].append({
                    'key': 'FFmpegSplitChapters',
                    'force_keyframes': False
                })

            def _info_get(obj, key, default=None):
                if isinstance(obj, dict):
                    return obj.get(key, default)
                return getattr(obj, key, default)

            def _extract_video_id_from_url(value):
                if not value:
                    return None
                parsed = urlparse(value)
                query_id = parse_qs(parsed.query).get('v', [None])[0]
                if query_id:
                    return query_id
                if parsed.netloc.endswith('youtu.be'):
                    path_id = parsed.path.strip('/').split('/')[0]
                    return path_id or None
                return None

            def resolve_expected_paths(url):
                target_filename = None
                video_id = None
                entry = getattr(self.info, 'entry', None)
                if isinstance(entry, dict):
                    entry_id = entry.get('id')
                    if isinstance(entry_id, str) and entry_id:
                        video_id = entry_id
                if not video_id:
                    candidate_id = getattr(self.info, 'id', None)
                    if isinstance(candidate_id, str) and re.fullmatch(r'[A-Za-z0-9_-]{11}', candidate_id):
                        video_id = candidate_id
                if not video_id:
                    video_id = _extract_video_id_from_url(url)

                try:
                    ydl = yt_dlp.YoutubeDL(params=ytdl_params)
                    info = ydl.extract_info(url, download=False)
                    filename = ydl.prepare_filename(info)
                    info_id = info.get('id')
                    if isinstance(info_id, str) and info_id:
                        if video_id and video_id != info_id:
                            log.debug(f"Using resolved video id {info_id} instead of requested id {video_id} for duplicate lookup")
                        video_id = info_id
                except Exception as exc:
                    log.debug(f"Existing file check failed for {url}: {exc}")
                    return None, None, video_id

                if not filename:
                    return None, None, video_id

                target_filename = filename
                candidates = [filename]
                requested_format = getattr(self.info, 'format', None)
                if requested_format in AUDIO_FORMATS:
                    base, _ = os.path.splitext(filename)
                    target_filename = base + f'.{requested_format}'
                    if target_filename not in candidates:
                        candidates.append(target_filename)

                for candidate in candidates:
                    if os.path.exists(candidate):
                        return target_filename, candidate, video_id
                return target_filename, None, video_id

            def _value_contains_video_id(value, video_id):
                if value is None:
                    return False
                if isinstance(value, (list, tuple, set)):
                    return any(_value_contains_video_id(item, video_id) for item in value)
                text = str(value)
                if not text:
                    return False
                return (
                    video_id in text
                    or f"watch?v={video_id}" in text
                    or f"youtu.be/{video_id}" in text
                )

            def _file_matches_video_id(path, video_id):
                base_name = os.path.basename(path)
                if video_id in base_name:
                    return True
                try:
                    metadata = MutagenFile(path)
                except Exception as exc:
                    log.debug(f"Failed reading metadata for duplicate lookup ({path}): {exc}")
                    return False
                tags = getattr(metadata, 'tags', None)
                if not tags:
                    return False
                try:
                    values = tags.values() if hasattr(tags, 'values') else []
                except Exception:
                    values = []
                for value in values:
                    if _value_contains_video_id(value, video_id):
                        return True
                return False

            def find_audio_duplicate_path_by_video_id(video_id, target_path=None):
                if not video_id or not self.audio_download_dir:
                    return None

                real_audio_root = os.path.realpath(self.audio_download_dir)
                target_real = os.path.realpath(target_path) if target_path else None
                requested_format = str(getattr(self.info, 'format', '') or '').lower()

                if self.state_dir:
                    completed_path = os.path.join(self.state_dir, 'completed')
                    try:
                        with shelve.open(completed_path, 'r') as shelf:
                            completed_items = list(shelf.items())
                    except Exception as exc:
                        log.debug(f"Failed to read completed downloads for duplicate lookup: {exc}")
                        completed_items = []

                    for _, saved_info in completed_items:
                        entry = _info_get(saved_info, 'entry', None)
                        entry_id = entry.get('id') if isinstance(entry, dict) else None
                        saved_url_id = _extract_video_id_from_url(_info_get(saved_info, 'url', None))
                        saved_redirect_id = _extract_video_id_from_url(_info_get(saved_info, 'redirect_url', None))
                        if video_id not in {entry_id, saved_url_id, saved_redirect_id}:
                            continue

                        saved_quality = _info_get(saved_info, 'quality', None)
                        saved_format = str(_info_get(saved_info, 'format', '') or '').lower()
                        if saved_quality != 'audio' and saved_format not in AUDIO_FORMATS:
                            continue
                        if requested_format in AUDIO_FORMATS and saved_format and saved_format != requested_format:
                            continue

                        saved_filename = _info_get(saved_info, 'filename', None)
                        if not isinstance(saved_filename, str) or not saved_filename:
                            continue

                        saved_folder = _info_get(saved_info, 'folder', None)
                        candidate_base = os.path.join(real_audio_root, saved_folder) if saved_folder else real_audio_root
                        candidate_path = os.path.realpath(os.path.join(candidate_base, saved_filename))
                        if not _is_within_root(candidate_path, real_audio_root):
                            continue
                        if target_real and candidate_path == target_real:
                            continue
                        if os.path.isfile(candidate_path):
                            return candidate_path

                for current_root, _, files in os.walk(real_audio_root):
                    for file_name in files:
                        extension = os.path.splitext(file_name)[1].lower().lstrip('.')
                        if extension not in AUDIO_FORMATS:
                            continue
                        if requested_format in AUDIO_FORMATS and extension != requested_format:
                            continue

                        candidate_path = os.path.realpath(os.path.join(current_root, file_name))
                        if target_real and candidate_path == target_real:
                            continue
                        if not _is_within_root(candidate_path, real_audio_root):
                            continue
                        if not os.path.isfile(candidate_path):
                            continue
                        if _file_matches_video_id(candidate_path, video_id):
                            return candidate_path
                return None

            def hard_link_audio_duplicate(source_path, target_path):
                if not source_path or not target_path:
                    return False

                if os.path.exists(target_path):
                    try:
                        if os.path.samefile(source_path, target_path):
                            return True
                    except OSError:
                        pass
                    log.info(f"Duplicate target already exists at {target_path}; skipping hard-link creation")
                    return False

                target_dir = os.path.dirname(target_path)
                if target_dir:
                    os.makedirs(target_dir, exist_ok=True)

                try:
                    os.link(source_path, target_path)
                    return True
                except OSError as exc:
                    if exc.errno == errno.EXDEV:
                        log.warning(f"Unable to hard-link duplicate across filesystems ({source_path} -> {target_path})")
                    else:
                        log.warning(f"Failed to hard-link duplicate ({source_path} -> {target_path}): {exc}")
                    return False

            def is_403_error(exc):
                msg = str(exc).lower()
                return "http error 403" in msg or "403 forbidden" in msg or "error 403" in msg or " 403" in msg

            def is_no_formats_error(exc):
                return "no video formats found" in str(exc).lower()

            def _normalize_url(value):
                if not value:
                    return None
                cleaned = value.replace('\\u0026', '&').replace('\\u003d', '=').replace('\\u002F', '/').replace('\\/', '/')
                return cleaned

            def _extract_watch_id(value):
                if not value:
                    return None
                parsed = urlparse(value)
                query_id = parse_qs(parsed.query).get('v', [None])[0]
                if query_id:
                    return query_id
                if parsed.netloc.endswith('youtu.be'):
                    path_id = parsed.path.strip('/').split('/')[0]
                    return path_id or None
                return None

            def _safe_search(pattern, text, flags=0):
                try:
                    return re.search(pattern, text, flags)
                except re.error as exc:
                    snippet = pattern if len(pattern) <= 120 else pattern[:117] + "..."
                    log.debug(f"Regex search failed for pattern {snippet!r}: {exc}")
                    return None

            def _safe_finditer(pattern, text):
                try:
                    return list(re.finditer(pattern, text))
                except re.error as exc:
                    snippet = pattern if len(pattern) <= 120 else pattern[:117] + "..."
                    log.debug(f"Regex finditer failed for pattern {snippet!r}: {exc}")
                    return []

            def _extract_js_object(html, marker):
                idx = html.find(marker)
                if idx == -1:
                    return None
                start = html.find('{', idx)
                if start == -1:
                    return None
                depth = 0
                in_string = False
                escape = False
                for pos in range(start, len(html)):
                    char = html[pos]
                    if in_string:
                        if escape:
                            escape = False
                        elif char == '\\':
                            escape = True
                        elif char == '"':
                            in_string = False
                    else:
                        if char == '"':
                            in_string = True
                        elif char == '{':
                            depth += 1
                        elif char == '}':
                            depth -= 1
                            if depth == 0:
                                return html[start:pos + 1]
                return None

            def _load_json_blob(blob, label):
                if not blob:
                    return None
                try:
                    return json.loads(blob)
                except Exception as exc:
                    js_to_json = getattr(yt_dlp.utils, 'js_to_json', None)
                    if not js_to_json:
                        log.debug(f"JSON parse failed for {label}: {exc}")
                        return None
                    try:
                        return json.loads(js_to_json(blob))
                    except Exception as exc2:
                        log.debug(f"JS-to-JSON parse failed for {label}: {exc2}")
                        return None

            def _collect_watch_ids(obj, ids=None):
                if ids is None:
                    ids = []
                if isinstance(obj, dict):
                    watch_endpoint = obj.get('watchEndpoint')
                    if isinstance(watch_endpoint, dict):
                        video_id = watch_endpoint.get('videoId')
                        if isinstance(video_id, str) and len(video_id) == 11 and video_id not in ids:
                            ids.append(video_id)
                    video_id = obj.get('videoId')
                    if isinstance(video_id, str) and len(video_id) == 11 and video_id not in ids:
                        ids.append(video_id)
                    for value in obj.values():
                        _collect_watch_ids(value, ids)
                elif isinstance(obj, list):
                    for value in obj:
                        _collect_watch_ids(value, ids)
                return ids

            def resolve_redirect_url(cookiefile=None):
                original_url = self.info.url
                original_id = _extract_watch_id(original_url)
                original_base = None
                if original_url:
                    parsed_original = urlparse(original_url)
                    if parsed_original.scheme and parsed_original.netloc:
                        original_base = f"{parsed_original.scheme}://{parsed_original.netloc}"
                base = original_base or ('https://music.youtube.com' if 'music.' in (original_url or '') else 'https://www.youtube.com')
                try:
                    params = dict(ytdl_params)
                    if cookiefile:
                        params['cookiefile'] = cookiefile
                    ydl = yt_dlp.YoutubeDL(params=params)
                    with ydl.urlopen(original_url) as resp:
                        final_url = resp.geturl() if hasattr(resp, 'geturl') else None
                        html = resp.read(2_000_000).decode('utf-8', 'ignore')
                except Exception as exc:
                    log.debug(f"Redirect check failed for {original_url}: {exc}")
                    return None, None

                candidates = []
                secondary_candidates = []
                candidate_url_map = {}
                id_counts = {}

                def add_id(video_id, weight=1, url=None):
                    if not video_id or video_id == original_id:
                        return
                    id_counts[video_id] = id_counts.get(video_id, 0) + weight
                    if url and video_id not in candidate_url_map:
                        candidate_url_map[video_id] = url
                if final_url:
                    candidates.append(final_url)
                canonical_match = _safe_search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)', html, re.IGNORECASE)
                if canonical_match:
                    candidates.append(canonical_match.group(1))
                meta_refresh_match = _safe_search(r'http-equiv=["\']refresh["\'][^>]+content=["\'][^"\']*url=([^"\']+)', html, re.IGNORECASE)
                if meta_refresh_match:
                    candidates.append(meta_refresh_match.group(1))
                location_match = _safe_search(r'window\.location(?:\.replace)?\(["\']([^"\']+)["\']\)', html)
                if location_match:
                    candidates.append(location_match.group(1))
                url_canonical_match = _safe_search(r'"urlCanonical"\s*:\s*"([^"]+)"', html)
                if url_canonical_match:
                    candidates.append(url_canonical_match.group(1))
                findall_urls = getattr(yt_dlp.utils, 'findall_urls', None)
                if callable(findall_urls):
                    for found_url in findall_urls(html):
                        if 'watch?v=' in found_url:
                            secondary_candidates.append(found_url)
                else:
                    for match in _safe_finditer(r'https?://[^\s"\'<>]+', html):
                        found_url = match.group(0)
                        if 'watch?v=' in found_url:
                            secondary_candidates.append(found_url)
                player_response = _load_json_blob(_extract_js_object(html, 'ytInitialPlayerResponse'), 'ytInitialPlayerResponse')
                if player_response:
                    microformat = player_response.get('microformat', {}).get('playerMicroformatRenderer', {})
                    canonical_url = microformat.get('canonicalUrl') or microformat.get('urlCanonical')
                    if canonical_url:
                        candidates.append(canonical_url)
                    playability = player_response.get('playabilityStatus', {})
                    for video_id in _collect_watch_ids(playability):
                        candidates.append(f"{base}/watch?v={video_id}")
                initial_endpoint_match = _safe_search(r'"INITIAL_ENDPOINT"\s*:\s*"((?:\\.|[^"\\])*)"', html)
                if initial_endpoint_match:
                    raw_initial = initial_endpoint_match.group(1)
                    try:
                        initial_json_str = json.loads(f"\"{raw_initial}\"")
                        initial_data = json.loads(initial_json_str)
                        video_id = (initial_data.get('watchEndpoint') or {}).get('videoId')
                        if video_id and original_base:
                            candidates.append(f"{original_base}/watch?v={video_id}")
                    except Exception as exc:
                        log.debug(f"INITIAL_ENDPOINT parse failed for {original_url}: {exc}")
                    id_match = _safe_search(r'videoId\\\\":\\\\\"([A-Za-z0-9_-]{11})', raw_initial)
                    if not id_match:
                        id_match = _safe_search(r'videoId\\":\\"([A-Za-z0-9_-]{11})', raw_initial)
                    if not id_match:
                        id_match = _safe_search(r'videoId":"([A-Za-z0-9_-]{11})', raw_initial)
                    if id_match and original_base:
                        candidates.append(f"{original_base}/watch?v={id_match.group(1)}")

                for pattern in (
                    r'"watchEndpoint"\s*:\s*\{[^}]*"videoId"\s*:\s*"([A-Za-z0-9_-]{11})"',
                    r'watchEndpoint\\\\":\\\\\\{[^}]*?videoId\\\\":\\\\\\\\\"([A-Za-z0-9_-]{11})',
                    r'https?://(?:music\\.)?youtube\\.com/watch\\?v=([A-Za-z0-9_-]{11})',
                    r'/watch\\?v=([A-Za-z0-9_-]{11})',
                    r'"videoId"\s*:\s*"([A-Za-z0-9_-]{11})"',
                    r'videoId\\":\\"([A-Za-z0-9_-]{11})',
                    r'videoId\\\\":\\\\\"([A-Za-z0-9_-]{11})',
                    r'videoId\\\\":\\\\\\\\\"([A-Za-z0-9_-]{11})',
                ):
                    for match in _safe_finditer(pattern, html):
                        add_id(match.group(1), weight=1)

                initial_data = _load_json_blob(_extract_js_object(html, 'ytInitialData'), 'ytInitialData')
                if not initial_data:
                    initial_data = _load_json_blob(_extract_js_object(html, 'YTMUSIC_INITIAL_DATA'), 'YTMUSIC_INITIAL_DATA')
                if initial_data:
                    for video_id in _collect_watch_ids(initial_data):
                        add_id(video_id, weight=1)

                for candidate in candidates + secondary_candidates:
                    candidate = _normalize_url(candidate)
                    if not candidate:
                        continue
                    if candidate.startswith('/'):
                        candidate = base + candidate
                    candidate_id = _extract_watch_id(candidate)
                    if candidate_id:
                        add_id(candidate_id, weight=5, url=candidate)

                if log.isEnabledFor(logging.DEBUG):
                    log.debug(f"Redirect candidates for {original_url}: {len(candidates)}")
                    if id_counts:
                        preview = ", ".join(f"{vid}:{id_counts[vid]}" for vid in list(id_counts.keys())[:5])
                        log.debug(f"Redirect candidate ids for {original_url}: {preview}")
                if not id_counts:
                    return None, None
                best_id = max(id_counts.items(), key=lambda item: item[1])[0]
                best_url = candidate_url_map.get(best_id, f"{base}/watch?v={best_id}")
                return best_url, best_id

            def retry_delay(attempt):
                schedule = [2, 5, 8, 13, 21]
                if attempt < len(schedule):
                    return schedule[attempt]
                return schedule[-1] + 5 * (attempt - len(schedule) + 1)

            if getattr(self.info, 'skip_existing_downloads', False):
                target_path, existing_path, initial_video_id = resolve_expected_paths(download_url)
                if existing_path:
                    log.info(f"Skipping download; file already exists at {existing_path}")
                    self.status_queue.put({'status': 'finished', 'filename': existing_path})
                    return

                is_audio = getattr(self.info, 'quality', None) == 'audio' or getattr(self.info, 'format', None) in AUDIO_FORMATS
                if is_audio and target_path:
                    candidate_video_ids = []

                    def _add_candidate_video_id(value):
                        if value and value not in candidate_video_ids:
                            candidate_video_ids.append(value)

                    _add_candidate_video_id(initial_video_id)
                    _add_candidate_video_id(_extract_watch_id(getattr(self.info, 'redirect_url', None)))

                    duplicate_source_path = None
                    duplicate_video_id = None
                    for candidate_video_id in candidate_video_ids:
                        duplicate_source_path = find_audio_duplicate_path_by_video_id(candidate_video_id, target_path=target_path)
                        if duplicate_source_path:
                            duplicate_video_id = candidate_video_id
                            break

                    resolved_redirect_url = None
                    if not duplicate_source_path:
                        redirect_url, redirect_id = resolve_redirect_url()
                        if not redirect_url and self.cookiefile_fallback:
                            redirect_url, redirect_id = resolve_redirect_url(cookiefile=self.cookiefile_fallback)
                        if redirect_id:
                            _add_candidate_video_id(redirect_id)
                            duplicate_source_path = find_audio_duplicate_path_by_video_id(redirect_id, target_path=target_path)
                            if duplicate_source_path:
                                duplicate_video_id = redirect_id
                                resolved_redirect_url = redirect_url

                    if duplicate_source_path and hard_link_audio_duplicate(duplicate_source_path, target_path):
                        if resolved_redirect_url and self.status_queue is not None:
                            self.status_queue.put({'redirect_url': resolved_redirect_url})
                        log.info(f"Skipping download; hard-linked duplicate for video id {duplicate_video_id}: {target_path}")
                        self.status_queue.put({'status': 'finished', 'filename': target_path})
                        return

            try:
                max_retries = int(getattr(self.info, 'retry_403_max', 0) or 0)
            except (TypeError, ValueError):
                max_retries = 0
            max_retries = max(0, max_retries)
            attempt = 0
            cookie_retry_used = False
            redirect_retry_used = False
            attempt_index = 0

            while True:
                attempt_index += 1
                log.debug(f"Download attempt {attempt_index} for {self.info.title} (cookies={'on' if ytdl_params.get('cookiefile') else 'off'}).")
                try:
                    ret = yt_dlp.YoutubeDL(params=ytdl_params).download([download_url])
                    self.status_queue.put({'status': 'finished' if ret == 0 else 'error'})
                    log.info(f"Finished download for: {self.info.title}")
                    return
                except yt_dlp.utils.YoutubeDLError as exc:
                    if is_no_formats_error(exc):
                        if not redirect_retry_used:
                            redirect_url, redirect_id = resolve_redirect_url()
                            if not redirect_url and self.cookiefile_fallback and not cookie_retry_used:
                                redirect_url, redirect_id = resolve_redirect_url(cookiefile=self.cookiefile_fallback)
                            if redirect_url:
                                download_url = redirect_url
                                if self.status_queue is not None:
                                    self.status_queue.put({'redirect_url': redirect_url})
                                redirect_retry_used = True
                                log.warning(f"No video formats found for {self.info.title}. Retrying with redirected URL {redirect_url}.")
                                continue
                        if self.cookiefile_fallback and not cookie_retry_used:
                            ytdl_params['cookiefile'] = self.cookiefile_fallback
                            cookie_retry_used = True
                            if self.status_queue is not None:
                                self.status_queue.put({'used_cookies': True})
                            log.warning(f"No video formats found for {self.info.title}. Retrying with cookies.")
                            continue
                    if is_403_error(exc) and attempt < max_retries:
                        delay = retry_delay(attempt)
                        attempt += 1
                        log.warning(f"HTTP 403 for {self.info.title}. Retrying {attempt}/{max_retries} in {delay}s.")
                        time.sleep(delay)
                        continue
                    log.error(f"Download error for {self.info.title}: {str(exc)}")
                    self.status_queue.put({'status': 'error', 'msg': str(exc)})
                    return
        except Exception as exc:
            log.error(f"Unexpected download error for {self.info.title}: {str(exc)}")
            self.status_queue.put({'status': 'error', 'msg': str(exc)})

    async def start(self, notifier):
        log.info(f"Preparing download for: {self.info.title}")
        if Download.manager is None:
            Download.manager = multiprocessing.Manager()
        self.status_queue = Download.manager.Queue()
        self.proc = multiprocessing.Process(target=self._download)
        self.proc.start()
        self.loop = asyncio.get_running_loop()
        self.notifier = notifier
        self.info.status = 'preparing'
        await self.notifier.updated(self.info)
        self.status_task = asyncio.create_task(self.update_status())
        await self.loop.run_in_executor(None, self.proc.join)
        # Signal update_status to stop and wait for it to finish
        # so that all status updates (including MoveFiles with correct
        # file size) are processed before _post_download_cleanup runs.
        if self.status_queue is not None:
            self.status_queue.put(None)
        await self.status_task

    def cancel(self):
        log.info(f"Cancelling download: {self.info.title}")
        if self.running():
            try:
                self.proc.kill()
            except Exception as e:
                log.error(f"Error killing process for {self.info.title}: {e}")
        self.canceled = True
        if self.status_queue is not None:
            self.status_queue.put(None)

    def close(self):
        log.info(f"Closing download process for: {self.info.title}")
        if self.started():
            self.proc.close()

    def running(self):
        try:
            return self.proc is not None and self.proc.is_alive()
        except ValueError:
            return False

    def started(self):
        return self.proc is not None

    async def update_status(self):
        while True:
            status = await self.loop.run_in_executor(None, self.status_queue.get)
            if status is None:
                log.info(f"Status update finished for: {self.info.title}")
                return
            if self.canceled:
                log.info(f"Download {self.info.title} is canceled; stopping status updates.")
                return
            self.tmpfilename = status.get('tmpfilename')
            if 'redirect_url' in status:
                self.info.redirect_url = status.get('redirect_url')
            if 'used_cookies' in status:
                self.info.used_cookies = bool(status.get('used_cookies'))
            if 'filename' in status:
                fileName = status.get('filename')
                self.info.filename = os.path.relpath(fileName, self.download_dir)
                self.info.size = os.path.getsize(fileName) if os.path.exists(fileName) else None
                if self.info.format == 'thumbnail':
                    self.info.filename = re.sub(r'\.webm$', '.jpg', self.info.filename)

            # Handle chapter files
            log.debug(f"Update status for {self.info.title}: {status}")
            if 'chapter_file' in status:
                chapter_file = status.get('chapter_file')
                if not hasattr(self.info, 'chapter_files'):
                    self.info.chapter_files = []
                rel_path = os.path.relpath(chapter_file, self.download_dir)
                file_size = os.path.getsize(chapter_file) if os.path.exists(chapter_file) else None
                #Postprocessor hook called multiple times with chapters. Only insert if not already present.
                existing = next((cf for cf in self.info.chapter_files if cf['filename'] == rel_path), None)
                if not existing:
                    self.info.chapter_files.append({'filename': rel_path, 'size': file_size})
                # Skip the rest of status processing for chapter files
                continue

            if 'status' not in status:
                await self.notifier.updated(self.info)
                continue
            self.info.status = status['status']
            self.info.msg = status.get('msg')
            if 'downloaded_bytes' in status:
                total = status.get('total_bytes') or status.get('total_bytes_estimate')
                if total:
                    self.info.percent = status['downloaded_bytes'] / total * 100
            self.info.speed = status.get('speed')
            self.info.eta = status.get('eta')
            log.debug(f"Updating status for {self.info.title}: {status}")
            await self.notifier.updated(self.info)

class PersistentQueue:
    def __init__(self, name, path):
        self.identifier = name
        pdir = os.path.dirname(path)
        if not os.path.isdir(pdir):
            os.mkdir(pdir)
        with shelve.open(path, 'c'):
            pass

        self.path = path
        self.repair()
        self.dict = OrderedDict()

    def load(self):
        for k, v in self.saved_items():
            _ensure_download_id(v, k)
            self.dict[k] = Download(None, None, None, None, None, None, {}, v)

    def exists(self, key):
        return key in self.dict

    def get(self, key):
        return self.dict[key]

    def items(self):
        return self.dict.items()

    def saved_items(self):
        with shelve.open(self.path, 'r') as shelf:
            items = list(shelf.items())
        for k, v in items:
            _ensure_download_id(v, k)
        return sorted(items, key=lambda item: item[1].timestamp)

    def put(self, value):
        key = _ensure_download_id(value.info, value.info.url)
        self.dict[key] = value
        with shelve.open(self.path, 'w') as shelf:
            shelf[key] = value.info

    def delete(self, key):
        if key in self.dict:
            del self.dict[key]
            with shelve.open(self.path, 'w') as shelf:
                shelf.pop(key, None)

    def next(self):
        k, v = next(iter(self.dict.items()))
        return k, v

    def empty(self):
        return not bool(self.dict)

    def repair(self):
        # check DB format
        type_check = subprocess.run(
            ["file", self.path],
            capture_output=True,
            text=True
        )
        db_type = type_check.stdout.lower()

        # create backup (<queue>.old)
        try:
            shutil.copy2(self.path, f"{self.path}.old")
        except Exception as e:
            # if we cannot backup then its not safe to attempt a repair
            #  since it could be due to a filesystem error
            log.debug(f"PersistentQueue:{self.identifier} backup failed, skipping repair")
            return

        if "gnu dbm" in db_type:
            # perform gdbm repair
            log_prefix = f"PersistentQueue:{self.identifier} repair (dbm/file)"
            log.debug(f"{log_prefix} started")
            try:
                result = subprocess.run(
                    ["gdbmtool", self.path],
                    input="recover verbose summary\n",
                    text=True,
                    capture_output=True,
                    timeout=60
                )
                log.debug(f"{log_prefix} {result.stdout}")
                if result.stderr:
                    log.debug(f"{log_prefix} failed: {result.stderr}")
            except FileNotFoundError:
                log.debug(f"{log_prefix} failed: 'gdbmtool' was not found")

            # perform null key cleanup
            log_prefix = f"PersistentQueue:{self.identifier} repair (null keys)"
            log.debug(f"{log_prefix} started")
            deleted = 0
            try:
                with dbm.open(self.path, "w") as db:
                    for key in list(db.keys()):
                        if len(key) > 0 and all(b == 0x00 for b in key):
                            log.debug(f"{log_prefix} deleting key of length {len(key)} (all NUL bytes)")
                            del db[key]
                            deleted += 1
                log.debug(f"{log_prefix} done - deleted {deleted} key(s)")
            except dbm.error:
                log.debug(f"{log_prefix} failed: db type is dbm.gnu, but the module is not available (dbm.error; module support may be missing or the file may be corrupted)")

        elif "sqlite" in db_type:
            # perform sqlite3 recovery
            log_prefix = f"PersistentQueue:{self.identifier} repair (sqlite3/file)"
            log.debug(f"{log_prefix} started")
            try:
                result = subprocess.run(
                    f"sqlite3 {self.path} '.recover' | sqlite3 {self.path}.tmp",
                    capture_output=True,
                    text=True,
                    shell=True,
                    timeout=60
                )
                if result.stderr:
                    log.debug(f"{log_prefix} failed: {result.stderr}")
                else:
                    shutil.move(f"{self.path}.tmp", self.path)
                    log.debug(f"{log_prefix}{result.stdout or ' was successful, no output'}")
            except FileNotFoundError:
                log.debug(f"{log_prefix} failed: 'sqlite3' was not found")

class DownloadQueue:
    def __init__(self, config, notifier):
        self.config = config
        self.notifier = notifier
        self.retry_403_max = self._parse_retry_403_max()
        self.request_cookiefile_refs = {}
        self.queue = PersistentQueue("queue", self.config.STATE_DIR + '/queue')
        self.done = PersistentQueue("completed", self.config.STATE_DIR + '/completed')
        self.pending = PersistentQueue("pending", self.config.STATE_DIR + '/pending')
        self.active_downloads = set()
        self.semaphore = asyncio.Semaphore(int(self.config.MAX_CONCURRENT_DOWNLOADS))
        self.done.load()

    def _parse_retry_403_max(self):
        try:
            value = int(self.config.RETRY_403_MAX)
        except (TypeError, ValueError):
            log.warning(f'Invalid RETRY_403_MAX value "{self.config.RETRY_403_MAX}", defaulting to 0')
            return 0
        return max(0, value)

    def _acquire_request_cookiefile(self, path):
        if not path:
            return
        self.request_cookiefile_refs[path] = self.request_cookiefile_refs.get(path, 0) + 1

    def _release_request_cookiefile(self, path):
        if not path:
            return
        remaining = self.request_cookiefile_refs.get(path, 0) - 1
        if remaining <= 0:
            self.request_cookiefile_refs.pop(path, None)
            _safe_unlink(path, label='request cookie file')
            return
        self.request_cookiefile_refs[path] = remaining

    def _has_playlist_entry(self, playlist_id, entry_id):
        if not entry_id:
            return False
        playlist_key = playlist_id or ''
        for _, dl in list(self.queue.items()) + list(self.pending.items()) + list(self.done.items()):
            entry = getattr(dl.info, 'entry', None)
            if not isinstance(entry, dict):
                continue
            existing_playlist_id = entry.get('playlist') or entry.get('playlist_id') or ''
            existing_entry_id = entry.get('id')
            if existing_playlist_id == playlist_key and existing_entry_id == entry_id:
                return True
        return False

    async def __import_queue(self):
        for k, v in self.queue.saved_items():
            _ensure_download_id(v, k)
            await self.__add_download(v, True)

    async def __import_pending(self):
        for k, v in self.pending.saved_items():
            _ensure_download_id(v, k)
            await self.__add_download(v, False)

    async def initialize(self):
        log.info("Initializing DownloadQueue")
        asyncio.create_task(self.__import_queue())
        asyncio.create_task(self.__import_pending())

    async def __start_download(self, download):
        if download.canceled:
            log.info(f"Download {download.info.title} was canceled, skipping start.")
            return
        async with self.semaphore:
            if download.canceled:
                log.info(f"Download {download.info.title} was canceled, skipping start.")
                return
            await download.start(self.notifier)
            self._post_download_cleanup(download)

    def _post_download_cleanup(self, download):
        if download.info.status != 'finished':
            if download.tmpfilename and os.path.isfile(download.tmpfilename):
                try:
                    os.remove(download.tmpfilename)
                except:
                    pass
            download.info.status = 'error'
        self._release_request_cookiefile(getattr(download, 'request_cookiefile', None))
        download.close()
        dl_key = _ensure_download_id(download.info, download.info.url)
        if self.queue.exists(dl_key):
            self.queue.delete(dl_key)
            if download.canceled:
                asyncio.create_task(self.notifier.canceled(dl_key))
            else:
                self.done.put(download)
                asyncio.create_task(self.notifier.completed(download.info))

    def __extract_info(self, url, request_cookiefile=None):
        debug_logging = logging.getLogger().isEnabledFor(logging.DEBUG)
        base_opts = dict(self.config.YTDL_OPTIONS)
        if request_cookiefile:
            base_opts['cookiefile'] = request_cookiefile
        cookiefile = None
        if self.config.COOKIEFILE_ON_NO_FORMATS_ONLY:
            cookiefile = base_opts.pop('cookiefile', None)
            if cookiefile:
                parsed = urlparse(url)
                query_list = parse_qs(parsed.query).get('list', [])
                is_liked_music = any(value == 'LM' for value in query_list)
                if is_liked_music:
                    base_opts['cookiefile'] = cookiefile
                    log.debug("Extract info with cookies for liked music playlist (list=LM).")
                else:
                    log.debug("Extract info without cookies (COOKIEFILE_ON_NO_FORMATS_ONLY enabled).")

        def build_params(opts):
            params = {
                'quiet': not debug_logging,
                'verbose': debug_logging,
                'no_color': True,
                'extract_flat': True,
                'ignore_no_formats_error': True,
                'noplaylist': True,
                'paths': {"home": self.config.DOWNLOAD_DIR, "temp": self.config.TEMP_DIR},
                **opts,
            }
            if 'impersonate' in opts:
                params['impersonate'] = yt_dlp.networking.impersonate.ImpersonateTarget.from_str(opts['impersonate'])
            return params

        def extract_with(opts):
            return yt_dlp.YoutubeDL(params=build_params(opts)).extract_info(url, download=False)

        try:
            return extract_with(base_opts)
        except yt_dlp.utils.YoutubeDLError as exc:
            msg = str(exc).lower()
            if cookiefile and "no video formats found" in msg:
                log.warning("No video formats found during extract_info. Retrying with cookies.")
                base_opts['cookiefile'] = cookiefile
                return extract_with(base_opts)
            raise

    def __calc_download_path(self, quality, format, folder):
        base_directory = self.config.DOWNLOAD_DIR if (quality != 'audio' and format not in AUDIO_FORMATS) else self.config.AUDIO_DOWNLOAD_DIR
        if folder:
            if not self.config.CUSTOM_DIRS:
                return None, {'status': 'error', 'msg': 'A folder for the download was specified but CUSTOM_DIRS is not true in the configuration.'}
            dldirectory = os.path.realpath(os.path.join(base_directory, folder))
            real_base_directory = os.path.realpath(base_directory)
            if not dldirectory.startswith(real_base_directory):
                return None, {'status': 'error', 'msg': f'Folder "{folder}" must resolve inside the base download directory "{real_base_directory}"'}
            if not os.path.isdir(dldirectory):
                if not self.config.CREATE_CUSTOM_DIRS:
                    return None, {'status': 'error', 'msg': f'Folder "{folder}" for download does not exist inside base directory "{real_base_directory}", and CREATE_CUSTOM_DIRS is not true in the configuration.'}
                os.makedirs(dldirectory, exist_ok=True)
        else:
            dldirectory = base_directory
        return dldirectory, None

    async def __add_download(self, dl, auto_start, request_cookiefile=None):
        if not hasattr(dl, 'skip_existing_downloads'):
            dl.skip_existing_downloads = self.config.SKIP_EXISTING_DOWNLOADS
        if not hasattr(dl, 'retry_403_max'):
            dl.retry_403_max = self.retry_403_max
        dldirectory, error_message = self.__calc_download_path(dl.quality, dl.format, dl.folder)
        if error_message is not None:
            return error_message
        output = self.config.OUTPUT_TEMPLATE if len(dl.custom_name_prefix) == 0 else f'{dl.custom_name_prefix}.{self.config.OUTPUT_TEMPLATE}'
        output_chapter = self.config.OUTPUT_TEMPLATE_CHAPTER
        entry = getattr(dl, 'entry', None)
        if entry is not None and entry.get('playlist_index') is not None:
            if len(self.config.OUTPUT_TEMPLATE_PLAYLIST):
                output = self.config.OUTPUT_TEMPLATE_PLAYLIST
            for property, value in entry.items():
                if property.startswith("playlist"):
                    output = _outtmpl_substitute_field(output, property, value)
        if entry is not None and entry.get('channel_index') is not None:
            if len(self.config.OUTPUT_TEMPLATE_CHANNEL):
                output = self.config.OUTPUT_TEMPLATE_CHANNEL
            for property, value in entry.items():
                if property.startswith("channel"):
                    output = _outtmpl_substitute_field(output, property, value)
        ytdl_options = dict(self.config.YTDL_OPTIONS)
        if request_cookiefile:
            ytdl_options['cookiefile'] = request_cookiefile
        cookiefile_fallback = None
        if self.config.COOKIEFILE_ON_NO_FORMATS_ONLY:
            cookiefile_fallback = ytdl_options.pop('cookiefile', None)
        if request_cookiefile is None and getattr(dl, 'cookiefile_fallback', None) is None:
            dl.cookiefile_fallback = cookiefile_fallback
        resolved_cookiefile_fallback = cookiefile_fallback if request_cookiefile is not None else getattr(dl, 'cookiefile_fallback', None)
        playlist_item_limit = getattr(dl, 'playlist_item_limit', 0)
        if playlist_item_limit > 0:
            log.info(f'playlist limit is set. Processing only first {playlist_item_limit} entries')
            ytdl_options['playlistend'] = playlist_item_limit
        download = Download(
            dldirectory,
            self.config.TEMP_DIR,
            output,
            output_chapter,
            dl.quality,
            dl.format,
            ytdl_options,
            dl,
            audio_download_dir=self.config.AUDIO_DOWNLOAD_DIR,
            state_dir=self.config.STATE_DIR,
            cookiefile_fallback=resolved_cookiefile_fallback,
            request_cookiefile=request_cookiefile,
        )
        if request_cookiefile:
            self._acquire_request_cookiefile(request_cookiefile)
        if auto_start is True:
            self.queue.put(download)
            asyncio.create_task(self.__start_download(download))
        else:
            self.pending.put(download)
        await self.notifier.added(dl)

    async def __add_entry(self, entry, quality, format, folder, custom_name_prefix, playlist_item_limit, auto_start, split_by_chapters, chapter_template, already, request_cookiefile=None):
        if not entry:
            return {'status': 'error', 'msg': "Invalid/empty data was given."}

        error = None
        if "live_status" in entry and "release_timestamp" in entry and entry.get("live_status") == "is_upcoming":
            dt_ts = datetime.fromtimestamp(entry.get("release_timestamp")).strftime('%Y-%m-%d %H:%M:%S %z')
            error = f"Live stream is scheduled to start at {dt_ts}"
        else:
            if "msg" in entry:
                error = entry["msg"]

        etype = entry.get('_type') or 'video'

        if etype.startswith('url'):
            log.debug('Processing as a url')
            return await self.add(entry['url'], quality, format, folder, custom_name_prefix, playlist_item_limit, auto_start, split_by_chapters, chapter_template, already, request_cookiefile=request_cookiefile)
        elif etype == 'playlist' or etype == 'channel':
            log.debug(f'Processing as a {etype}')
            entries = entry['entries']
            # Convert generator to list if needed (for len() and slicing operations)
            if isinstance(entries, types.GeneratorType):
                entries = list(entries)
            log.info(f'playlist detected with {len(entries)} entries')
            if self.config.PLAYLIST_ITEMS_OLDEST_FIRST:
                log.info('Playlist order set to oldest-first; processing newest items last')
                entries.reverse()
            index_digits = len(str(len(entries)))
            results = []
            if playlist_item_limit > 0:
                log.info(f'Item limit is set. Processing only first {playlist_item_limit} entries')
                entries = entries[:playlist_item_limit]
            for index, etr in enumerate(entries, start=1):
                etr["_type"] = "video"
                etr[etype] = entry.get("id") or entry.get("channel_id") or entry.get("channel")
                etr[f"{etype}_index"] = '{{0:0{0:d}d}}'.format(index_digits).format(index)
                for property in ("id", "title", "uploader", "uploader_id"):
                    if property in entry:
                        etr[f"{etype}_{property}"] = entry[property]
                results.append(await self.__add_entry(etr, quality, format, folder, custom_name_prefix, playlist_item_limit, auto_start, split_by_chapters, chapter_template, already, request_cookiefile=request_cookiefile))
            if any(res['status'] == 'error' for res in results):
                return {'status': 'error', 'msg': ', '.join(res['msg'] for res in results if res['status'] == 'error' and 'msg' in res)}
            return {'status': 'ok'}
        elif etype == 'video' or (etype.startswith('url') and 'id' in entry and 'title' in entry):
            log.debug('Processing as a video')
            key = entry.get('webpage_url') or entry['url']
            entry_id = entry.get('id')
            playlist_id = entry.get('playlist') or entry.get('playlist_id')
            if entry_id and self._has_playlist_entry(playlist_id, entry_id):
                log.info(f'Skipping duplicate playlist entry: {entry_id} in playlist {playlist_id or "none"}')
                return {'status': 'ok'}
            if not any(dl.info.url == key for _, dl in self.queue.items()):
                dl = DownloadInfo(entry['id'], entry.get('title') or entry['id'], key, quality, format, folder, custom_name_prefix, error, entry, playlist_item_limit, split_by_chapters, chapter_template, self.config.SKIP_EXISTING_DOWNLOADS, self.retry_403_max)
                add_result = await self.__add_download(dl, auto_start, request_cookiefile=request_cookiefile)
                if add_result is not None:
                    return add_result
            return {'status': 'ok'}
        return {'status': 'error', 'msg': f'Unsupported resource "{etype}"'}

    async def add(self, url, quality, format, folder, custom_name_prefix, playlist_item_limit, auto_start=True, split_by_chapters=False, chapter_template=None, already=None, entry_override=None, request_cookiefile=None):
        if entry_override is None and isinstance(already, dict):
            entry_override = already
            already = None
        if not isinstance(already, set):
            already = set()
        if isinstance(entry_override, dict):
            override_url = entry_override.get('original_url') or entry_override.get('webpage_url') or entry_override.get('url')
            if override_url and url != override_url:
                url = override_url
                entry_override['url'] = override_url
                entry_override['webpage_url'] = override_url
        log.info(f'adding {url}: {quality=} {format=} {already=} {folder=} {custom_name_prefix=} {playlist_item_limit=} {auto_start=} {split_by_chapters=} {chapter_template=} {entry_override is not None=} {request_cookiefile is not None=}')
        if url in already:
            log.info('recursion detected, skipping')
            return {'status': 'ok'}
        else:
            already.add(url)
        try:
            if isinstance(entry_override, dict):
                entry = dict(entry_override)
                if '_type' not in entry:
                    entry['_type'] = 'video'
                return await self.__add_entry(entry, quality, format, folder, custom_name_prefix, playlist_item_limit, auto_start, split_by_chapters, chapter_template, already, request_cookiefile=request_cookiefile)
            try:
                entry = await asyncio.get_running_loop().run_in_executor(None, self.__extract_info, url, request_cookiefile)
            except yt_dlp.utils.YoutubeDLError as exc:
                return {'status': 'error', 'msg': str(exc)}
            return await self.__add_entry(entry, quality, format, folder, custom_name_prefix, playlist_item_limit, auto_start, split_by_chapters, chapter_template, already, request_cookiefile=request_cookiefile)
        finally:
            if request_cookiefile and self.request_cookiefile_refs.get(request_cookiefile, 0) == 0:
                _safe_unlink(request_cookiefile, label='request cookie file')

    async def start_pending(self, ids):
        for id in ids:
            if not self.pending.exists(id):
                log.warn(f'requested start for non-existent download {id}')
                continue
            dl = self.pending.get(id)
            self.queue.put(dl)
            self.pending.delete(id)
            asyncio.create_task(self.__start_download(dl))
        return {'status': 'ok'}

    async def cancel(self, ids):
        for id in ids:
            if self.pending.exists(id):
                dl = self.pending.get(id)
                self._release_request_cookiefile(getattr(dl, 'request_cookiefile', None))
                self.pending.delete(id)
                await self.notifier.canceled(id)
                continue
            if not self.queue.exists(id):
                log.warn(f'requested cancel for non-existent download {id}')
                continue
            if self.queue.get(id).started():
                self.queue.get(id).cancel()
            else:
                dl = self.queue.get(id)
                self._release_request_cookiefile(getattr(dl, 'request_cookiefile', None))
                self.queue.delete(id)
                await self.notifier.canceled(id)
        return {'status': 'ok'}

    async def clear(self, ids):
        for id in ids:
            if not self.done.exists(id):
                log.warn(f'requested delete for non-existent download {id}')
                continue
            if self.config.DELETE_FILE_ON_TRASHCAN:
                dl = self.done.get(id)
                try:
                    dldirectory, _ = self.__calc_download_path(dl.info.quality, dl.info.format, dl.info.folder)
                    os.remove(os.path.join(dldirectory, dl.info.filename))
                except Exception as e:
                    log.warn(f'deleting file for download {id} failed with error message {e!r}')
            self.done.delete(id)
            await self.notifier.cleared(id)
        return {'status': 'ok'}

    def get(self):
        return (list((k, v.info) for k, v in self.queue.items()) +
                list((k, v.info) for k, v in self.pending.items()),
                list((k, v.info) for k, v in self.done.items()))
