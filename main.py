import os
import asyncio
import uvicorn
import unicodedata
import re
import random
import json
import httpx
import time
import io
import subprocess
import tempfile
from urllib.parse import quote_plus, urlparse, parse_qs
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.sessions import StringSession

# 🔧 Pillow con fallback gracioso
try:
    from PIL import Image as _PIL_Image
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_Image = None
    _PIL_AVAILABLE = False
    print("⚠️  Pillow no disponible. Instalar con: pip install Pillow")

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
API_ID             = int(os.getenv("API_ID", "0"))
API_HASH           = os.getenv("API_HASH", "")
SESSION_STRING     = os.getenv("SESSION_STRING", "")
PUBLIC_URL         = os.getenv("PUBLIC_URL", "").rstrip('/')
CHANNEL_IDENTIFIER = '@PEELYE'

# --- PUERTO INTERNO (para que FFmpeg acceda al /stream local) ---
INTERNAL_PORT = int(os.getenv("PORT", 8080))

# --- YOUTUBE BACKUP ---
YOUTUBE_API_KEY    = os.getenv("YOUTUBE_API_KEY", "").strip()

# --- GOOGLE KNOWLEDGE GRAPH ---
GOOGLE_KG_API_KEY  = os.getenv("GOOGLE_KG_API_KEY", "").strip()

# --- TMDB ---
TMDB_API_KEY       = os.getenv("TMDB_API_KEY", "").strip()
TMDB_API_BASE      = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE    = "https://image.tmdb.org/t/p/w500"

# --- TVMaze (gratuita, sin API key) ---
TVMAZE_API_BASE    = "https://api.tvmaze.com"

# --- GEMINI AI ---
GEMINI_API_KEY     = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL       = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_API_URL     = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)

# --- IMAGEN PLACEHOLDER ---
PLACEHOLDER_IMAGE_BASE = (
    "https://blogger.googleusercontent.com/img/b/R29vZ2xl/"
    "AVvXsEh4rh5wpJEnn2Ju-9BAVNsMIKx4AsSvOhyphenhyphenyepiiNTezVnUXgT9qLnEk2YQnwov"
    "zS2DDTNemG17EtXdNFUvo4Q990S8SURYYemyNHQNNPJFB1tdwNfk7Ctk7ndf4Pttq35E9"
    "M5SHHyWVANJ9NPtqYTBbElxiQJqZ-9hOAkQhuOJWQ00MunjG6euBdbjaXGkG/s1536/1000094899.png"
)

# ---------------------------------------------------------------------------
# 📦 CONFIGURACIÓN DE VOLUMEN FLY.IO
# ---------------------------------------------------------------------------
DATA_DIR = "/app/data"
CACHE_FILE = os.path.join(DATA_DIR, "pelis_cache.json")
THUMBS_DIR = os.path.join(DATA_DIR, "thumbnails")

# Crear carpeta de miniaturas solo si no existe
os.makedirs(THUMBS_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# OPTIMIZACIÓN / LÍMITES
# ---------------------------------------------------------------------------
MAX_CONCURRENCY           = max(5,  min(15, int(os.getenv("MAX_CONCURRENCY",           "10"))))
CATALOG_POOL_TTL          = max(60,         int(os.getenv("CATALOG_POOL_TTL",          "600")))
CATALOG_FETCH_CONCURRENCY = max(1,  min(10, int(os.getenv("CATALOG_FETCH_CONCURRENCY", "5"))))
MAX_ENRICH_NEW            = max(10, min(80, int(os.getenv("MAX_ENRICH_NEW",            "25"))))

CACHE_SAVE_EVERY      = 10

# ---------------------------------------------------------------------------
# STREAMING (FIX DEFINITIVO)
# ---------------------------------------------------------------------------
STREAM_CHUNK_SIZE = max(
    64 * 1024,
    min(1024 * 1024, int(os.getenv("STREAM_CHUNK_SIZE", str(512 * 1024))))
)

# ---------------------------------------------------------------------------
# THUMBNAILS
# ---------------------------------------------------------------------------
THUMB_CACHE_TTL = max(60, int(os.getenv("THUMB_CACHE_TTL", "3600")))
THUMB_CACHE_MAX = max(50, min(2000, int(os.getenv("THUMB_CACHE_MAX", "500"))))

# 🔧 Tamaño estándar de miniaturas
TARGET_THUMB_WIDTH  = 500
TARGET_THUMB_HEIGHT = 750

# ---------------------------------------------------------------------------
# 🔧 LÍMITES DE CONCURRENCIA PARA THUMBNAILS
# ---------------------------------------------------------------------------
THUMB_FFMPEG_SEM_LIMIT = max(1, min(6, int(os.getenv("THUMB_FFMPEG_SEM_LIMIT", "3"))))

# ---------------------------------------------------------------------------
# ✅ FFmpeg ajustes solicitados
# ---------------------------------------------------------------------------
FFMPEG_TIMEOUT = float(os.getenv("FFMPEG_TIMEOUT", "15"))

# ---------------------------------------------------------------------------
# ✅ Cache IA (persistente) para reducir 429
# ---------------------------------------------------------------------------
AI_CACHE_KEY = "__ai_cache__"
AI_CACHE_TTL_OK_S    = int(os.getenv("AI_CACHE_TTL_OK_S",    str(30 * 24 * 3600)))
AI_CACHE_TTL_NONE_S  = int(os.getenv("AI_CACHE_TTL_NONE_S",  str(24 * 3600)))
AI_CACHE_TTL_429_S   = int(os.getenv("AI_CACHE_TTL_429_S",   str(6 * 3600)))
AI_CACHE_TTL_ERR_S   = int(os.getenv("AI_CACHE_TTL_ERR_S",   str(30 * 60)))
AI_SEM_LIMIT         = max(1, min(4, int(os.getenv("AI_SEM_LIMIT", "2"))))

# ---------------------------------------------------------------------------
# OPTIMIZACIÓN EXTRA: CACHÉ DE RECIENTES POR CANAL
# ---------------------------------------------------------------------------
SEARCH_CHANNEL_CACHE_TTL          = max(10, int(os.getenv("SEARCH_CHANNEL_CACHE_TTL", "120")))
SEARCH_CHANNEL_CACHE_LIMIT        = max(20, min(200, int(os.getenv("SEARCH_CHANNEL_CACHE_LIMIT", "80"))))
SEARCH_CHANNEL_WARMUP_CONCURRENCY = max(1, min(10, int(os.getenv("SEARCH_CHANNEL_WARMUP_CONCURRENCY", "4"))))
SEARCH_CHANNEL_FETCH_TIMEOUT      = float(os.getenv("SEARCH_CHANNEL_FETCH_TIMEOUT", "2.8"))
CHANNELS_READY_MAX_WAIT_SEARCH    = float(os.getenv("CHANNELS_READY_MAX_WAIT_SEARCH", "6.0"))

# ---------------------------------------------------------------------------
# ✅ MÍNIMO DE RESULTADOS POR CATEGORÍA
# ---------------------------------------------------------------------------
MIN_CATEGORY_RESULTS = 15

# ---------------------------------------------------------------------------
# CANALES DE RESPALDO
# ---------------------------------------------------------------------------
_REQUIRED_CHANNELS = [
    '@animadasssss',
    '@goodanalsex',
    '@pelisdeterror2',
    '@peliculasdetodogeneroo',
    '@PeliculasCristianasBpB',
    '@Peliculas_Cristianas_Caprichos',
    '@peliculasdeanimes1349',
    '@peliculaspridelezz',
    '@peliculasadul',
    '@oldiemovies',
    '@Infantiles_Videos',
    '@kidsvideos',
    '@AnimesFinalizadosHD',
    '@Shin_sekai_animes_en_emision_1',
    '@pelis123anime4611',
    '@dibupelis',
    '@MundoPelisgratis15',
    '@archivotvcinepiuraperu',
    '@adult_swim_peliculas_a',
    '@peliculascristian',
    '@kdramaevery',
    '@kdram3',
    '@Kdamasfinalizadosymas',
    '@kdramasubstitulado',
    '@k_dramas9',
    '@dramaesp',
    '@SportsTV90',
    '@peliculasynoticias',
]


def _dedupe_channels(channels: list) -> list:
    seen, out = set(), []
    for ch in channels:
        ch_clean = (ch or "").strip()
        if not ch_clean:
            continue
        key = ch_clean.lower()
        if key not in seen:
            seen.add(key)
            out.append(ch_clean)
    return out


BACKUP_CHANNELS = _dedupe_channels(_REQUIRED_CHANNELS)

# ---------------------------------------------------------------------------
# MAPA DE GÉNEROS → CANALES
# ---------------------------------------------------------------------------
GENRE_CHANNEL_MAP: dict = {
    "anime":           ["@peliculasdeanimes1349", "@AnimesFinalizadosHD",
                        "@Shin_sekai_animes_en_emision_1", "@pelis123anime4611", "@animadasssss"],
    "animacion":       ["@animadasssss", "@dibupelis", "@peliculasdeanimes1349",
                        "@Infantiles_Videos", "@kidsvideos"],
    "animación":       ["@animadasssss", "@dibupelis", "@peliculasdeanimes1349",
                        "@Infantiles_Videos", "@kidsvideos"],
    "terror":          ["@pelisdeterror2"],
    "horror":          ["@pelisdeterror2"],
    "miedo":           ["@pelisdeterror2"],
    "cristiana":       ["@PeliculasCristianasBpB", "@Peliculas_Cristianas_Caprichos", "@peliculascristian"],
    "cristiano":       ["@PeliculasCristianasBpB", "@Peliculas_Cristianas_Caprichos", "@peliculascristian"],
    "religion":        ["@PeliculasCristianasBpB", "@Peliculas_Cristianas_Caprichos", "@peliculascristian"],
    "religión":        ["@PeliculasCristianasBpB", "@Peliculas_Cristianas_Caprichos", "@peliculascristian"],
    "infantil":        ["@Infantiles_Videos", "@kidsvideos", "@dibupelis", "@animadasssss"],
    "niños":           ["@Infantiles_Videos", "@kidsvideos", "@dibupelis"],
    "ninos":           ["@Infantiles_Videos", "@kidsvideos", "@dibupelis"],
    "kids":            ["@Infantiles_Videos", "@kidsvideos", "@dibupelis"],
    "familia":         ["@Infantiles_Videos", "@kidsvideos", "@peliculasdetodogeneroo"],
    "clasica":         ["@oldiemovies"],
    "clásica":         ["@oldiemovies"],
    "vintage":         ["@oldiemovies"],
    "antigua":         ["@oldiemovies"],
    "adultos":         ["@peliculasadul", "@adult_swim_peliculas_a"],
    "adulto":          ["@peliculasadul", "@adult_swim_peliculas_a"],
    "accion":          ["@PEELYE", "@peliculasdetodogeneroo", "@MundoPelisgratis15"],
    "acción":          ["@PEELYE", "@peliculasdetodogeneroo", "@MundoPelisgratis15"],
    "aventura":        ["@PEELYE", "@peliculasdetodogeneroo", "@MundoPelisgratis15"],
    "drama":           ["@peliculasdetodogeneroo", "@MundoPelisgratis15", "@archivotvcinepiuraperu"],
    "comedia":         ["@peliculasdetodogeneroo", "@MundoPelisgratis15"],
    "romance":         ["@peliculasdetodogeneroo", "@MundoPelisgratis15"],
    "romantica":       ["@peliculasdetodogeneroo", "@MundoPelisgratis15"],
    "romántica":       ["@peliculasdetodogeneroo", "@MundoPelisgratis15"],
    "ciencia ficcion": ["@PEELYE", "@peliculasdetodogeneroo"],
    "ciencia ficción": ["@PEELYE", "@peliculasdetodogeneroo"],
    "sci-fi":          ["@PEELYE", "@peliculasdetodogeneroo"],
    "scifi":           ["@PEELYE", "@peliculasdetodogeneroo"],
    "ficcion":         ["@PEELYE", "@peliculasdetodogeneroo"],
    "ficción":         ["@PEELYE", "@peliculasdetodogeneroo"],
    "suspenso":        ["@peliculasdetodogeneroo", "@PEELYE"],
    "thriller":        ["@peliculasdetodogeneroo", "@PEELYE"],
    "documental":      ["@archivotvcinepiuraperu", "@peliculasdetodogeneroo"],
    "documentary":     ["@archivotvcinepiuraperu", "@peliculasdetodogeneroo"],
    "documentales":    ["@archivotvcinepiuraperu", "@peliculasdetodogeneroo"],
    "lgbt":            ["@peliculaspridelezz"],
    "lgbtq":           ["@peliculaspridelezz"],
    "pride":           ["@peliculaspridelezz"],
    "musical":         ["@peliculasdetodogeneroo"],
    "western":         ["@oldiemovies", "@peliculasdetodogeneroo"],
    "fantasia":        ["@peliculasdetodogeneroo", "@PEELYE"],
    "fantasía":        ["@peliculasdetodogeneroo", "@PEELYE"],
    "fantastica":      ["@peliculasdetodogeneroo", "@PEELYE"],
    "fantástica":      ["@peliculasdetodogeneroo", "@PEELYE"],
    "policial":        ["@peliculasdetodogeneroo", "@PEELYE"],
    "crimen":          ["@peliculasdetodogeneroo", "@PEELYE"],
    "criminal":        ["@peliculasdetodogeneroo", "@PEELYE"],
    "guerra":          ["@peliculasdetodogeneroo", "@PEELYE"],
    "historia":        ["@archivotvcinepiuraperu", "@peliculasdetodogeneroo"],
    "historica":       ["@archivotvcinepiuraperu", "@peliculasdetodogeneroo"],
    "histórica":       ["@archivotvcinepiuraperu", "@peliculasdetodogeneroo"],
    "misterio":        ["@peliculasdetodogeneroo", "@pelisdeterror2"],
    "mystery":         ["@peliculasdetodogeneroo", "@pelisdeterror2"],
    "kdrama":          ["@kdramaevery", "@kdram3", "@Kdamasfinalizadosymas",
                        "@kdramasubstitulado", "@k_dramas9", "@dramaesp"],
    "k-drama":         ["@kdramaevery", "@kdram3", "@Kdamasfinalizadosymas",
                        "@kdramasubstitulado", "@k_dramas9", "@dramaesp"],
    "drama coreano":   ["@kdramaevery", "@kdram3", "@Kdamasfinalizadosymas",
                        "@kdramasubstitulado", "@k_dramas9", "@dramaesp"],
    "deportes":        ["@SportsTV90", "@peliculasynoticias"],
    "sports":          ["@SportsTV90", "@peliculasynoticias"],
    "futbol":          ["@SportsTV90", "@peliculasynoticias"],
    "fútbol":          ["@SportsTV90", "@peliculasynoticias"],
    "general":         ["@peliculasdetodogeneroo", "@MundoPelisgratis15", "@PEELYE"],
}


# ---------------------------------------------------------------------------
# HELPER: canales para un género dado
# ---------------------------------------------------------------------------
def _get_genre_channels(genre: str) -> list:
    g = (genre or "").strip().lower()
    channels = GENRE_CHANNEL_MAP.get(g, [])
    if not channels:
        g_norm = unicodedata.normalize("NFD", g)
        g_norm = "".join(c for c in g_norm if unicodedata.category(c) != "Mn")
        channels = GENRE_CHANNEL_MAP.get(g_norm, [])
    if not channels:
        for key, val in GENRE_CHANNEL_MAP.items():
            k_norm = unicodedata.normalize("NFD", key)
            k_norm = "".join(c for c in k_norm if unicodedata.category(c) != "Mn")
            if g in k_norm or k_norm in g:
                channels = val
                break
    return channels


# ---------------------------------------------------------------------------
# CACHÉ HÍBRIDA: RAM + JSON PERSISTENTE (ahora en /app/data/pelis_cache.json)
# ---------------------------------------------------------------------------
def _ensure_data_dir_exists():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception:
        pass


async def _load_persistent_cache() -> dict:
    _ensure_data_dir_exists()

    def _read():
        try:
            if not os.path.exists(CACHE_FILE):
                return {}
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception as e:
            print(f"⚠️  Error cargando caché persistente: {e}")
            return {}

    return await asyncio.to_thread(_read)


async def _save_persistent_cache(cache_dict: dict) -> None:
    _ensure_data_dir_exists()

    def _write():
        try:
            tmp_path = CACHE_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(cache_dict, f, ensure_ascii=False)
            os.replace(tmp_path, CACHE_FILE)
        except Exception as e:
            print(f"⚠️  Error guardando caché persistente: {e}")

    await asyncio.to_thread(_write)


def normalize_title(title: str) -> str:
    title = (title or "").strip().lower()
    title = unicodedata.normalize("NFD", title)
    title = "".join(c for c in title if unicodedata.category(c) != "Mn")
    title = re.sub(r"\s+", " ", title)
    return title


def _cache_key_from_query(query_title: str, year) -> str:
    base = normalize_title(query_title or "")
    y    = (year or "").strip()
    return f"{base}::{y}" if y else base


# ---------------------------------------------------------------------------
# THUMB CACHE HELPERS
# ---------------------------------------------------------------------------
def _thumb_cache_prune(cache: dict):
    try:
        if len(cache) > THUMB_CACHE_MAX:
            excess = len(cache) - THUMB_CACHE_MAX
            for k in list(cache.keys())[:excess]:
                del cache[k]
    except Exception:
        pass


def _detect_mime_type(data: bytes) -> str:
    if not data:
        return "image/jpeg"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data[:4] == b"RIFF" and b"WEBP" in data[:16]:
        return "image/webp"
    return "image/jpeg"


# ---------------------------------------------------------------------------
# 🔧 Recortar/redimensionar imagen a 500x750 con cover mode
# ---------------------------------------------------------------------------
def _crop_cover_to_poster(image_data: bytes) -> bytes:
    if not _PIL_AVAILABLE or not image_data:
        return image_data
    try:
        img = _PIL_Image.open(io.BytesIO(image_data))
        if img.mode != "RGB":
            img = img.convert("RGB")

        target_w, target_h = TARGET_THUMB_WIDTH, TARGET_THUMB_HEIGHT
        src_w, src_h = img.size

        if src_w == 0 or src_h == 0:
            return image_data

        scale = max(target_w / src_w, target_h / src_h)
        new_w = max(int(src_w * scale), target_w)
        new_h = max(int(src_h * scale), target_h)

        img = img.resize((new_w, new_h), _PIL_Image.LANCZOS)

        left = (new_w - target_w) // 2
        top  = (new_h - target_h) // 2
        img  = img.crop((left, top, left + target_w, top + target_h))

        out = io.BytesIO()
        img.save(out, format="JPEG", quality=88, optimize=True)
        return out.getvalue()
    except Exception as e:
        print(f"⚠️  Error en _crop_cover_to_poster: {e}")
        return image_data


def _build_public_url(path: str) -> str:
    if PUBLIC_URL:
        return f"{PUBLIC_URL}{path}"
    return path


def _extract_ch_from_stream_url(stream_url: str) -> int:
    try:
        if not stream_url:
            return 0
        parsed = urlparse(stream_url)
        qs = parse_qs(parsed.query or "")
        ch_vals = qs.get("ch") or []
        if not ch_vals:
            return 0
        return int(ch_vals[0])
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# ✅ _thumb_url_for_message valida que el ID sea numérico
# ---------------------------------------------------------------------------
def _thumb_url_for_message(message_id, stream_url=None, ch=None):
    if not message_id:
        return None
    try:
        msg_id_int = int(message_id)
    except (ValueError, TypeError):
        return None
    ch_final = 0
    if ch is not None:
        ch_final = int(ch)
    elif stream_url:
        ch_final = _extract_ch_from_stream_url(stream_url)
    return _build_public_url(f"/thumb/{msg_id_int}?ch={ch_final}")


def _is_placeholder_image(url) -> bool:
    u = (url or "").strip()
    return (not u) or (u == PLACEHOLDER_IMAGE_BASE)


# ---------------------------------------------------------------------------
# 🔧 YouTube thumbnail apunta al proxy /ytthumb/{vid}
# ---------------------------------------------------------------------------
def _youtube_thumb_from_stream_url(stream_url):
    try:
        if not stream_url:
            return None
        if "youtube.com/watch" in stream_url:
            parsed = urlparse(stream_url)
            qs = parse_qs(parsed.query or "")
            vid = (qs.get("v") or [None])[0]
            if vid:
                return _build_public_url(f"/ytthumb/{vid}")
        if "youtu.be/" in stream_url:
            vid = stream_url.rstrip("/").split("/")[-1]
            if vid:
                return _build_public_url(f"/ytthumb/{vid}")
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# ✅ Cache IA helpers (Google KG / Gemini) con persistencia
# ---------------------------------------------------------------------------
def _ai_cache_entry_ttl_s(status: str) -> int:
    st = (status or "").lower()
    if st == "ok":
        return AI_CACHE_TTL_OK_S
    if st == "none":
        return AI_CACHE_TTL_NONE_S
    if st == "429":
        return AI_CACHE_TTL_429_S
    if st == "err":
        return AI_CACHE_TTL_ERR_S
    return AI_CACHE_TTL_ERR_S


async def _ai_cache_get(kind: str, key: str):
    try:
        ai_cache = getattr(app.state, "ai_cache", None)
        if not isinstance(ai_cache, dict):
            return None, None

        k = f"{kind}:{key}"
        entry = ai_cache.get(k)
        if not isinstance(entry, dict):
            return None, None

        ts = entry.get("ts")
        status = entry.get("status") or "err"
        ttl_s = _ai_cache_entry_ttl_s(status)

        if not isinstance(ts, (int, float)):
            return None, None

        if (time.time() - float(ts)) > float(ttl_s):
            return None, None

        return entry.get("data"), status
    except Exception:
        return None, None


async def _ai_cache_set(kind: str, key: str, data, status: str):
    try:
        ai_cache = getattr(app.state, "ai_cache", None)
        if not isinstance(ai_cache, dict):
            return
        k = f"{kind}:{key}"
        async with getattr(app.state, "ai_cache_lock", asyncio.Lock()):
            ai_cache[k] = {
                "ts": time.time(),
                "status": (status or "err"),
                "data": data,
            }
            setattr(app.state, "meta_cache_dirty", True)
    except Exception:
        return


def _meta_is_full_enough_for_persist(meta: dict) -> bool:
    try:
        if not isinstance(meta, dict):
            return False
        img = meta.get("imagen_url")
        yr  = meta.get("año")
        syn = meta.get("sinopsis")
        if _is_placeholder_image(img):
            return False
        if not (isinstance(img, str) and img.strip()):
            return False
        if not (isinstance(yr, str) and yr.strip() and len(yr.strip()) >= 4):
            return False
        if not (isinstance(syn, str) and syn.strip()):
            return False
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 🔧 FIX PRINCIPAL: _extract_video_frame — SEEK OPTIMIZADO + TIMEOUT 15s
# ---------------------------------------------------------------------------
async def _extract_video_frame(message_id: int, ch: int) -> bytes | None:
    out_path = None
    try:
        stream_url = f"http://127.0.0.1:{INTERNAL_PORT}/stream/{message_id}?ch={ch}"
        out_path   = tempfile.mktemp(suffix="_thumb.jpg")

        def _run_ffmpeg_via_stream() -> bool:
            strategies = [
                (10, True,  int(FFMPEG_TIMEOUT)),
                ( 5, True,  int(FFMPEG_TIMEOUT)),
                ( 3, True,  int(FFMPEG_TIMEOUT)),
                ( 1, False, int(FFMPEG_TIMEOUT)),
                ( 0, True,  12),
            ]

            http_opts = [
                "-reconnect",          "1",
                "-reconnect_streamed", "1",
                "-reconnect_delay_max","2",
                "-rw_timeout",         "7000000",
                "-probesize",          "1000000",
                "-analyzeduration",    "1000000",
            ]

            for seek_secs, pre_seek, timeout_s in strategies:
                try:
                    if os.path.exists(out_path):
                        os.unlink(out_path)
                except Exception:
                    pass

                if pre_seek:
                    args = [
                        "ffmpeg", "-y",
                        "-hide_banner", "-loglevel", "error",
                        *http_opts,
                        "-ss",       str(seek_secs),
                        "-i",        stream_url,
                        "-frames:v", "1",
                        "-q:v",      "3",
                        "-threads",  "1",
                        "-an",
                        "-f",        "image2",
                        out_path,
                    ]
                else:
                    args = [
                        "ffmpeg", "-y",
                        "-hide_banner", "-loglevel", "error",
                        *http_opts,
                        "-i",        stream_url,
                        "-ss",       str(seek_secs),
                        "-frames:v", "1",
                        "-q:v",      "3",
                        "-threads",  "1",
                        "-an",
                        "-f",        "image2",
                        out_path,
                    ]

                try:
                    subprocess.run(
                        args,
                        capture_output=True,
                        timeout=timeout_s,
                    )

                    if (os.path.exists(out_path)
                            and os.path.getsize(out_path) > 500):
                        seek_type = "pre" if pre_seek else "post"
                        print(
                            f"   ✅ Frame extraído seek={seek_secs}s "
                            f"({seek_type}-seek) para msg {message_id}"
                        )
                        return True

                except FileNotFoundError:
                    print("⚠️  ffmpeg no encontrado. Instalar: apt-get install -y ffmpeg")
                    return False

                except subprocess.TimeoutExpired:
                    print(
                        f"   ⏱️  FFmpeg timeout seek={seek_secs}s "
                        f"(límite={timeout_s}s) msg {message_id}, probando siguiente..."
                    )
                    continue

                except Exception as ex:
                    print(
                        f"   ⚠️  FFmpeg error seek={seek_secs}s "
                        f"msg {message_id}: {ex}"
                    )
                    continue

            return False

        success = await asyncio.to_thread(_run_ffmpeg_via_stream)

        if success and os.path.exists(out_path) and os.path.getsize(out_path) > 500:
            with open(out_path, "rb") as f:
                return f.read()

        if out_path and os.path.exists(out_path):
            try:
                os.unlink(out_path)
            except Exception:
                pass
        return None

    except Exception as e:
        print(f"⚠️  Error en _extract_video_frame (msg {message_id}): {e}")
        return None

    finally:
        if out_path:
            try:
                if os.path.exists(out_path):
                    os.unlink(out_path)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 🔧 _background_frame_extract — EXTRACCIÓN EN SEGUNDO PLANO
# ---------------------------------------------------------------------------
async def _background_frame_extract(
    message_id: int,
    ch:         int,
    cache_key:  str,
    entity,
    done_event: asyncio.Event,
) -> None:
    thumb_data: bytes | None = None
    try:
        message = await client.get_messages(entity, ids=message_id)
        if not message:
            print(f"   ℹ️  Sin miniatura disponible para msg {message_id} (mensaje no encontrado)")
            return

        if hasattr(message, "photo") and message.photo:
            try:
                thumb_data = await asyncio.wait_for(
                    client.download_media(message.photo, bytes),
                    timeout=4.0,
                )
            except Exception:
                thumb_data = None

        if not thumb_data and message.document and message.document.thumbs:
            try:
                thumb_data = await asyncio.wait_for(
                    client.download_media(message.document.thumbs[-1], bytes),
                    timeout=4.0,
                )
            except Exception:
                thumb_data = None

        if not thumb_data:
            is_video = (
                message.document is not None
                and message.file  is not None
                and message.file.mime_type is not None
                and "video" in message.file.mime_type.lower()
            )

            if is_video:
                ffmpeg_sem = getattr(app.state, "ffmpeg_sem", None)
                try:
                    if ffmpeg_sem:
                        async with ffmpeg_sem:
                            thumb_data = await _extract_video_frame(message_id, ch)
                    else:
                        thumb_data = await _extract_video_frame(message_id, ch)

                except Exception as ex:
                    print(f"   ⚠️  Error extrayendo frame msg {message_id}: {ex}")
                    thumb_data = None

        if thumb_data:
            processed   = _crop_cover_to_poster(thumb_data)
            mime        = "image/jpeg"
            thumb_cache = getattr(app.state, "thumb_cache", {})
            async with app.state.thumb_cache_lock:
                _thumb_cache_prune(thumb_cache)
                thumb_cache[cache_key] = (time.monotonic(), processed, mime)
            print(f"   ✅ Miniatura lista (bg) para msg {message_id}")
        else:
            print(f"   ℹ️  Sin miniatura disponible para msg {message_id}")

    except Exception as e:
        print(f"⚠️  Error en _background_frame_extract (msg {message_id}): {e}")

    finally:
        done_event.set()
        in_progress: dict = getattr(app.state, "thumb_in_progress", {})
        in_progress.pop(cache_key, None)


# ---------------------------------------------------------------------------
# 🆕 _prelaunch_thumb_extractions
# ---------------------------------------------------------------------------
async def _prelaunch_thumb_extractions(results: list) -> None:
    try:
        thumb_cache      = getattr(app.state, "thumb_cache",           {})
        in_progress      = getattr(app.state, "thumb_in_progress",      {})
        in_progress_lock = getattr(app.state, "thumb_in_progress_lock",
                                   asyncio.Lock())
        entities         = getattr(app.state, "entities", [getattr(app.state, "entity", None)])

        for r in results:
            msg_id = r.get("id")
            if not msg_id:
                continue

            ch_val    = _extract_ch_from_stream_url(r.get("stream_url", ""))
            cache_key = f"{msg_id}:{ch_val}"

            async with app.state.thumb_cache_lock:
                cached = thumb_cache.get(cache_key)
                if cached:
                    ts, _, _ = cached
                    if time.monotonic() - ts < THUMB_CACHE_TTL:
                        continue

            async with in_progress_lock:
                if cache_key in in_progress:
                    continue

                entity = (
                    entities[ch_val]
                    if (0 <= ch_val < len(entities) and entities[ch_val] is not None)
                    else getattr(app.state, "entity", None)
                )
                if entity is None:
                    continue

                done_event             = asyncio.Event()
                in_progress[cache_key] = done_event

            asyncio.create_task(
                _background_frame_extract(msg_id, ch_val, cache_key, entity, done_event)
            )

    except Exception as e:
        print(f"⚠️  Error en _prelaunch_thumb_extractions: {e}")


# ---------------------------------------------------------------------------
# LIFESPAN
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("📡 Conectando a Telegram...")
    await client.connect()

    app.state.entity               = None
    app.state.entities             = [None]
    app.state.channels_ready       = False
    app.state.meta_cache_lock      = asyncio.Lock()
    app.state.meta_cache_dirty     = False
    app.state.catalog_pool_cache   = {"ts": 0.0, "pool": []}

    app.state.search_channel_media_cache = {}
    app.state.search_channel_cache_locks = {}

    app.state.thumb_cache      = {}
    app.state.thumb_cache_lock = asyncio.Lock()

    app.state.ffmpeg_sem             = asyncio.Semaphore(THUMB_FFMPEG_SEM_LIMIT)
    app.state.thumb_in_progress      = {}
    app.state.thumb_in_progress_lock = asyncio.Lock()

    app.state.ai_cache      = {}
    app.state.ai_cache_lock = asyncio.Lock()
    app.state.ai_sem        = asyncio.Semaphore(AI_SEM_LIMIT)

    app.state.last_persist_save_ts = 0.0

    loaded = await _load_persistent_cache()
    ai_loaded = {}
    if isinstance(loaded, dict) and AI_CACHE_KEY in loaded and isinstance(loaded.get(AI_CACHE_KEY), dict):
        ai_loaded = loaded.get(AI_CACHE_KEY) or {}
        try:
            del loaded[AI_CACHE_KEY]
        except Exception:
            pass

    app.state.meta_cache = loaded if isinstance(loaded, dict) else {}
    app.state.ai_cache   = ai_loaded if isinstance(ai_loaded, dict) else {}

    print(f"🧠 Caché persistente cargada: {len(app.state.meta_cache)} entradas")
    print(f"🤖 Caché IA cargada: {len(app.state.ai_cache)} entradas")
    print(f"⚙️  Semáforo FFmpeg: max={THUMB_FFMPEG_SEM_LIMIT} procesos paralelos")
    print(f"⚙️  Puerto interno para thumbnails: {INTERNAL_PORT}")
    print(f"⚙️  Estrategia seeks: [10s/{int(FFMPEG_TIMEOUT)}s, 5s/{int(FFMPEG_TIMEOUT)}s, 3s/{int(FFMPEG_TIMEOUT)}s, 1s-post/{int(FFMPEG_TIMEOUT)}s, 0s/12s]")
    print(f"⚙️  IA semáforo: max={AI_SEM_LIMIT}")

    try:
        main_entity = await client.get_entity(CHANNEL_IDENTIFIER)
        app.state.entity   = main_entity
        app.state.entities = [main_entity]
        print(f"✅ Canal principal cargado: {main_entity.title}")
    except Exception as e:
        print(f"❌ Error al cargar canal principal: {e}")

    async def _load_backup_channels():
        sem = asyncio.Semaphore(5)

        async def _load_one(ch: str):
            async with sem:
                try:
                    entity = await client.get_entity(ch)
                    print(f"✅ Canal de respaldo cargado: {entity.title}")
                    return entity
                except Exception as ex:
                    print(f"⚠️  No se pudo cargar canal de respaldo {ch}: {ex}")
                    return None

        backup_entities = await asyncio.gather(
            *[_load_one(ch) for ch in BACKUP_CHANNELS]
        )
        app.state.entities    = [app.state.entity] + list(backup_entities)
        app.state.channels_ready = True
        loaded_n = sum(1 for e in app.state.entities if e is not None)
        print(f"✅ Todos los canales cargados: {loaded_n} disponibles")

        async def _warmup_search_cache():
            try:
                sem_w = asyncio.Semaphore(SEARCH_CHANNEL_WARMUP_CONCURRENCY)

                async def _warm_one(i: int, e):
                    if e is None:
                        return
                    async with sem_w:
                        try:
                            await _get_recent_media_cached(i, e, force_refresh=True)
                        except Exception:
                            pass

                entities = getattr(app.state, "entities", [])
                await asyncio.gather(
                    *[_warm_one(i, e) for i, e in enumerate(entities)],
                    return_exceptions=True
                )
                print("⚡ Warm-up de caché por canal completado")
            except Exception as ex:
                print(f"⚠️  Warm-up caché por canal falló: {ex}")

        asyncio.create_task(_warmup_search_cache())

    asyncio.create_task(_load_backup_channels())

    yield

    if getattr(app.state, "meta_cache_dirty", False):
        print("💾 Guardando caché pendiente antes de apagar...")
        to_save = dict(getattr(app.state, "meta_cache", {}) or {})
        to_save[AI_CACHE_KEY] = dict(getattr(app.state, "ai_cache", {}) or {})
        await _save_persistent_cache(to_save)

    await client.disconnect()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


# ---------------------------------------------------------------------------
# HEALTH CHECK
# ---------------------------------------------------------------------------
@app.get("/health")
async def health_check():
    channels_up       = sum(1 for e in getattr(app.state, "entities", []) if e is not None)
    in_progress_count = len(getattr(app.state, "thumb_in_progress", {}))
    return JSONResponse({
        "status":               "ok",
        "channels_ready":       getattr(app.state, "channels_ready", False),
        "channels_loaded":      channels_up,
        "cache_entries":        len(getattr(app.state, "meta_cache", {})),
        "thumb_cache_entries":  len(getattr(app.state, "thumb_cache", {})),
        "thumbs_in_progress":   in_progress_count,
        "internal_port":        INTERNAL_PORT,
    })


# ---------------------------------------------------------------------------
# 🔧 MEJORA 1 & 2: LIMPIEZA AVANZADA DE TÍTULOS Y DETECCIÓN DE AÑO
# ---------------------------------------------------------------------------
_MAX_TITLE_LEN = 100

# Patrones de ruido a eliminar del título
_NOISE_PATTERNS_ADVANCED = [
    # Resoluciones y calidad de video
    r"\b4k\b",
    r"\b2k\b",
    r"\b8k\b",
    r"\b1080p?\b",
    r"\b720p?\b",
    r"\b480p?\b",
    r"\b360p?\b",
    r"\bfull\s*hd\b",
    r"\bhd\b",
    r"\bhdts\b",
    r"\bhdcam\b",
    r"\bhdrip\b",
    r"\bwebdl\b",
    r"\bweb[-\s]?dl\b",
    r"\bweb[-\s]?rip\b",
    r"\bblu[-\s]?ray\b",
    r"\bbluray\b",
    r"\bdvdrip\b",
    r"\bdvdscr\b",
    r"\bbdrip\b",
    r"\bcam\b",
    r"\bts\b",
    r"\bscreener\b",
    # Idiomas y doblaje
    r"\bdoblaje\s+latino\b",
    r"\bdoblado\s+al?\s+espa[ñn]ol\b",
    r"\baudio\s+latino\b",
    r"\baudio\s+espa[ñn]ol\b",
    r"\bsub(?:titulo)?s?\s+espa[ñn]ol\b",
    r"\bsub(?:titulo)?s?\s+español\b",
    r"\bsubtitulado\b",
    r"\bsubtiulado\b",
    r"\bcastellano\b",
    r"\blatino\b",
    r"\bespa[ñn]ol\b",
    r"\bingles\b",
    r"\benglish\b",
    r"\bdual\b",
    r"\bdoblaje\b",
    # Texto extra genérico
    r"\bpel[ií]cula\b",
    r"\bpeliculas?\b",
    r"\bcompleta\b",
    r"\bcompleto\b",
    r"\boficial\b",
    r"\bonline\b",
    r"\bgratis\b",
    r"\bfull\b",
    r"\btrailer\b",
    r"\bremaster(?:ed|izado)?\b",
    r"\bextended\b",
    r"\bdirectors?\s*cut\b",
    r"\bunrated\b",
    r"\bversion\s+extendida\b",
]

# Años válidos: 4 dígitos entre 1900 y año actual + 2
_VALID_YEAR_RANGE = (1900, 2027)


def _extract_year_advanced(text: str):
    """
    Detecta el año de estreno de forma robusta.
    Maneja formatos como: (1993), (1993-1527), 1993, año mezclado con texto.
    Retorna el año como string de 4 dígitos o None.
    """
    if not text:
        return None

    # Patrón: año entre paréntesis, posiblemente con rango (1993-xxxx)
    # Captura el primer año de 4 dígitos en paréntesis
    m = re.search(r'\((\d{4})(?:-\d+)?\)', text)
    if m:
        yr = int(m.group(1))
        if _VALID_YEAR_RANGE[0] <= yr <= _VALID_YEAR_RANGE[1]:
            return str(yr)

    # Patrón: año precedido o seguido de separador
    m = re.search(r'(?:^|[\s\[\(,\-_|])((?:19|20)\d{2})(?:$|[\s\]\),\-_|])', text)
    if m:
        yr = int(m.group(1))
        if _VALID_YEAR_RANGE[0] <= yr <= _VALID_YEAR_RANGE[1]:
            return str(yr)

    # Último recurso: cualquier año de 4 dígitos válido
    all_years = re.findall(r'\b((?:19|20)\d{2})\b', text)
    for y_str in all_years:
        yr = int(y_str)
        if _VALID_YEAR_RANGE[0] <= yr <= _VALID_YEAR_RANGE[1]:
            return str(yr)

    return None


def _advanced_clean_title(raw_title: str) -> tuple:
    """
    Limpieza avanzada de título de Telegram.
    Retorna (titulo_limpio, año_detectado).

    Maneja casos como:
    - "12 nombre película 4k"
    - "37×45 nombre película (1950)"
    - "nombre película (HD) [Latino]"
    - "45. Título Película (2019) 1080p Latino"
    """
    if not raw_title:
        return ("Película", None)

    t = raw_title.strip()

    # 1. Eliminar decoraciones de markdown/links
    t = re.sub(r'\[([^\]]*)\]\(https?://[^\)]*\)', r'\1', t)
    t = re.sub(r'\]\s*\(https?://[^\)]*\)', '', t)
    t = re.sub(r'https?://\S+', '', t)
    t = re.sub(r'[\[\]]', '', t)
    t = re.sub(r"[*_`]+", "", t)
    t = re.sub(r"^[#]+\s*", "", t)

    # 2. Tomar solo la primera línea
    t = t.split('\n')[0].strip()

    # 3. Extraer año ANTES de limpiar (para no perderlo)
    detected_year = _extract_year_advanced(t)

    # 4. Eliminar números al inicio seguidos de punto, paréntesis, corchete o espacio
    #    Ejemplos: "12 título", "37. título", "45) título", "12- título"
    t = re.sub(r'^\d+[\s\.\)\-\]\|×xX]+', '', t).strip()

    # 5. Eliminar dimensiones tipo "37×45" o "1920x1080" al inicio
    t = re.sub(r'^\d+\s*[×xX]\s*\d+\s*', '', t).strip()

    # 6. Eliminar texto entre paréntesis/corchetes que contenga solo ruido
    #    (año, resolución, idioma, etc.) — pero preservar si parece parte del título
    def _remove_noise_brackets(s: str) -> str:
        # Eliminar (1993), (HD), [Latino], [1080p], etc.
        # Preservar si el contenido tiene más de 4 palabras (podría ser subtítulo)
        def _replacer(m):
            inner = m.group(1).strip()
            # Si solo contiene año, resolución o idioma → eliminar
            if re.fullmatch(r'[\d\s\-×xXpkKhHdDrR]+', inner):
                return ' '
            if len(inner.split()) <= 3:
                # Comprobar si es puro ruido
                inner_lower = inner.lower()
                noise_words = [
                    'hd', '4k', '2k', '8k', 'latino', 'castellano', 'español',
                    'ingles', 'english', 'sub', 'dual', 'doblado', 'bluray',
                    'web-dl', 'dvdrip', 'cam', 'ts', 'rip', 'hdts', 'remasterizado',
                ]
                for nw in noise_words:
                    if nw in inner_lower:
                        return ' '
            return m.group(0)  # preservar si no es ruido claro

        s = re.sub(r'\(([^()]*)\)', _replacer, s)
        s = re.sub(r'\[([^\[\]]*)\]', lambda m: _replacer(type('M', (), {'group': lambda self, x: m.group(x) if x else m.group(0)})()),  s)
        return s

    t = _remove_noise_brackets(t)

    # 7. Eliminar el año detectado del título (evita confusión en búsqueda API)
    if detected_year:
        t = re.sub(r'\b' + re.escape(detected_year) + r'\b', ' ', t)

    # 8. Aplicar patrones de ruido avanzados
    for pat in _NOISE_PATTERNS_ADVANCED:
        t = re.sub(pat, ' ', t, flags=re.IGNORECASE)

    # 9. Eliminar separadores y caracteres especiales residuales
    t = re.sub(r'[|•·_,;!?¡¿"\']+', ' ', t)
    # Eliminar paréntesis vacíos o con solo espacios
    t = re.sub(r'\(\s*\)', ' ', t)
    t = re.sub(r'\[\s*\]', ' ', t)

    # 10. Eliminar números sueltos al inicio o al final
    t = re.sub(r'^\d+\s+', '', t).strip()
    t = re.sub(r'\s+\d+$', '', t).strip()

    # 11. Limpiar espacios múltiples y guiones solitarios
    t = re.sub(r'\s*-\s*$', '', t).strip()
    t = re.sub(r'^\s*-\s*', '', t).strip()
    t = re.sub(r'\s+', ' ', t).strip()

    # 12. Truncar si sigue siendo muy largo
    if len(t) > _MAX_TITLE_LEN:
        for sep in (' - ', '. ', ', '):
            idx = t.find(sep, 10)
            if 10 < idx < _MAX_TITLE_LEN:
                t = t[:idx].strip()
                break
        else:
            t = t[:_MAX_TITLE_LEN].strip()

    # 13. Capitalizar correctamente si quedó en minúsculas
    if t and t == t.lower():
        t = t.title()

    return (t.strip() or "Película", detected_year)


# ---------------------------------------------------------------------------
# EXTRACCIÓN DE TÍTULO LIMPIA (mantiene compatibilidad con código existente)
# ---------------------------------------------------------------------------
def _extract_title_from_caption(caption: str) -> str:
    if not caption:
        return "Película"

    first_line = caption.split('\n')[0].strip()

    first_line = re.sub(r'\[([^\]]*)\]\(https?://[^\)]*\)', r'\1', first_line)
    first_line = re.sub(r'\]\s*\(https?://[^\)]*\)', '', first_line)
    first_line = re.sub(r'https?://\S+', '', first_line)
    first_line = re.sub(r'[\[\]]', '', first_line)

    first_line = first_line.strip()
    if len(first_line) > _MAX_TITLE_LEN:
        for sep in ('.', ',', ';', '!', '?', ' - '):
            idx = first_line.find(sep, 15)
            if 15 < idx < _MAX_TITLE_LEN:
                first_line = first_line[:idx].strip()
                break
        else:
            first_line = first_line[:_MAX_TITLE_LEN].strip()

    return first_line.strip() or "Película"


# ---------------------------------------------------------------------------
# 🔧 MEJORA 3: ORDENAMIENTO POR CAPÍTULOS Y SAGAS
# ---------------------------------------------------------------------------

# Palabras clave de sagas conocidas para agrupar
_SAGA_KEYWORDS = [
    "rambo", "rocky", "terminator", "star wars", "batman", "superman",
    "avengers", "iron man", "spider-man", "spiderman", "thor", "hulk",
    "fast furious", "rapidos furiosos", "mission impossible", "james bond",
    "indiana jones", "jurassic", "matrix", "transformers", "alien",
    "predator", "pirates caribbean", "piratas caribe", "el hobbit",
    "el señor de los anillos", "lord of the rings", "harry potter",
    "x-men", "xmen", "bourne", "john wick", "mad max", "el padrino",
    "godfather", "toy story", "shrek", "ice age", "era de hielo",
    "kung fu panda", "cars", "finding nemo", "buscando a nemo",
    "how to train", "dragon", "narnia", "twilight", "crepusculo",
    "saw", "final destination", "destino final", "paranormal activity",
    "halloween", "friday 13th", "viernes 13", "a nightmare on elm street",
    "nightmare elm", "scream", "conjuring", "insidious",
]

_ROMAN_RE = r"(?:I{1,3}|IV|V|VI{0,3}|IX|X{1,3}|XI{0,3}|XIV|XV|XVI{0,3}|XIX|XX)"


def extract_chapter_number(result: dict) -> int:
    """
    Extrae el número de capítulo/parte de una película para ordenamiento.
    Mejorado para detectar sagas y ordenar correctamente.
    """
    title = result.get("titulo") or result.get("title", "")
    title_lower = title.lower()

    # Patrones directos de capítulo/episodio/parte
    patterns = [
        # "Capítulo 3", "Cap. 3", "Ep. 3", "Episodio 3"
        r'(?:cap[ií]tulo|cap[.]?|ep(?:isodio)?[.]?|parte|vol(?:[.]|umen)?)\s*[:\-]?\s*(\d+)',
        # "Parte 2", "Volumen 3"
        r'(?:parte|volumen|vol)\s*[:\-]?\s*(\d+)',
        # Número romano al final: "Rocky III", "Star Wars IV"
        r'\b(' + _ROMAN_RE + r')\s*$',
        # Número al final entre paréntesis o solo: "Rocky 3", "Alien 4"
        r'\s(\d{1,2})\s*(?:$|\(|\[)',
        # Número precedido de nombre de saga: detectar posición
        r'(\d{1,2})$',
    ]

    for i, pat in enumerate(patterns):
        m = re.search(pat, title, re.IGNORECASE)
        if m:
            val = m.group(1)
            # Convertir romano a entero
            roman_map = {
                'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
                'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10,
                'XI': 11, 'XII': 12, 'XIII': 13, 'XIV': 14, 'XV': 15,
                'XVI': 16, 'XVII': 17, 'XVIII': 18, 'XIX': 19, 'XX': 20,
            }
            if val.upper() in roman_map:
                return roman_map[val.upper()]
            try:
                return int(val)
            except ValueError:
                continue

    return 0


def _detect_saga_name(title: str) -> str | None:
    """
    Detecta si un título pertenece a una saga conocida.
    Retorna el nombre normalizado de la saga o None.
    """
    title_norm = normalize_title(title)
    for saga in _SAGA_KEYWORDS:
        saga_norm = normalize_title(saga)
        if saga_norm in title_norm:
            return saga_norm
    return None


def _sort_results_by_saga_and_chapter(results: list) -> list:
    """
    Ordena resultados agrupando sagas y ordenando por número de capítulo.
    Las películas sin saga se mantienen en el orden original.
    """
    # Separar resultados con saga de los sin saga
    saga_groups: dict = {}
    no_saga: list = []

    for r in results:
        title = r.get("titulo") or r.get("title", "")
        saga = _detect_saga_name(title)
        if saga:
            if saga not in saga_groups:
                saga_groups[saga] = []
            saga_groups[saga].append(r)
        else:
            no_saga.append(r)

    # Ordenar dentro de cada saga por número de capítulo
    sorted_with_saga = []
    for saga_name, saga_items in sorted(saga_groups.items()):
        sorted_items = sorted(saga_items, key=extract_chapter_number)
        sorted_with_saga.extend(sorted_items)

    # Combinar: sagas primero (ordenadas), luego el resto
    return sorted_with_saga + no_saga


# ---------------------------------------------------------------------------
# HELPERS: limpieza de títulos (mantiene compatibilidad)
# ---------------------------------------------------------------------------
_NOISE_PATTERNS = [
    r"\bdoblaje\s+latino\b",
    r"\bcastellano\b",
    r"\blatino\b",
    r"\bpel[ií]cula\b",
    r"\bfull\s*hd\b",
    r"\b1080p\b",
    r"\b720p\b",
    r"\bhdts\b",
    r"\bweb[-\s]?dl\b",
    r"\bblu[-\s]?ray\b",
    r"\baudio\s+latino\b",
    r"\bsub\s+espa[ñn]ol\b",
    r"\bsub\s+español\b",
]


def _strip_decorations(title: str) -> str:
    t = (title or "").strip()
    t = re.sub(r'\]\s*\(https?://[^\)]*\)', '', t)
    t = re.sub(r'https?://\S+', '', t)
    t = re.sub(r'[\[\]]', '', t)
    t = re.sub(r"[*_`]+", "", t)
    t = re.sub(r"^[#]+", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _extract_year_from_title(title: str):
    """Mantiene compatibilidad — delega a la versión avanzada."""
    return _extract_year_advanced(title)


def _remove_bracketed_text(s: str) -> str:
    if not s:
        return s
    out = s
    for _ in range(3):
        out = re.sub(r"\([^()]*\)", " ", out)
        out = re.sub(r"\[[^\[\]]*\]", " ", out)
    return out


def _clean_title_for_api(title: str) -> str:
    """
    Versión mejorada: usa _advanced_clean_title para máxima limpieza.
    """
    clean, _ = _advanced_clean_title(title)
    # Eliminar año residual
    clean = re.sub(r'\b(19\d{2}|20\d{2})\b', ' ', clean).strip()
    # Eliminar indicadores de capítulo/temporada para búsqueda
    clean = re.sub(
        rf"\b(?:cap[ií]tulo|cap[.]?|ep(?:isodio)?[.]?|parte|vol(?:[.]|umen)?|"
        rf"temporada|season)\s*[:\-]?\s*(?:\d+|{_ROMAN_RE})\b",
        " ", clean, flags=re.IGNORECASE,
    )
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean or title


def _build_tmdb_query_from_title(title: str):
    """
    Versión mejorada: usa limpieza avanzada.
    """
    # Intentar limpieza avanzada primero
    clean_title, detected_year = _advanced_clean_title(title)

    # Si la limpieza avanzada dio un resultado razonable, usarlo
    q = clean_title

    # Eliminar año de la query (ya lo tenemos separado)
    if detected_year:
        q = re.sub(r'\b' + re.escape(detected_year) + r'\b', ' ', q).strip()

    # Eliminar indicadores de capítulo para la búsqueda API
    q = re.sub(
        rf"\b(?:cap[ií]tulo|cap[.]?|ep(?:isodio)?[.]?|parte|vol(?:[.]|umen)?|"
        rf"temporada|season)\s*[:\-]?\s*(?:\d+|{_ROMAN_RE})\b",
        " ", q, flags=re.IGNORECASE,
    )

    # Fallback al método original si el título quedó muy corto
    if len(q.strip()) < 3:
        raw  = _strip_decorations(title)
        year_fallback = _extract_year_advanced(raw)
        q    = _clean_title_for_api(raw)
        q = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", q).strip()
        return (q or raw), (detected_year or year_fallback)

    q = re.sub(r"\s+", " ", q).strip()
    return (q or clean_title), detected_year


def _placeholder_image_for_title(title: str) -> str:
    return PLACEHOLDER_IMAGE_BASE


def _nn_str(v, default: str = "n/a") -> str:
    """Retorna el valor como string, o 'n/a' si está vacío/None."""
    if v is None:
        return default
    s = v.strip() if isinstance(v, str) else str(v)
    return s if s else default


def _nn_num(v, default=0):
    return v if v is not None else default


# ---------------------------------------------------------------------------
# FETCH + CACHÉ DE RECIENTES POR CANAL
# ---------------------------------------------------------------------------
async def _fetch_recent_media_from_channel(ch_index: int, entity, limit: int) -> list:
    if entity is None:
        return []
    results = []
    async for message in client.iter_messages(entity, limit=limit):
        if message.media and (message.video or message.document):
            caption     = message.text or ""
            title       = _extract_title_from_caption(caption)
            direct_link = (
                f"{PUBLIC_URL}/stream/{message.id}?ch={ch_index}"
                if PUBLIC_URL else f"/stream/{message.id}?ch={ch_index}"
            )
            results.append({
                "id":         message.id,
                "title":      title,
                "size":       (
                    f"{round(message.file.size / (1024 * 1024), 2)} MB"
                    if message.file else "n/a"
                ),
                "stream_url": direct_link,
            })
            if len(results) >= 50:
                break
    return results


async def _get_recent_media_cached(ch_index: int, entity, force_refresh: bool = False) -> list:
    now = time.monotonic()

    cache = getattr(app.state, "search_channel_media_cache", None)
    if not isinstance(cache, dict):
        return []

    if not force_refresh:
        entry = cache.get(ch_index)
        if isinstance(entry, dict):
            ts    = entry.get("ts") or 0.0
            items = entry.get("items") or []
            if items and (now - ts) < SEARCH_CHANNEL_CACHE_TTL:
                return list(items)

    locks = getattr(app.state, "search_channel_cache_locks", {})
    if ch_index not in locks:
        locks[ch_index] = asyncio.Lock()

    async with locks[ch_index]:
        if not force_refresh:
            entry = cache.get(ch_index)
            if isinstance(entry, dict):
                ts    = entry.get("ts") or 0.0
                items = entry.get("items") or []
                if items and (now - ts) < SEARCH_CHANNEL_CACHE_TTL:
                    return list(items)

        try:
            items = await asyncio.wait_for(
                _fetch_recent_media_from_channel(ch_index, entity, SEARCH_CHANNEL_CACHE_LIMIT),
                timeout=SEARCH_CHANNEL_FETCH_TIMEOUT,
            )
        except (asyncio.TimeoutError, Exception) as e:
            print(f"⚠️  Error/timeout fetching channel [{ch_index}]: {e}")
            items = []

        cache[ch_index] = {"ts": time.monotonic(), "items": items}
        return list(items)


# ---------------------------------------------------------------------------
# 🔧 MEJORA 6 & 7: NORMALIZACIÓN DEL ESQUEMA JSON DE RESPUESTA
# Estructura exacta con claves en orden correcto y "n/a" para vacíos
# ---------------------------------------------------------------------------
def _to_peliculas_json_schema(items: list) -> list:
    out = []
    for it in (items or []):
        titulo       = it.get("titulo") or it.get("title") or it.get("nombre") or "Película"
        imagen_url   = it.get("imagen_url") or ""
        pelicula_url = it.get("pelicula_url") or it.get("stream_url") or it.get("url") or ""
        desc         = (it.get("descripcion") or it.get("sinopsis") or "").strip()

        # Extraer campos con fallback a "n/a"
        _fecha    = it.get("fecha_lanzamiento")
        _dur      = it.get("duracion")
        _idioma   = it.get("idioma_original")
        _pop      = it.get("popularidad")
        _punt     = it.get("puntuacion")
        _gen      = it.get("generos")
        _anio     = it.get("año")

        # Helper interno para normalizar campos de texto
        def _field_str(val) -> str:
            if val is None:
                return "n/a"
            s = str(val).strip()
            return s if s and s not in ("", "0", "0.0") else "n/a"

        def _field_num(val) -> str:
            if val is None or val == 0:
                return "n/a"
            try:
                f = float(val)
                return str(round(f, 1)) if f != 0 else "n/a"
            except (ValueError, TypeError):
                return "n/a"

        # Construir objeto con estructura exacta y orden correcto
        obj = {
            "titulo":            _field_str(titulo) if titulo and titulo != "Película" else titulo,
            "imagen_url":        _nn_str(imagen_url, "n/a"),
            "pelicula_url":      _nn_str(pelicula_url, "n/a"),
            "descripcion":       desc if desc and desc != "Sin descripción disponible." else "n/a",
            "fecha_lanzamiento": _field_str(_fecha),
            "duracion":          _field_str(_dur),
            "idioma_original":   _field_str(_idioma),
            "popularidad":       _field_num(_pop),
            "puntuacion":        _field_num(_punt),
            "generos":           _field_str(_gen),
            "año":               _field_str(_anio),
        }

        out.append(obj)
    return out


# ---------------------------------------------------------------------------
# FILTROS AVANZADOS POST-ENRIQUECIMIENTO
# ---------------------------------------------------------------------------
def _apply_advanced_filters(
    results:  list,
    year=None,
    genre=None,
    language=None,
    desde=None,
    hasta=None,
) -> list:
    if not results:
        return results

    genre_norm = (genre or "").strip().lower() if genre else None
    if genre_norm:
        _tmp = unicodedata.normalize("NFD", genre_norm)
        genre_norm_na = "".join(c for c in _tmp if unicodedata.category(c) != "Mn")
    else:
        genre_norm_na = None

    lang_upper = (language or "").strip().upper() if language else None

    filtered = []
    for r in results:
        if year:
            r_year = str(r.get("año") or "")
            if r_year == "n/a":
                r_year = ""
            if not r_year.startswith(year):
                continue

        if desde or hasta:
            r_year_str = str(r.get("año") or "")
            if r_year_str == "n/a":
                r_year_str = ""
            try:
                r_year_int = int(r_year_str[:4]) if len(r_year_str) >= 4 else None
                if r_year_int is not None:
                    if desde and r_year_int < desde:
                        continue
                    if hasta and r_year_int > hasta:
                        continue
            except (ValueError, TypeError):
                pass

        if genre_norm:
            r_gen_raw = (r.get("generos") or "").lower()
            if r_gen_raw == "n/a":
                r_gen_raw = ""
            _tmp2 = unicodedata.normalize("NFD", r_gen_raw)
            r_gen_na = "".join(c for c in _tmp2 if unicodedata.category(c) != "Mn")
            if not (genre_norm in r_gen_raw or genre_norm_na in r_gen_na):
                continue

        if lang_upper:
            r_lang = (r.get("idioma_original") or "").upper()
            if r_lang == "N/A":
                r_lang = ""
            if lang_upper not in r_lang:
                continue

        filtered.append(r)

    return filtered


# ---------------------------------------------------------------------------
# 🔧 MEJORA 5: PARSER DE BÚSQUEDA COMBINADA
# Soporta: nombre, categoría, año, rango de años, combinaciones
# ---------------------------------------------------------------------------
def _parse_combined_query(q: str) -> dict:
    """
    Parsea una query combinada y extrae:
    - texto libre (nombre de película)
    - género/categoría
    - año exacto o rango de años

    Ejemplos:
    - "rambo"                  → {text: "rambo"}
    - "acción 2019 rambo"      → {text: "rambo", genre: "acción", year: "2019"}
    - "animación 2010-2020"    → {genre: "animación", desde: 2010, hasta: 2020}
    - "2019"                   → {year: "2019"}
    - "accion rambo"           → {text: "rambo", genre: "acción"}
    """
    if not q:
        return {}

    result = {}
    remaining = q.strip()

    # 1. Detectar rango de años: "2010-2020" o "2010 2020" (dos años consecutivos)
    range_match = re.search(r'\b((?:19|20)\d{2})\s*[-–]\s*((?:19|20)\d{2})\b', remaining)
    if range_match:
        yr1, yr2 = int(range_match.group(1)), int(range_match.group(2))
        result['desde'] = min(yr1, yr2)
        result['hasta'] = max(yr1, yr2)
        remaining = remaining[:range_match.start()] + remaining[range_match.end():]
        remaining = remaining.strip()

    # 2. Detectar año exacto
    if 'desde' not in result:
        year_match = re.search(r'\b((?:19|20)\d{2})\b', remaining)
        if year_match:
            yr = int(year_match.group(1))
            if _VALID_YEAR_RANGE[0] <= yr <= _VALID_YEAR_RANGE[1]:
                result['year'] = str(yr)
                remaining = remaining[:year_match.start()] + remaining[year_match.end():]
                remaining = remaining.strip()

    # 3. Detectar género/categoría
    # Normalizar el texto restante y buscar géneros conocidos
    remaining_norm = unicodedata.normalize("NFD", remaining.lower())
    remaining_norm = "".join(c for c in remaining_norm if unicodedata.category(c) != "Mn")

    # Ordenar géneros por longitud descendente (para detectar primero los más específicos)
    genre_keys_sorted = sorted(GENRE_CHANNEL_MAP.keys(), key=len, reverse=True)

    detected_genre = None
    genre_start = -1
    genre_end = -1

    for genre_key in genre_keys_sorted:
        gk_norm = unicodedata.normalize("NFD", genre_key.lower())
        gk_norm = "".join(c for c in gk_norm if unicodedata.category(c) != "Mn")
        # Buscar el género como palabra completa (o frase completa)
        pattern = r'\b' + re.escape(gk_norm) + r'\b'
        gm = re.search(pattern, remaining_norm)
        if gm:
            detected_genre = genre_key
            genre_start = gm.start()
            genre_end = gm.end()
            break

    if detected_genre:
        result['genre'] = detected_genre
        # Eliminar el género del texto restante
        remaining = (remaining[:genre_start] + remaining[genre_end:]).strip()

    # 4. Lo que queda es el texto de búsqueda libre
    remaining = re.sub(r'\s+', ' ', remaining).strip()
    # Limpiar caracteres sueltos
    remaining = re.sub(r'^[\s\-,;.]+|[\s\-,;.]+$', '', remaining).strip()

    if remaining and len(remaining) >= 1:
        result['text'] = remaining

    return result


# ---------------------------------------------------------------------------
# COROUTINE NULA
# ---------------------------------------------------------------------------
async def _noop():
    return None


# ---------------------------------------------------------------------------
# GOOGLE KNOWLEDGE GRAPH (con cache IA persistente)
# ---------------------------------------------------------------------------
async def _google_kg_search(
    http,
    query_title: str,
    year,
):
    if not GOOGLE_KG_API_KEY:
        return None

    ck = _cache_key_from_query(query_title, year)
    cached_data, cached_status = await _ai_cache_get("kg", ck)
    if cached_status in ("ok", "none", "429", "err"):
        if cached_status == "ok" and isinstance(cached_data, dict):
            return cached_data
        if cached_status in ("none", "429"):
            return None
        if cached_status == "err" and cached_data is None:
            return None

    try:
        def _is_str(x) -> bool:
            return isinstance(x, str) and x.strip() != ""

        def _safe_get(d, key, default=None):
            return d.get(key, default) if isinstance(d, dict) else default

        def _first_dict(x):
            if isinstance(x, dict): return x
            if isinstance(x, list):
                for it in x:
                    if isinstance(it, dict): return it
            return None

        def _coerce_text(x) -> str:
            if x is None:          return ""
            if isinstance(x, str): return x
            if isinstance(x, dict):
                for k in ("@value", "name", "articleBody"):
                    v = x.get(k)
                    if _is_str(v): return v
                return str(x)
            if isinstance(x, list):
                for it in x:
                    t = _coerce_text(it)
                    if _is_str(t): return t
                return ""
            return str(x)

        def _strip_text(x) -> str:
            s = _coerce_text(x)
            return s.strip() if isinstance(s, str) else ""

        def _extract_types(x) -> list:
            if isinstance(x, list): return [t for t in x if isinstance(t, str)]
            if isinstance(x, str):  return [x]
            return []

        def _extract_image_url(image_field):
            img = image_field
            if isinstance(img, list): img = _first_dict(img) or {}
            if not isinstance(img, dict): return None
            c1 = _strip_text(img.get("contentUrl"))
            if _is_str(c1): return c1
            c2 = _strip_text(img.get("url"))
            if _is_str(c2): return c2
            return None

        def _extract_article_body(best: dict) -> str:
            detailed = _safe_get(best, "detailedDescription", None)
            if isinstance(detailed, dict):
                return _strip_text(detailed.get("articleBody"))
            if isinstance(detailed, list):
                first = _first_dict(detailed)
                if isinstance(first, dict):
                    return _strip_text(first.get("articleBody"))
            return _strip_text(detailed)

        def _extract_genres(best: dict):
            raw   = _safe_get(best, "genre", None)
            names = []

            def _add(g):
                if g is None: return
                if isinstance(g, str) and g.strip():
                    names.append(g.strip()); return
                if isinstance(g, dict):
                    for k in ("name", "@value"):
                        s = g.get(k)
                        if isinstance(s, str) and s.strip():
                            names.append(s.strip()); return
                if isinstance(g, list):
                    for it in g: _add(it)

            _add(raw)
            return ", ".join(n for n in names if n) or None

        params = {
            "query":     query_title,
            "key":       GOOGLE_KG_API_KEY,
            "limit":     3,
            "indent":    "false",
            "types":     ["Movie", "TVSeries"],
            "languages": ["es", "en"],
        }

        async with getattr(app.state, "ai_sem", asyncio.Semaphore(1)):
            r = await http.get("https://kgsearch.googleapis.com/v1/entities:search", params=params)

        if r.status_code == 429:
            await _ai_cache_set("kg", ck, None, "429")
            return None

        r.raise_for_status()
        data  = r.json()
        items = _safe_get(data, "itemListElement", []) or []
        if not isinstance(items, list) or not items:
            await _ai_cache_set("kg", ck, None, "none")
            return None

        best = None
        for item in items:
            res = item.get("result") if isinstance(item, dict) else None
            if not isinstance(res, dict): continue
            sd = _strip_text(res.get("startDate"))
            if year and _is_str(sd) and sd.startswith(year):
                best = res; break
        if best is None:
            for item in items:
                res = item.get("result") if isinstance(item, dict) else None
                if isinstance(res, dict): best = res; break
        if not isinstance(best, dict) or not best:
            await _ai_cache_set("kg", ck, None, "none")
            return None

        name         = _strip_text(best.get("name")) or query_title
        short_desc   = _strip_text(best.get("description"))
        article_body = _extract_article_body(best)
        sinopsis     = (article_body or short_desc).strip() or None
        if not _is_str(sinopsis): sinopsis = None

        imagen_url        = _extract_image_url(best.get("image"))
        start_date_raw    = best.get("startDate")
        start_date        = _strip_text(start_date_raw)
        year_out          = start_date[:4] if (isinstance(start_date, str) and len(start_date) >= 4) else None
        fecha_lanzamiento = start_date if _is_str(start_date) else None

        types      = _extract_types(best.get("@type"))
        media_type = ("movie" if "Movie" in types else "tv" if "TVSeries" in types else None)
        generos    = _extract_genres(best)

        print(
            f"   🌐 Google KG → '{name}' [{media_type}] "
            f"año={year_out} img={'✓' if imagen_url else '✗'} "
            f"sinopsis={'✓' if sinopsis else '✗'}"
        )
        out = {
            "source":                "google_kg",
            "tmdb_id":               None,
            "media_type":            media_type,
            "titulo":                name,
            "imagen_url":            imagen_url,
            "sinopsis":              sinopsis,
            "fecha_lanzamiento":     fecha_lanzamiento,
            "duracion":              None,
            "idioma_original":       None,
            "popularidad":           None,
            "puntuacion":            None,
            "generos":               generos,
            "año":                   year_out or year,
            "descripcion_detallada": short_desc.strip() if isinstance(short_desc, str) and short_desc.strip() else None,
        }

        await _ai_cache_set("kg", ck, out, "ok")
        return out

    except Exception as e:
        await _ai_cache_set("kg", ck, None, "err")
        print(f"⚠️  Error de Google KG ({query_title}): {e}")
        return None


# ---------------------------------------------------------------------------
# TMDB
# ---------------------------------------------------------------------------
async def _tmdb_search_and_details(
    http,
    query_title: str,
    year,
):
    if not TMDB_API_KEY:
        return None
    try:
        params = {
            "api_key":       TMDB_API_KEY,
            "query":         query_title,
            "include_adult": "false",
            "language":      "es-ES",
            "page":          1,
        }
        r = await http.get(f"{TMDB_API_BASE}/search/multi", params=params)
        r.raise_for_status()
        results    = r.json().get("results") or []
        candidates = [x for x in results if x.get("media_type") in ("movie", "tv")]
        if not candidates:
            return None

        if year:
            by_year = []
            for x in candidates:
                d = (x.get("release_date") or x.get("first_air_date") or "")
                if d.startswith(year):
                    by_year.append(x)
            if by_year:
                candidates = by_year

        candidates.sort(key=lambda x: (x.get("popularity") or 0), reverse=True)
        best       = candidates[0]
        tmdb_id    = best.get("id")
        media_type = best.get("media_type")
        if not tmdb_id or media_type not in ("movie", "tv"):
            return None

        detail_params = {"api_key": TMDB_API_KEY, "language": "es-ES"}
        if media_type == "movie":
            d = await http.get(f"{TMDB_API_BASE}/movie/{tmdb_id}", params=detail_params)
            d.raise_for_status()
            details       = d.json()
            title         = details.get("title") or details.get("original_title") or query_title
            poster_path   = details.get("poster_path")
            backdrop_path = details.get("backdrop_path")
            overview      = details.get("overview")
            release_date  = details.get("release_date")
            runtime       = details.get("runtime")
            orig_lang     = details.get("original_language")
            popularity    = details.get("popularity")
            vote_avg      = details.get("vote_average")
            genres_list   = details.get("genres") or []
            tagline       = details.get("tagline")
        else:
            d = await http.get(f"{TMDB_API_BASE}/tv/{tmdb_id}", params=detail_params)
            d.raise_for_status()
            details       = d.json()
            title         = details.get("name") or details.get("original_name") or query_title
            poster_path   = details.get("poster_path")
            backdrop_path = details.get("backdrop_path")
            overview      = details.get("overview")
            release_date  = details.get("first_air_date")
            run_list      = details.get("episode_run_time") or []
            runtime       = run_list[0] if run_list else None
            orig_lang     = details.get("original_language")
            popularity    = details.get("popularity")
            vote_avg      = details.get("vote_average")
            genres_list   = details.get("genres") or []
            tagline       = details.get("tagline")

        genres    = ", ".join(g.get("name") for g in genres_list if g.get("name")) or None
        poster    = poster_path or backdrop_path
        image_url = f"{TMDB_IMAGE_BASE}{poster}" if poster else None
        year_out  = (release_date[:4] if release_date else None) or year

        print(
            f"   🎬 TMDb → '{title}' [{media_type}] "
            f"año={year_out} img={'✓' if image_url else '✗'}"
        )
        return {
            "source":                "tmdb",
            "tmdb_id":               tmdb_id,
            "media_type":            media_type,
            "titulo":                title,
            "imagen_url":            image_url,
            "sinopsis":              overview,
            "fecha_lanzamiento":     release_date,
            "duracion":              (f"{runtime} minutos" if isinstance(runtime, int) and runtime > 0 else None),
            "idioma_original":       (orig_lang.upper() if orig_lang else None),
            "popularidad":           popularity,
            "puntuacion":            vote_avg,
            "generos":               genres,
            "año":                   year_out,
            "descripcion_detallada": tagline or None,
        }
    except Exception as e:
        print(f"⚠️  TMDb error ({query_title}): {e}")
        return None


# ---------------------------------------------------------------------------
# TVMaze — gratuita, sin API key
# ---------------------------------------------------------------------------
async def _tvmaze_fetch(
    http,
    query_title: str,
    year,
):
    try:
        r = await http.get(
            f"{TVMAZE_API_BASE}/search/shows",
            params={"q": quote_plus(query_title)},
        )
        r.raise_for_status()
        items = r.json() or []
        if not items:
            return None

        best_show = None
        if year:
            for item in items:
                show = item.get("show") or {}
                if (show.get("premiered") or "").startswith(year):
                    best_show = show; break
        if best_show is None and items:
            best_show = items[0].get("show") or {}
        if not best_show:
            return None

        image_obj  = best_show.get("image") or {}
        imagen_url = image_obj.get("original") or image_obj.get("medium") or None

        summary_raw = best_show.get("summary") or ""
        sinopsis    = re.sub(r"<[^>]+>", "", summary_raw).strip() or None

        genres_list = best_show.get("genres") or []
        generos     = ", ".join(genres_list) if genres_list else None

        rating_obj  = best_show.get("rating") or {}
        puntuacion  = rating_obj.get("average") or None

        runtime_val = best_show.get("runtime")
        duracion    = (f"{runtime_val} minutos" if isinstance(runtime_val, int) and runtime_val > 0 else None)

        premiered   = best_show.get("premiered") or ""
        year_out    = premiered[:4] if len(premiered) >= 4 else year
        titulo      = best_show.get("name") or query_title

        print(
            f"   📺 TVMaze → '{titulo}' año={year_out} "
            f"img={'✓' if imagen_url else '✗'} "
            f"sinopsis={'✓' if sinopsis else '✗'}"
        )
        return {
            "source":                "tvmaze",
            "tmdb_id":               None,
            "media_type":            "tv",
            "titulo":                titulo,
            "imagen_url":            imagen_url,
            "sinopsis":              sinopsis,
            "fecha_lanzamiento":     premiered or None,
            "duracion":              duracion,
            "idioma_original":       best_show.get("language") or None,
            "popularidad":           None,
            "puntuacion":            puntuacion,
            "generos":               generos,
            "año":                   year_out,
            "descripcion_detallada": None,
        }
    except Exception as e:
        print(f"⚠️  TVMaze error ({query_title}): {e}")
        return None


# ---------------------------------------------------------------------------
# GEMINI AI: completa metadatos faltantes (SOLO EN /search, LIMITADO A 10) + cache IA
# ---------------------------------------------------------------------------
_GEMINI_CALL_COUNTER = {"count": 0}


async def _gemini_complete_metadata(
    http,
    title: str,
    year,
    existing_meta: dict,
):
    if not GEMINI_API_KEY:
        return None

    ck = _cache_key_from_query(title, year)
    cached_data, cached_status = await _ai_cache_get("gemini", ck)
    if cached_status in ("ok", "none", "429", "err"):
        if cached_status == "ok" and isinstance(cached_data, dict):
            return cached_data
        if cached_status in ("none", "429"):
            return None
        if cached_status == "err" and cached_data is None:
            return None

    if _GEMINI_CALL_COUNTER["count"] >= 10:
        print(f"   ⚠️  Límite de IA (10) alcanzado, no se usa Gemini para '{title}'")
        await _ai_cache_set("gemini", ck, None, "none")
        return None

    try:
        _GEMINI_CALL_COUNTER["count"] += 1
        year_hint = f" ({year})" if year else ""
        prompt = (
            f'Proporciona metadatos de la película o serie titulada "{title}"{year_hint} '
            f'en formato JSON estricto con estas claves exactas:\n'
            f'{{"sinopsis": "descripción breve en español (máx 180 palabras)",\n'
            f' "generos": "géneros separados por coma (ej: Acción, Aventura)",\n'
            f' "año": "año de estreno como string de 4 dígitos (ej: \\"2019\\")",\n'
            f' "idioma_original": "código ISO 639-1 en mayúsculas (ES, EN, JA, KO, FR, etc.)",\n'
            f' "duracion": "duración en formato \\"120 minutos\\" o null si es serie",\n'
            f' "fecha_lanzamiento": "fecha en formato YYYY-MM-DD o null si no se conoce exactamente"}}\n'
            f'Reglas:\n'
            f'- Solo incluye los campos que conoces con certeza.\n'
            f'- Si un campo es desconocido o incierto, usa el valor null.\n'
            f'- Responde ÚNICAMENTE con el JSON válido, sin explicaciones, sin markdown, sin texto adicional.'
        )

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature":     0.1,
                "maxOutputTokens": 512,
            },
        }

        async with getattr(app.state, "ai_sem", asyncio.Semaphore(1)):
            r = await http.post(
                GEMINI_API_URL,
                params={"key": GEMINI_API_KEY},
                json=payload,
                timeout=5.0,
            )

        if r.status_code == 429:
            await _ai_cache_set("gemini", ck, None, "429")
            print(f"⚠️  Gemini rate limit (429) para '{title}' — usando cache cooldown")
            return None

        r.raise_for_status()

        data       = r.json()
        candidates = data.get("candidates") or []
        if not candidates:
            await _ai_cache_set("gemini", ck, None, "none")
            return None

        text = (
            (candidates[0].get("content") or {})
            .get("parts", [{}])[0]
            .get("text", "")
        ) or ""
        text = text.strip()

        text = re.sub(r"```(?:json)?", "", text)
        text = re.sub(r"```\s*$",      "", text).strip()

        result = json.loads(text)
        if not isinstance(result, dict):
            await _ai_cache_set("gemini", ck, None, "none")
            return None

        print(f"   🤖 Gemini ({_GEMINI_CALL_COUNTER['count']}/10) → completó metadatos para '{title}'")
        await _ai_cache_set("gemini", ck, result, "ok")
        return result

    except json.JSONDecodeError:
        print(f"⚠️  Gemini devolvió JSON inválido para '{title}'")
        await _ai_cache_set("gemini", ck, None, "err")
        return None
    except Exception as e:
        print(f"⚠️  Gemini error ({title}): {e}")
        await _ai_cache_set("gemini", ck, None, "err")
        return None


# ---------------------------------------------------------------------------
# MERGE con prioridades diferenciadas
# ---------------------------------------------------------------------------
def _merge_metadata_with_kg(
    kg,
    tmdb,
    tvmaze,
    fallback_title: str,
    fallback_year,
) -> dict:
    text_sources  = [s for s in [kg, tmdb, tvmaze] if isinstance(s, dict)]
    image_sources = [s for s in [tmdb, kg, tvmaze]  if isinstance(s, dict)]

    def pick(key: str):
        for src in text_sources:
            v = src.get(key)
            if v is not None and v != "": return v
        return None

    def pick_image():
        for src in image_sources:
            v = src.get("imagen_url")
            if v is not None and v != "": return v
        return None

    tmdb_id    = (tmdb.get("tmdb_id")      if isinstance(tmdb,   dict) else None) or \
                 (kg.get("tmdb_id")        if isinstance(kg,     dict) else None)
    media_type = (tmdb.get("media_type")   if isinstance(tmdb,   dict) else None) or \
                 (kg.get("media_type")     if isinstance(kg,     dict) else None) or \
                 (tvmaze.get("media_type") if isinstance(tvmaze, dict) else None)

    return {
        "tmdb_id":               tmdb_id,
        "media_type":            media_type,
        "titulo":                pick("titulo")               or fallback_title or "Película",
        "imagen_url":            pick_image(),
        "sinopsis":              pick("sinopsis"),
        "fecha_lanzamiento":     pick("fecha_lanzamiento"),
        "duracion":              pick("duracion"),
        "idioma_original":       pick("idioma_original"),
        "popularidad":           pick("popularidad"),
        "puntuacion":            pick("puntuacion"),
        "generos":               pick("generos"),
        "año":                   pick("año")                  or fallback_year,
        "descripcion_detallada": pick("descripcion_detallada"),
    }


# ---------------------------------------------------------------------------
# CACHÉ: get / set con dirty flag
# ---------------------------------------------------------------------------
async def _meta_cache_get(cache_key: str):
    meta_cache = getattr(app.state, "meta_cache", None)
    if not isinstance(meta_cache, dict): return None
    return meta_cache.get(cache_key)


async def _meta_cache_set(cache_key: str, metadata: dict) -> None:
    if not cache_key or not isinstance(metadata, dict):
        return
    metadata.pop("stream_url",   None)
    metadata.pop("pelicula_url", None)

    async with app.state.meta_cache_lock:
        app.state.meta_cache[cache_key] = metadata
        app.state.meta_cache_dirty      = True

        total = len(app.state.meta_cache)

        if total % CACHE_SAVE_EVERY == 0:
            to_save = dict(app.state.meta_cache)
            to_save[AI_CACHE_KEY] = dict(getattr(app.state, "ai_cache", {}) or {})
            await _save_persistent_cache(to_save)
            app.state.meta_cache_dirty = False
            app.state.last_persist_save_ts = time.time()

        if _meta_is_full_enough_for_persist(metadata):
            now = time.time()
            last_ts = float(getattr(app.state, "last_persist_save_ts", 0.0) or 0.0)
            if (now - last_ts) > 30.0:
                to_save = dict(app.state.meta_cache)
                to_save[AI_CACHE_KEY] = dict(getattr(app.state, "ai_cache", {}) or {})
                await _save_persistent_cache(to_save)
                app.state.meta_cache_dirty = False
                app.state.last_persist_save_ts = now


# ---------------------------------------------------------------------------
# 🔧 ENRIQUECIMIENTO PRINCIPAL (prioriza TMDB antes que KG/Gemini)
# Usa limpieza avanzada de títulos para mejor detección de poster
# ---------------------------------------------------------------------------
async def enrich_results_with_tmdb(
    results: list,
    max_new=None,
    use_gemini: bool = False,
    catalog_mode: bool = False,
) -> list:
    request_cache: dict = {}
    semaphore     = asyncio.Semaphore(MAX_CONCURRENCY)
    new_counter   = {"n": 0}
    limit_new     = max_new if max_new is not None else len(results)
    _timeout      = httpx.Timeout(connect=2.0, read=3.5, write=2.0, pool=1.0)

    async with httpx.AsyncClient(timeout=_timeout) as http:

        async def enrich_one(r: dict) -> dict:
            title_raw = r.get("title") or "Película"

            # 🔧 MEJORA: Usar limpieza avanzada
            clean_title_adv, detected_year_adv = _advanced_clean_title(title_raw)
            fallback_title = clean_title_adv or _strip_decorations(title_raw) or "Película"

            # Año: preferir el detectado por limpieza avanzada
            fallback_year_title = detected_year_adv or _extract_year_advanced(title_raw)

            # Query para APIs: título limpio sin año ni ruido
            query_title, year = _build_tmdb_query_from_title(title_raw)
            # Preferir año detectado por método avanzado
            if detected_year_adv:
                year = detected_year_adv

            ck = _cache_key_from_query(query_title, year)

            meta = request_cache.get(ck)

            if not meta:
                meta = await _meta_cache_get(ck)

                need_repair = isinstance(meta, dict) and (
                    _is_placeholder_image(meta.get("imagen_url")) or
                    not meta.get("sinopsis")   or
                    not meta.get("año")        or
                    not meta.get("titulo")
                )

                if (not meta) or need_repair:
                    if new_counter["n"] >= limit_new:
                        pelicula_url = r.get("stream_url") or ""
                        thumb = _thumb_url_for_message(r.get("id"), pelicula_url)
                        yt    = _youtube_thumb_from_stream_url(pelicula_url)
                        img_final = thumb or yt or ""
                        return {
                            "titulo":                fallback_title or "Película",
                            "imagen_url":            img_final,
                            "pelicula_url":          pelicula_url,
                            "descripcion":           "n/a",
                            "fecha_lanzamiento":     "n/a",
                            "duracion":              "n/a",
                            "idioma_original":       "n/a",
                            "popularidad":           0,
                            "puntuacion":            0,
                            "generos":               "n/a",
                            "año":                   fallback_year_title or year or "n/a",
                            "id":                    r.get("id"),
                            "size":                  _nn_str(r.get("size"), "n/a"),
                            "descripcion_detallada": "n/a",
                        }

                    new_counter["n"] += 1

                    kg = tmdb = tvmaze = None

                    async with semaphore:
                        tmdb = await (_tmdb_search_and_details(http, query_title, year) if TMDB_API_KEY else _noop())

                        need_image = not (isinstance(tmdb, dict) and tmdb.get("imagen_url"))
                        need_text  = not (isinstance(tmdb, dict) and tmdb.get("sinopsis") and tmdb.get("año"))

                        if (GOOGLE_KG_API_KEY and (need_image or need_text)):
                            kg = await _google_kg_search(http, query_title, year)

                        combined_has_image    = bool(
                            (isinstance(tmdb, dict) and tmdb.get("imagen_url")) or
                            (isinstance(kg,   dict) and kg.get("imagen_url"))
                        )
                        combined_has_synopsis = bool(
                            (isinstance(tmdb, dict) and tmdb.get("sinopsis")) or
                            (isinstance(kg,   dict) and kg.get("sinopsis"))
                        )
                        combined_has_year     = bool(
                            (isinstance(tmdb, dict) and tmdb.get("año")) or
                            (isinstance(kg,   dict) and kg.get("año"))
                        )

                        if not (combined_has_image and combined_has_synopsis and combined_has_year):
                            tvmaze = await _tvmaze_fetch(http, query_title, year)

                    meta = _merge_metadata_with_kg(
                        kg, tmdb, tvmaze,
                        fallback_title=fallback_title,
                        fallback_year=fallback_year_title or year,
                    )

                    if use_gemini and GEMINI_API_KEY and not (meta.get("sinopsis") and meta.get("generos")):
                        gemini_data = await _gemini_complete_metadata(
                            http, fallback_title, year, meta
                        )
                        if isinstance(gemini_data, dict):
                            for _gk in ["sinopsis", "generos", "año", "idioma_original",
                                        "duracion", "fecha_lanzamiento"]:
                                if not meta.get(_gk) and gemini_data.get(_gk):
                                    meta[_gk] = gemini_data[_gk]

                    print(
                        f"   ✅ Enriquecimiento → '{meta.get('titulo', '?')}' "
                        f"img={'✓' if meta.get('imagen_url') else '✗'} "
                        f"sinopsis={'✓' if meta.get('sinopsis') else '✗'} "
                        f"año={meta.get('año', '?')}"
                    )
                    await _meta_cache_set(ck, meta)

                request_cache[ck] = meta

            pelicula_url = r.get("stream_url") or ""

            meta_img = meta.get("imagen_url") if isinstance(meta, dict) else None
            if _is_placeholder_image(meta_img):
                meta_img = None

            thumb_img  = _thumb_url_for_message(r.get("id"), pelicula_url)
            yt_img     = _youtube_thumb_from_stream_url(pelicula_url)

            if catalog_mode:
                imagen_url = meta_img or thumb_img or yt_img or ""
            else:
                imagen_url = meta_img or thumb_img or yt_img or ""

            descripcion = (meta.get("sinopsis") if isinstance(meta, dict) else None) or "n/a"
            year_out    = (meta.get("año") if isinstance(meta, dict) else None) or fallback_year_title or year or "n/a"

            # Usar título de API si está disponible, sino el limpio
            api_titulo = meta.get("titulo") if isinstance(meta, dict) else None
            titulo_final = api_titulo or fallback_title or "Película"

            return {
                "titulo":                _nn_str(titulo_final, "Película"),
                "imagen_url":            _nn_str(imagen_url, "n/a"),
                "pelicula_url":          _nn_str(pelicula_url, "n/a"),
                "descripcion":           _nn_str(descripcion,  "n/a"),
                "fecha_lanzamiento":     _nn_str(meta.get("fecha_lanzamiento") if isinstance(meta, dict) else None, "n/a"),
                "duracion":              _nn_str(meta.get("duracion")           if isinstance(meta, dict) else None, "n/a"),
                "idioma_original":       _nn_str(meta.get("idioma_original")    if isinstance(meta, dict) else None, "n/a"),
                "popularidad":           _nn_num(meta.get("popularidad")        if isinstance(meta, dict) else None, 0),
                "puntuacion":            _nn_num(meta.get("puntuacion")         if isinstance(meta, dict) else None, 0),
                "generos":               _nn_str(meta.get("generos")            if isinstance(meta, dict) else None, "n/a"),
                "año":                   _nn_str(year_out, "n/a"),
                "id":                    r.get("id"),
                "size":                  _nn_str(r.get("size"), "n/a"),
                "descripcion_detallada": _nn_str(meta.get("descripcion_detallada") if isinstance(meta, dict) else None, "n/a"),
            }

        tasks    = [enrich_one(r) for r in results]
        enriched = await asyncio.gather(*tasks, return_exceptions=True)

        final = []
        for item in enriched:
            if isinstance(item, dict):
                final.append(item)
            else:
                final.append({
                    "titulo":                "Película",
                    "imagen_url":            "n/a",
                    "pelicula_url":          "n/a",
                    "descripcion":           "n/a",
                    "fecha_lanzamiento":     "n/a",
                    "duracion":              "n/a",
                    "idioma_original":       "n/a",
                    "popularidad":           0,
                    "puntuacion":            0,
                    "generos":               "n/a",
                    "año":                   "n/a",
                    "id":                    None,
                    "size":                  "n/a",
                    "descripcion_detallada": "n/a",
                })
        return final


# ---------------------------------------------------------------------------
# FORMATO BÁSICO (sin APIs)
# ---------------------------------------------------------------------------
def _format_results_without_apis(final_results: list, catalog_mode: bool = False) -> list:
    formatted = []
    for r in final_results:
        title_raw = r.get("title") or "Película"
        # 🔧 MEJORA: Usar limpieza avanzada
        titulo, detected_year = _advanced_clean_title(title_raw)
        year = detected_year or _extract_year_advanced(title_raw) or "n/a"

        pelicula_url = r.get("stream_url") or ""

        thumb_img = _thumb_url_for_message(r.get("id"), pelicula_url)
        yt_img    = _youtube_thumb_from_stream_url(pelicula_url)
        img_final = thumb_img or yt_img or "n/a"

        formatted.append({
            "titulo":                titulo or "Película",
            "imagen_url":            img_final,
            "pelicula_url":          _nn_str(pelicula_url, "n/a"),
            "descripcion":           "n/a",
            "fecha_lanzamiento":     "n/a",
            "duracion":              "n/a",
            "idioma_original":       "n/a",
            "popularidad":           0,
            "puntuacion":            0,
            "generos":               "n/a",
            "año":                   year,
            "id":                    r.get("id"),
            "size":                  _nn_str(r.get("size"), "n/a"),
            "descripcion_detallada": "n/a",
        })
    return formatted


# ---------------------------------------------------------------------------
# 🔧 MEJORA 5: ENDPOINT /search CON BÚSQUEDA COMBINADA
# Soporta: nombre, categoría, año, rango, combinaciones
# ---------------------------------------------------------------------------
@app.get("/search")
async def search(
    q:        str | None = Query(None,  description="Texto de búsqueda (nombre, categoría, año o combinación)"),
    year:     str | None = Query(None,  description="Año exacto de estreno (ej: 2019)"),
    genre:    str | None = Query(None,  description="Género o categoría (ej: Acción, Anime, Terror)"),
    language: str | None = Query(None,  description="Idioma original en código ISO (ej: ES, EN, JA)"),
    desde:    int | None = Query(None,  description="Año mínimo (ej: 2010)"),
    hasta:    int | None = Query(None,  description="Año máximo (ej: 2023)"),
    canal:    str | None = Query(None,  description="Canal específico de Telegram (ej: @animadasssss)"),
):
    has_any = any([q, year, genre, language, desde, hasta, canal])
    if not has_any:
        raise HTTPException(
            status_code=400,
            detail=(
                "Se requiere al menos un parámetro: "
                "q, year, genre, language, desde, hasta, canal"
            ),
        )
    if q is not None and len(q.strip()) < 1:
        raise HTTPException(
            status_code=400,
            detail="El parámetro 'q' no puede estar vacío",
        )

    _GEMINI_CALL_COUNTER["count"] = 0

    # 🔧 MEJORA 5: Parsear query combinada si 'q' fue proporcionado
    # Esto permite búsquedas como "acción 2019 rambo" en un solo parámetro
    parsed_q = {}
    if q and q.strip():
        parsed_q = _parse_combined_query(q.strip())

    # Los parámetros explícitos tienen prioridad sobre los parseados de 'q'
    effective_text  = parsed_q.get('text') or (q if not parsed_q.get('genre') and not parsed_q.get('year') and not parsed_q.get('desde') else None)
    effective_genre = genre or parsed_q.get('genre')
    effective_year  = year  or parsed_q.get('year')
    effective_desde = desde or parsed_q.get('desde')
    effective_hasta = hasta or parsed_q.get('hasta')

    # Si q fue completamente "consumido" por parseo (año o género), usar solo el texto restante
    if q and parsed_q:
        effective_text = parsed_q.get('text')
        # Si no quedó texto libre pero hay genre/year, limpiar 'q' para búsqueda
        if not effective_text and (parsed_q.get('genre') or parsed_q.get('year') or parsed_q.get('desde')):
            effective_text = None
        elif effective_text:
            # Verificar que tiene longitud mínima
            if len(effective_text.strip()) < 2:
                effective_text = None

    # Si q original no fue parseado (sin género ni año detectados), usarlo completo como texto
    if q and not any([parsed_q.get('genre'), parsed_q.get('year'), parsed_q.get('desde')]):
        effective_text = q.strip()

    print(f"🔍 Búsqueda: text='{effective_text}' genre='{effective_genre}' year='{effective_year}' desde={effective_desde} hasta={effective_hasta}")

    try:
        if not getattr(app.state, "channels_ready", False):
            waited = 0.0
            while not getattr(app.state, "channels_ready", False) and waited < CHANNELS_READY_MAX_WAIT_SEARCH:
                await asyncio.sleep(0.3)
                waited += 0.3

        entities = getattr(app.state, "entities", [app.state.entity])
        all_entities_indexed = [(i, e) for i, e in enumerate(entities) if e is not None]

        # 🔧 MEJORA 4: Selección de canales optimizada por categoría
        if canal:
            canal_clean = canal.strip().lstrip('@').lower()
            entities_indexed = [
                (i, e) for i, e in all_entities_indexed
                if (getattr(e, 'username', '') or '').lower() == canal_clean
            ]
            if not entities_indexed:
                entities_indexed = all_entities_indexed[:1]
        elif effective_genre:
            genre_channels = _get_genre_channels(effective_genre)
            if genre_channels:
                genre_usernames = [gc.lstrip('@').lower() for gc in genre_channels]
                entities_indexed = [
                    (i, e) for i, e in all_entities_indexed
                    if (getattr(e, 'username', '') or '').lower() in genre_usernames
                ]
                if not entities_indexed:
                    entities_indexed = all_entities_indexed
            else:
                entities_indexed = all_entities_indexed
        else:
            entities_indexed = all_entities_indexed

        async def search_in_channel(ch_index: int, entity) -> list:
            if entity is None:
                return []
            results = []
            try:
                if effective_text:
                    msg_iter = client.iter_messages(entity, search=effective_text.strip())

                    async for message in msg_iter:
                        if message.media and (message.video or message.document):
                            caption     = message.text or ""
                            title       = _extract_title_from_caption(caption)
                            direct_link = (
                                f"{PUBLIC_URL}/stream/{message.id}?ch={ch_index}"
                                if PUBLIC_URL else f"/stream/{message.id}?ch={ch_index}"
                            )
                            results.append({
                                "id":         message.id,
                                "title":      title,
                                "size":       (
                                    f"{round(message.file.size / (1024 * 1024), 2)} MB"
                                    if message.file else "n/a"
                                ),
                                "stream_url": direct_link,
                            })
                else:
                    results = await _get_recent_media_cached(ch_index, entity)

                print(f"   📺 Canal [{ch_index}] ({entity.title}): {len(results)} resultado(s)")
            except Exception as e:
                print(f"⚠️  Error en canal [{ch_index}] ({getattr(entity, 'title', ch_index)}): {e}")
            return results

        tasks = [search_in_channel(ch_index, entity) for ch_index, entity in entities_indexed]
        raw   = await asyncio.gather(*tasks, return_exceptions=True)

        all_results: list = []
        for item in raw:
            if isinstance(item, list):
                all_results.extend(item)

        seen, unique = set(), []
        for result in all_results:
            key = normalize_title(result["title"])
            if key not in seen:
                seen.add(key); unique.append(result)

        # 🔧 MEJORA 3: Ordenar por capítulos/sagas
        unique = _sort_results_by_saga_and_chapter(unique)
        final_results = unique

        print(f"🎯 Resultados: {len(final_results)} únicos (de {len(all_results)} totales)")

        if effective_genre and len(final_results) < MIN_CATEGORY_RESULTS:
            print(
                f"⚠️  Categoría '{effective_genre}': solo {len(final_results)} resultado(s). "
                f"Complementando hasta {MIN_CATEGORY_RESULTS}..."
            )
            searched_indices = {i for i, _ in entities_indexed}
            remaining_entities = [
                (i, e) for i, e in all_entities_indexed
                if i not in searched_indices and e is not None
            ]
            seen_keys = {normalize_title(r["title"]) for r in final_results}

            async def _fetch_supplement(ch_index: int, entity) -> list:
                out = []
                try:
                    cached = await _get_recent_media_cached(ch_index, entity)
                    for msg in cached:
                        key = normalize_title(msg.get("title", ""))
                        if key not in seen_keys:
                            out.append(msg)
                except Exception as ex:
                    print(f"⚠️  Error complementando canal [{ch_index}]: {ex}")
                return out

            supplement_tasks = [_fetch_supplement(i, e) for i, e in remaining_entities]
            supplement_raw   = await asyncio.gather(*supplement_tasks, return_exceptions=True)

            for item in supplement_raw:
                if isinstance(item, list):
                    for r in item:
                        key = normalize_title(r.get("title", ""))
                        if key not in seen_keys:
                            seen_keys.add(key)
                            final_results.append(r)
                        if len(final_results) >= MIN_CATEGORY_RESULTS:
                            break
                if len(final_results) >= MIN_CATEGORY_RESULTS:
                    break

            print(f"✅ Complemento aplicado: ahora {len(final_results)} resultado(s)")

        if final_results:
            asyncio.create_task(_prelaunch_thumb_extractions(final_results))

        if not final_results and effective_text:
            print("🟦 Sin resultados en Telegram. Usando respaldo YouTube...")
            yt_results = await youtube_fallback(effective_text.strip())
            if yt_results:
                try:
                    enriched = await asyncio.wait_for(
                        enrich_results_with_tmdb(yt_results, max_new=MAX_ENRICH_NEW, use_gemini=True),
                        timeout=4.0,
                    )
                except asyncio.TimeoutError:
                    print("⚠️  /search YouTube enrichment timeout")
                    enriched = _format_results_without_apis(yt_results)
                if any([effective_year, effective_genre, language, effective_desde, effective_hasta]):
                    enriched = _apply_advanced_filters(enriched, effective_year, effective_genre, language, effective_desde, effective_hasta)
                return _to_peliculas_json_schema(enriched)

        try:
            enriched = await asyncio.wait_for(
                enrich_results_with_tmdb(final_results, max_new=MAX_ENRICH_NEW, use_gemini=True),
                timeout=4.0,
            )
        except asyncio.TimeoutError:
            print("⚠️  /search enrichment timeout — devolviendo formato básico")
            enriched = _format_results_without_apis(final_results)

        if any([effective_year, effective_genre, language, effective_desde, effective_hasta]):
            enriched = _apply_advanced_filters(enriched, effective_year, effective_genre, language, effective_desde, effective_hasta)
            print(f"🔎 Filtros avanzados aplicados → {len(enriched)} resultado(s)")

        return _to_peliculas_json_schema(enriched)

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en /search: {e}")
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# ENDPOINT /catalog
# ---------------------------------------------------------------------------
@app.get("/catalog")
async def catalog():
    try:
        now         = time.monotonic()
        pool_cache  = getattr(app.state, "catalog_pool_cache", None) or {"ts": 0.0, "pool": []}
        cached_ts   = pool_cache.get("ts")   or 0.0
        cached_pool = pool_cache.get("pool") or []

        if isinstance(cached_pool, list) and len(cached_pool) > 0 and (now - cached_ts) < CATALOG_POOL_TTL:
            pool = cached_pool
        else:
            fetch_sem = asyncio.Semaphore(CATALOG_FETCH_CONCURRENCY)

            async def fetch_from_channel(ch_index: int, entity) -> list:
                if entity is None: return []
                results = []
                try:
                    async with fetch_sem:
                        async for message in client.iter_messages(entity, limit=200):
                            if message.media and (message.video or message.document):
                                caption     = message.text or ""
                                title       = _extract_title_from_caption(caption)
                                direct_link = (
                                    f"{PUBLIC_URL}/stream/{message.id}?ch={ch_index}"
                                    if PUBLIC_URL else f"/stream/{message.id}?ch={ch_index}"
                                )
                                results.append({
                                    "id":         message.id,
                                    "title":      title,
                                    "size":       (
                                        f"{round(message.file.size / (1024 * 1024), 2)} MB"
                                        if message.file else "n/a"
                                    ),
                                    "stream_url": direct_link,
                                })
                                if len(results) >= 30: break
                except Exception as e:
                    print(f"⚠️  Error en canal [{ch_index}] para /catalog: {e}")
                return results

            deadline = time.monotonic() + 15.0
            while not getattr(app.state, "channels_ready", False) and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            entities  = getattr(app.state, "entities", [app.state.entity])
            all_tasks = [
                fetch_from_channel(i, e)
                for i, e in enumerate(entities)
                if e is not None
            ]
            raw = await asyncio.gather(*all_tasks, return_exceptions=True)

            pool      = []
            seen_keys = set()
            for item in raw:
                if isinstance(item, list):
                    for r in item:
                        key = normalize_title(r.get("title", ""))
                        if key not in seen_keys:
                            seen_keys.add(key)
                            pool.append(r)

            random.shuffle(pool)
            app.state.catalog_pool_cache = {"ts": time.monotonic(), "pool": pool}

        sample_size = min(50, len(pool))
        sample      = random.sample(pool, sample_size) if sample_size > 0 else []

        try:
            enriched = await asyncio.wait_for(
                enrich_results_with_tmdb(
                    sample,
                    max_new=MAX_ENRICH_NEW,
                    catalog_mode=True,
                ),
                timeout=8.0,
            )
        except asyncio.TimeoutError:
            print("⚠️  /catalog enrichment timeout — devolviendo formato básico")
            enriched = _format_results_without_apis(sample, catalog_mode=True)

        # ✅ FILTRADO: Solo guardamos películas que tengan imagen real
        enriched_with_poster = [
            item for item in enriched
            if item.get("imagen_url", "") and item.get("imagen_url", "") not in ("n/a", "")
        ]

        print(
            f"📚 /catalog: {len(sample)} muestreados → "
            f"{len(enriched_with_poster)} con imagen "
            f"(descartados: {len(enriched) - len(enriched_with_poster)})"
        )

        return _to_peliculas_json_schema(enriched_with_poster)

    except Exception as e:
        print(f"❌ Error en /catalog: {e}")
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# ENDPOINT /ytthumb/{video_id}
# ---------------------------------------------------------------------------
@app.get("/ytthumb/{video_id}")
async def youtube_thumbnail_proxy(video_id: str):
    cache_key = f"yt:{video_id}"

    thumb_cache = getattr(app.state, "thumb_cache", {})
    async with app.state.thumb_cache_lock:
        cached = thumb_cache.get(cache_key)
        if cached:
            ts, data, mime = cached
            if time.monotonic() - ts < THUMB_CACHE_TTL:
                return Response(content=data, media_type=mime)

    thumb_data = None
    for quality in ["maxresdefault", "sddefault", "hqdefault", "mqdefault", "default"]:
        url = f"https://img.youtube.com/vi/{video_id}/{quality}.jpg"
        try:
            async with httpx.AsyncClient(timeout=5.0) as http:
                r = await http.get(url)
                if r.status_code == 200 and len(r.content) > 5000:
                    thumb_data = r.content
                    break
        except Exception:
            continue

    if not thumb_data:
        return Response(
            status_code=302,
            headers={"Location": PLACEHOLDER_IMAGE_BASE},
        )

    processed = _crop_cover_to_poster(thumb_data)
    mime      = "image/jpeg"

    async with app.state.thumb_cache_lock:
        _thumb_cache_prune(thumb_cache)
        thumb_cache[cache_key] = (time.monotonic(), processed, mime)

    return Response(content=processed, media_type=mime)


# ---------------------------------------------------------------------------
# ENDPOINT /thumb/{message_id}
# ---------------------------------------------------------------------------
@app.get("/thumb/{message_id}")
async def get_thumbnail(message_id: int, request: Request, ch: int = 0):
    try:
        thumb_cache = getattr(app.state, "thumb_cache", {})
        cache_key   = f"{message_id}:{ch}"

        async with app.state.thumb_cache_lock:
            cached = thumb_cache.get(cache_key)
            if cached:
                ts, data, mime = cached
                if time.monotonic() - ts < THUMB_CACHE_TTL:
                    return Response(content=data, media_type=mime)

        entities = getattr(app.state, "entities", [app.state.entity])
        entity   = (
            entities[ch]
            if (0 <= ch < len(entities) and entities[ch] is not None)
            else app.state.entity
        )

        message = await asyncio.wait_for(
            client.get_messages(entity, ids=message_id),
            timeout=3.0,
        )
        if not message:
            raise HTTPException(status_code=404, detail="Miniatura no disponible (mensaje no encontrado)")

        thumb_data: bytes | None = None

        if hasattr(message, "photo") and message.photo:
            try:
                thumb_data = await asyncio.wait_for(
                    client.download_media(message.photo, bytes),
                    timeout=2.5,
                )
            except Exception:
                thumb_data = None

        if not thumb_data and message.document and message.document.thumbs:
            try:
                thumb_data = await asyncio.wait_for(
                    client.download_media(message.document.thumbs[-1], bytes),
                    timeout=2.5,
                )
            except Exception:
                thumb_data = None

        if thumb_data:
            processed = _crop_cover_to_poster(thumb_data)
            mime      = "image/jpeg"
            async with app.state.thumb_cache_lock:
                _thumb_cache_prune(thumb_cache)
                thumb_cache[cache_key] = (time.monotonic(), processed, mime)
            return Response(content=processed, media_type=mime)

        is_video = (
            message.document is not None
            and message.file  is not None
            and message.file.mime_type is not None
            and "video" in message.file.mime_type.lower()
        )

        if not is_video:
            raise HTTPException(
                status_code=404,
                detail="Miniatura no disponible (no es un video)",
            )

        in_progress      = getattr(app.state, "thumb_in_progress",      {})
        in_progress_lock = getattr(app.state, "thumb_in_progress_lock",  asyncio.Lock())

        async with in_progress_lock:
            if cache_key not in in_progress:
                done_event                 = asyncio.Event()
                in_progress[cache_key]     = done_event
                print(f"   🎞️  Lanzando extracción en segundo plano para msg {message_id}...")
                asyncio.create_task(
                    _background_frame_extract(
                        message_id, ch, cache_key, entity, done_event
                    )
                )
                done_event_ref = done_event
            else:
                done_event_ref = in_progress[cache_key]

        try:
            await asyncio.wait_for(
                asyncio.shield(done_event_ref.wait()),
                timeout=3.5,
            )
        except asyncio.TimeoutError:
            pass

        async with app.state.thumb_cache_lock:
            cached = thumb_cache.get(cache_key)
            if cached:
                ts, data, mime = cached
                if time.monotonic() - ts < THUMB_CACHE_TTL:
                    return Response(content=data, media_type=mime)

        raise HTTPException(
            status_code=404,
            detail="Miniatura procesándose en segundo plano (reintentar en unos segundos)",
        )

    except HTTPException:
        raise
    except asyncio.TimeoutError:
        raise HTTPException(status_code=404, detail="Miniatura no disponible (timeout)")
    except Exception as e:
        print(f"⚠️  Error en /thumb/{message_id}: {e}")
        raise HTTPException(status_code=404, detail="Miniatura no disponible")


# ---------------------------------------------------------------------------
# PARSE RANGE HEADER
# ---------------------------------------------------------------------------
def _parse_range_header(range_header, file_size: int):
    if not range_header:
        return None
    try:
        rh = range_header.strip().lower()
        if not rh.startswith("bytes="):
            return None
        spec = rh.replace("bytes=", "", 1).strip()
        if "," in spec:
            spec = spec.split(",", 1)[0].strip()

        if "-" not in spec:
            return None
        start_s, end_s = spec.split("-", 1)
        start_s = start_s.strip()
        end_s   = end_s.strip()

        if start_s == "" and end_s == "":
            return None

        if start_s == "" and end_s.isdigit():
            length = int(end_s)
            if length > file_size:
                length = file_size
            start = file_size - length
            end   = file_size - 1
            return (start, end)

        if not start_s.isdigit():
            return None
        start = int(start_s)

        if end_s == "":
            end = file_size - 1
        else:
            if not end_s.isdigit():
                return None
            end = int(end_s)

        if end >= file_size:
            end = file_size - 1

        if start > end or start >= file_size:
            return None

        return (start, end)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# ENDPOINT /stream/{message_id}
# ---------------------------------------------------------------------------
@app.get("/stream/{message_id}")
async def stream_video(message_id: int, request: Request, ch: int = 0):
    try:
        entities = getattr(app.state, "entities", [app.state.entity])
        entity   = (
            entities[ch]
            if (0 <= ch < len(entities) and entities[ch] is not None)
            else app.state.entity
        )

        message = await client.get_messages(entity, ids=message_id)
        if not message or not message.file:
            raise HTTPException(status_code=404, detail="Video no encontrado")

        file_size = int(message.file.size or 0)
        if file_size <= 0:
            raise HTTPException(status_code=404, detail="Video no encontrado")

        range_header  = request.headers.get("range")
        byte_range    = _parse_range_header(range_header, file_size)
        content_type  = message.file.mime_type or "video/mp4"

        if byte_range is None:
            start          = 0
            content_length = file_size

            async def chunk_generator_full(offset: int, limit: int):
                try:
                    async for chunk in client.iter_download(
                        message.media,
                        offset=offset,
                        limit=limit,
                        chunk_size=STREAM_CHUNK_SIZE,
                    ):
                        yield chunk
                except asyncio.CancelledError:
                    return
                except Exception:
                    return

            headers = {
                "Content-Type":      content_type,
                "Accept-Ranges":     "bytes",
                "Content-Length":    str(content_length),
                "Cache-Control":     "no-store",
                "X-Accel-Buffering": "no",
            }

            return StreamingResponse(
                chunk_generator_full(start, content_length),
                status_code=200,
                headers=headers,
                media_type=content_type,
            )

        start, end     = byte_range
        content_length = (end - start) + 1

        if content_length <= 0:
            return Response(
                status_code=416,
                content=b"",
                headers={"Content-Range": f"bytes */{file_size}"},
            )

        async def chunk_generator_range(offset: int, limit: int):
            try:
                async for chunk in client.iter_download(
                    message.media,
                    offset=offset,
                    limit=limit,
                    chunk_size=STREAM_CHUNK_SIZE,
                ):
                    yield chunk
            except asyncio.CancelledError:
                return
            except Exception:
                return

        headers = {
            "Content-Type":      content_type,
            "Accept-Ranges":     "bytes",
            "Content-Range":     f"bytes {start}-{end}/{file_size}",
            "Content-Length":    str(content_length),
            "Cache-Control":     "no-store",
            "X-Accel-Buffering": "no",
        }

        return StreamingResponse(
            chunk_generator_range(start, content_length),
            status_code=206,
            headers=headers,
            media_type=content_type,
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"⚠️  Error de streaming: {e}")
        raise HTTPException(status_code=500, detail="Error de streaming")


# ---------------------------------------------------------------------------
# YOUTUBE FALLBACK
# ---------------------------------------------------------------------------
async def youtube_fallback(youtube_query: str) -> list:
    if not YOUTUBE_API_KEY:
        return []
    url    = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part":       "snippet",
        "q":          youtube_query,
        "key":        YOUTUBE_API_KEY,
        "type":       "video",
        "maxResults": 1,
    }
    try:
        async with httpx.AsyncClient(timeout=3.5) as http:
            r = await http.get(url, params=params)
            r.raise_for_status()
            data = r.json()
        items = data.get("items") or []
        if not items:
            return []
        first    = items[0]
        video_id = ((first.get("id") or {}).get("videoId")) or ""
        snippet  = first.get("snippet") or {}
        title    = (snippet.get("title") or "YouTube Video").strip()
        if not video_id:
            return []
        return [{
            "id":         video_id,
            "title":      title,
            "size":       "n/a",
            "stream_url": f"https://www.youtube.com/watch?v={video_id}",
        }]
    except Exception as e:
        print(f"⚠️  Error usando YouTube fallback: {e}")
        return []


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=INTERNAL_PORT)
