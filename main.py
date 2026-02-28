import os
import asyncio
import uvicorn
import unicodedata
import re
import random
import json
import httpx
import time
import io                    # 🔧 NUEVO
import subprocess            # 🔧 NUEVO
import tempfile              # 🔧 NUEVO
from urllib.parse import quote_plus, urlparse, parse_qs
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.sessions import StringSession

# 🔧 NUEVO: Pillow con fallback gracioso
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
# OPTIMIZACIÓN / LÍMITES
# ---------------------------------------------------------------------------
# 🔧 Reducimos un poco la concurrencia para evitar saturar el ancho de banda en descargas de frames
MAX_CONCURRENCY           = max(3,  min(10, int(os.getenv("MAX_CONCURRENCY",           "8"))))
CATALOG_POOL_TTL          = max(60,         int(os.getenv("CATALOG_POOL_TTL",          "600")))
CATALOG_FETCH_CONCURRENCY = max(1,  min(10, int(os.getenv("CATALOG_FETCH_CONCURRENCY", "5"))))
MAX_ENRICH_NEW            = max(10, min(80, int(os.getenv("MAX_ENRICH_NEW",            "25"))))

PERSISTENT_CACHE_PATH = "/data/cache_peliculas.json"
CACHE_SAVE_EVERY      = 10

# ---------------------------------------------------------------------------
# STREAMING (FIX DEFINITIVO)
# ---------------------------------------------------------------------------
# 🔧 Optimización de Buffering: Aumentamos el tamaño de chunk para streaming más fluido
STREAM_CHUNK_SIZE = max(
    256 * 1024,
    min(4 * 1024 * 1024, int(os.getenv("STREAM_CHUNK_SIZE", str(1024 * 1024))))
)

# ---------------------------------------------------------------------------
# THUMBNAILS
# ---------------------------------------------------------------------------
THUMB_CACHE_TTL = max(60, int(os.getenv("THUMB_CACHE_TTL", "3600")))
THUMB_CACHE_MAX = max(50, min(2000, int(os.getenv("THUMB_CACHE_MAX", "500"))))

# 🔧 NUEVO: Tamaño estándar de miniaturas
TARGET_THUMB_WIDTH  = 500
TARGET_THUMB_HEIGHT = 750

# ✅ NUEVO: Caché persistente de miniaturas en disco (/data)
THUMB_DISK_DIR = os.getenv("THUMB_DISK_DIR", "/data/thumbs").strip() or "/data/thumbs"

# ✅ NUEVO: Concurrencia de generación de miniaturas (ffmpeg + descargas)
THUMB_GEN_CONCURRENCY = max(1, min(6, int(os.getenv("THUMB_GEN_CONCURRENCY", "2"))))

# ---------------------------------------------------------------------------
# OPTIMIZACIÓN EXTRA: CACHÉ DE RECIENTES POR CANAL
# ---------------------------------------------------------------------------
SEARCH_CHANNEL_CACHE_TTL          = max(10, int(os.getenv("SEARCH_CHANNEL_CACHE_TTL", "120")))
SEARCH_CHANNEL_CACHE_LIMIT        = max(20, min(200, int(os.getenv("SEARCH_CHANNEL_CACHE_LIMIT", "80"))))
SEARCH_CHANNEL_WARMUP_CONCURRENCY = max(1, min(10, int(os.getenv("SEARCH_CHANNEL_WARMUP_CONCURRENCY", "4"))))
# 🔧 Optimización: Aumentamos el timeout para evitar errores de fetching en canales lentos
SEARCH_CHANNEL_FETCH_TIMEOUT      = float(os.getenv("SEARCH_CHANNEL_FETCH_TIMEOUT", "8.0"))
CHANNELS_READY_MAX_WAIT_SEARCH    = float(os.getenv("CHANNELS_READY_MAX_WAIT_SEARCH", "12.0"))

# ---------------------------------------------------------------------------
# ✅ NUEVO: MÍNIMO DE RESULTADOS POR CATEGORÍA
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
# CACHÉ HÍBRIDA: RAM + JSON PERSISTENTE
# ---------------------------------------------------------------------------
def _ensure_data_dir_exists():
    try:
        os.makedirs(os.path.dirname(PERSISTENT_CACHE_PATH), exist_ok=True)
    except Exception:
        pass


async def _load_persistent_cache() -> dict:
    _ensure_data_dir_exists()

    def _read():
        try:
            if not os.path.exists(PERSISTENT_CACHE_PATH):
                return {}
            with open(PERSISTENT_CACHE_PATH, "r", encoding="utf-8") as f:
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
            tmp_path = PERSISTENT_CACHE_PATH + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(cache_dict, f, ensure_ascii=False)
            os.replace(tmp_path, PERSISTENT_CACHE_PATH)
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
# ✅ NUEVO: helpers de miniaturas persistentes (/data)
# ---------------------------------------------------------------------------
def _ensure_thumb_disk_dir_exists():
    try:
        os.makedirs(THUMB_DISK_DIR, exist_ok=True)
    except Exception as e:
        print(f"⚠️  No se pudo crear THUMB_DISK_DIR={THUMB_DISK_DIR}: {e}")


def _thumb_disk_path(message_id: int, ch: int) -> str:
    safe_mid = int(message_id)
    safe_ch  = int(ch)
    return os.path.join(THUMB_DISK_DIR, f"{safe_mid}_{safe_ch}.jpg")


async def _read_file_bytes(path: str) -> bytes | None:
    def _read():
        try:
            if not path or (not os.path.exists(path)):
                return None
            with open(path, "rb") as f:
                return f.read()
        except Exception:
            return None
    return await asyncio.to_thread(_read)


async def _write_file_bytes_atomic(path: str, data: bytes) -> bool:
    def _write():
        try:
            if not path or not data:
                return False
            tmp_path = path + ".tmp"
            with open(tmp_path, "wb") as f:
                f.write(data)
            os.replace(tmp_path, path)
            return True
        except Exception as e:
            print(f"⚠️  Error guardando miniatura en disco: {e}")
            try:
                if path and os.path.exists(path + ".tmp"):
                    os.unlink(path + ".tmp")
            except Exception:
                pass
            return False
    return await asyncio.to_thread(_write)


async def _thumb_get_from_memory_or_disk(message_id: int, ch: int) -> tuple[bytes | None, str]:
    """
    Devuelve (bytes, mime). Si no existe, (None, "image/jpeg").
    - Primero mira caché RAM
    - Luego mira disco /data/thumbs
    """
    mime = "image/jpeg"
    try:
        thumb_cache = getattr(app.state, "thumb_cache", {})
        cache_key   = f"{int(message_id)}:{int(ch)}"
        async with app.state.thumb_cache_lock:
            cached = thumb_cache.get(cache_key)
            if cached:
                ts, data, cmime = cached
                if time.monotonic() - ts < THUMB_CACHE_TTL and data:
                    return data, (cmime or mime)
    except Exception:
        pass

    # disco
    try:
        path = _thumb_disk_path(int(message_id), int(ch))
        data = await _read_file_bytes(path)
        if data and _is_valid_image_bytes(data):
            # subimos a RAM
            try:
                thumb_cache = getattr(app.state, "thumb_cache", {})
                cache_key   = f"{int(message_id)}:{int(ch)}"
                async with app.state.thumb_cache_lock:
                    _thumb_cache_prune(thumb_cache)
                    thumb_cache[cache_key] = (time.monotonic(), data, mime)
            except Exception:
                pass
            return data, mime
    except Exception:
        pass

    return None, mime


async def _mark_thumb_inflight(cache_key: str) -> bool:
    """
    Devuelve True si logró marcar como inflight (es decir, no estaba en progreso).
    """
    try:
        lock = getattr(app.state, "thumb_inflight_lock", None)
        infl = getattr(app.state, "thumb_inflight", None)
        if lock is None or infl is None:
            return False
        async with lock:
            if cache_key in infl:
                return False
            infl.add(cache_key)
            return True
    except Exception:
        return False


async def _unmark_thumb_inflight(cache_key: str) -> None:
    try:
        lock = getattr(app.state, "thumb_inflight_lock", None)
        infl = getattr(app.state, "thumb_inflight", None)
        if lock is None or infl is None:
            return
        async with lock:
            infl.discard(cache_key)
    except Exception:
        pass


async def _generate_and_persist_thumb(message_id: int, ch: int) -> None:
    """
    Generación real (background):
    - descarga miniatura embebida o extrae frame con ffmpeg
    - recorta a 500x750
    - guarda en /data/thumbs y RAM cache
    """
    cache_key = f"{int(message_id)}:{int(ch)}"
    sem = getattr(app.state, "thumb_gen_semaphore", None)
    if sem is None:
        sem = asyncio.Semaphore(THUMB_GEN_CONCURRENCY)

    try:
        async with sem:
            # Evitar trabajo si ya existe en disco mientras esperábamos
            path = _thumb_disk_path(int(message_id), int(ch))
            if os.path.exists(path):
                return

            entities = getattr(app.state, "entities", [app.state.entity])
            entity   = (
                entities[ch]
                if (0 <= ch < len(entities) and entities[ch] is not None)
                else app.state.entity
            )

            message = await client.get_messages(entity, ids=int(message_id))
            if not message:
                return

            thumb_data = None

            # --- Intento 1: foto del mensaje ---
            if hasattr(message, 'photo') and message.photo:
                try:
                    thumb_data = await client.download_media(message.photo, bytes)
                except Exception:
                    thumb_data = None

            # --- Intento 2: miniatura embebida del documento ---
            if not thumb_data and message.document and message.document.thumbs:
                try:
                    thumb_data = await client.download_media(
                        message.document.thumbs[-1], bytes
                    )
                except Exception:
                    thumb_data = None

            # --- Intento 3: extraer frame real del video (ROBUSTO) ---
            if not thumb_data:
                is_video = (
                    message.document is not None
                    and message.file is not None
                    and message.file.mime_type is not None
                    and "video" in (message.file.mime_type or "").lower()
                )
                if is_video:
                    try:
                        # background: damos más margen global, pero sin bloquear endpoints
                        thumb_data = await asyncio.wait_for(
                            _extract_video_frame(message),
                            timeout=45.0,
                        )
                    except asyncio.TimeoutError:
                        thumb_data = None
                    except Exception:
                        thumb_data = None

            if not thumb_data:
                return

            if not _is_valid_image_bytes(thumb_data):
                return

            # recorte estándar
            thumb_data = _crop_cover_to_poster(thumb_data)
            if not thumb_data:
                return

            # guardar en disco
            ok = await _write_file_bytes_atomic(path, thumb_data)
            if not ok:
                return

            # guardar en RAM cache
            try:
                thumb_cache = getattr(app.state, "thumb_cache", {})
                async with app.state.thumb_cache_lock:
                    _thumb_cache_prune(thumb_cache)
                    thumb_cache[cache_key] = (time.monotonic(), thumb_data, "image/jpeg")
            except Exception:
                pass

    except Exception as e:
        print(f"⚠️  Error generando miniatura BG ({cache_key}): {e}")
    finally:
        await _unmark_thumb_inflight(cache_key)


async def _ensure_thumb_bg(message_id: int, ch: int) -> None:
    """
    Disparar y olvidar:
    - si existe (RAM/disco): nada
    - si no existe: marca inflight y crea task
    """
    try:
        data, _ = await _thumb_get_from_memory_or_disk(int(message_id), int(ch))
        if data:
            return

        cache_key = f"{int(message_id)}:{int(ch)}"
        marked = await _mark_thumb_inflight(cache_key)
        if not marked:
            return

        asyncio.create_task(_generate_and_persist_thumb(int(message_id), int(ch)))
    except Exception:
        # Si algo falla antes de marcar, no pasa nada.
        pass


async def _prewarm_thumbnails_from_results(enriched_results: list) -> None:
    """
    Recorre resultados ya enriquecidos y dispara generación BG de miniaturas
    SOLO cuando la imagen actual sea del tipo /thumb/...
    (no bloquea, no altera respuesta).
    """
    try:
        if not enriched_results:
            return

        # límite suave para no disparar demasiados ffmpeg por búsqueda
        max_to_prewarm = max(10, min(50, int(os.getenv("THUMB_PREWARM_LIMIT", "30"))))
        count = 0

        for r in (enriched_results or []):
            if count >= max_to_prewarm:
                break

            try:
                img = (r.get("imagen_url") or "").strip()
                if "/thumb/" not in img:
                    continue

                mid = r.get("id")
                if mid is None:
                    continue

                # ch desde pelicula_url (stream_url)
                pu = (r.get("pelicula_url") or r.get("stream_url") or "").strip()
                ch = _extract_ch_from_stream_url(pu) if pu else 0

                count += 1
                await _ensure_thumb_bg(int(mid), int(ch))
            except Exception:
                continue

    except Exception as e:
        print(f"⚠️  Error en prewarm thumbnails: {e}")


# ---------------------------------------------------------------------------
# 🔧 NUEVO: Recortar/redimensionar imagen a 500x750 con cover mode
# ---------------------------------------------------------------------------
def _crop_cover_to_poster(image_data: bytes) -> bytes:
    """
    Redimensiona y recorta image_data al tamaño estándar TARGET_THUMB_WIDTH x TARGET_THUMB_HEIGHT
    usando modo 'cover': escala para cubrir toda el área y luego recorta al centro.
    Siempre devuelve JPEG. Si Pillow no está disponible, devuelve los bytes originales.
    """
    if not _PIL_AVAILABLE or not image_data:
        return image_data
    try:
        img = _PIL_Image.open(io.BytesIO(image_data))
        # Convertir a RGB (elimina canal alfa si existe)
        if img.mode != "RGB":
            img = img.convert("RGB")

        target_w, target_h = TARGET_THUMB_WIDTH, TARGET_THUMB_HEIGHT
        src_w, src_h = img.size

        if src_w == 0 or src_h == 0:
            return image_data

        # Cover: escalar para que ninguna dimensión quede por debajo del objetivo
        scale = max(target_w / src_w, target_h / src_h)
        new_w = max(int(src_w * scale), target_w)
        new_h = max(int(src_h * scale), target_h)

        img = img.resize((new_w, new_h), _PIL_Image.LANCZOS)

        # Recorte centrado
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
# ✅ FIX MINIATURAS: _thumb_url_for_message valida que el ID sea numérico
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
# 🔧 MODIFICADO: YouTube thumbnail apunta al proxy /ytthumb/{vid}
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
                # 🔧 Apunta al proxy interno que recorta a 500x750
                return _build_public_url(f"/ytthumb/{vid}")
        if "youtu.be/" in stream_url:
            vid = stream_url.rstrip("/").split("/")[-1]
            if vid:
                # 🔧 Apunta al proxy interno que recorta a 500x750
                return _build_public_url(f"/ytthumb/{vid}")
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 🔧 NUEVO (ROBUSTO): Validación fuerte de imagen para evitar vacías/incorrectas
# ---------------------------------------------------------------------------
def _is_valid_image_bytes(data: bytes) -> bool:
    if not data or len(data) < 1500:
        return False

    # Firma básica rápida
    if not (data.startswith(b"\xff\xd8\xff") or data.startswith(b"\x89PNG\r\n\x1a\n") or (data[:4] == b"RIFF" and b"WEBP" in data[:16])):
        # Podría ser válido igualmente, pero evitamos basura binaria típica
        # (si Pillow está disponible, lo verificamos igual)
        pass

    if _PIL_AVAILABLE:
        try:
            img = _PIL_Image.open(io.BytesIO(data))
            img.verify()  # valida estructura sin decodificar todo
            # Reabrir para leer size (verify() deja el objeto en estado no usable)
            img2 = _PIL_Image.open(io.BytesIO(data))
            w, h = img2.size
            if w is None or h is None:
                return False
            if w < 64 or h < 64:
                return False
            return True
        except Exception:
            return False

    # Sin Pillow: validación mínima por tamaño
    return len(data) > 5000


# ---------------------------------------------------------------------------
# 🔧 NUEVO (ROBUSTO): Extrae un frame real del video Telegram cuando no hay miniatura
# ---------------------------------------------------------------------------
async def _extract_video_frame(message) -> bytes | None:
    """
    MODO ROBUSTO/AGRESIVO:
    - Reintenta extracción en: segundo 120, 60, 2, 300 (en ese orden).
    - Usa ffmpeg con parámetros de velocidad (probesize/analyzeduration bajos) en primer intento.
    - Si falla, usa un modo robusto con mayor tolerancia.
    - Si falla con descarga parcial, escala progresivamente el tamaño descargado.
    - Último recurso: descarga completa del video y vuelve a intentar.
    - Valida que la imagen sea real (no vacía/incorrecta) antes de devolverla.
    - Maneja errores sin detener el proceso.
    """
    duration_secs = 0
    if message.document and message.document.attributes:
        for attr in message.document.attributes:
            if hasattr(attr, 'duration'):
                duration_secs = attr.duration
                break

    # ✅ Preferencia por ~minuto 2 para posters más "representativos"
    seek_points = [120, 60, 2, 300]

    # Si conocemos duración, evitamos buscar fuera (sin romper la estrategia):
    # Clamp suave (mantiene el orden y evita t>=duración).
    if isinstance(duration_secs, int) and duration_secs > 0:
        clamped = []
        for t in seek_points:
            tt = max(0, min(int(t), max(0, duration_secs - 1)))
            clamped.append(tt)
        # Deduplicar manteniendo orden
        seen_t = set()
        seek_points = []
        for t in clamped:
            if t not in seen_t:
                seen_t.add(t)
                seek_points.append(t)

    vf_path  = None
    out_path = None
    full_download_path = None

    try:
        # ------------------------------------------------------------
        # 1) Descarga progresiva (prefijo) para intentar rápido
        # ------------------------------------------------------------
        progressive_limits = [
            8  * 1024 * 1024,    # 8 MB
            25 * 1024 * 1024,    # 25 MB
            60 * 1024 * 1024,    # 60 MB
        ]

        downloaded = bytearray()
        downloaded_len = 0

        async def _download_prefix_to(limit_bytes: int) -> bytes | None:
            nonlocal downloaded_len, downloaded
            if limit_bytes <= 0:
                return None
            if downloaded_len >= limit_bytes:
                return bytes(downloaded)

            try:
                # Descarga incremental desde offset ya descargado
                async for chunk in client.iter_download(
                    message.media,
                    offset=downloaded_len,
                    limit=(limit_bytes - downloaded_len),
                    chunk_size=512 * 1024,
                ):
                    if not chunk:
                        break
                    downloaded.extend(chunk)
                    downloaded_len += len(chunk)
                    if downloaded_len >= limit_bytes:
                        break
                return bytes(downloaded) if downloaded_len > 0 else None
            except Exception as e:
                print(f"⚠️  Error descargando prefijo de video (msg {getattr(message, 'id', '?')}): {e}")
                return bytes(downloaded) if downloaded_len > 0 else None

        def _ffmpeg_extract_frame_fast(input_path: str, seek_time: int, output_path: str) -> bool:
            """
            FFmpeg ULTRA-RÁPIDO:
            - analyzeduration/probesize muy bajos para evitar análisis completo.
            - seek antes de -i
            - 1 frame
            - escala/crop directo a 500x750 (reduce trabajo de Pillow y salida uniforme).
            """
            try:
                vf = (
                    f"scale={TARGET_THUMB_WIDTH}:{TARGET_THUMB_HEIGHT}:force_original_aspect_ratio=increase,"
                    f"crop={TARGET_THUMB_WIDTH}:{TARGET_THUMB_HEIGHT}"
                )
                cmd = [
                    "ffmpeg", "-y",
                    "-hide_banner",
                    "-loglevel", "error",
                    "-nostdin",
                    "-ss", str(int(seek_time)),
                    "-probesize", "32k",
                    "-analyzeduration", "0",
                    "-i", input_path,
                    "-map", "0:v:0",
                    "-an", "-sn",
                    "-frames:v", "1",
                    "-vf", vf,
                    "-q:v", "3",
                    "-f", "image2",
                    output_path,
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    timeout=5,
                )

                if result is None:
                    return False
                if not os.path.exists(output_path):
                    return False

                try:
                    with open(output_path, "rb") as f:
                        frame_data = f.read()
                except Exception:
                    return False

                if not _is_valid_image_bytes(frame_data):
                    return False

                return True
            except subprocess.TimeoutExpired:
                return False
            except Exception:
                return False

        def _ffmpeg_extract_frame_robust(input_path: str, seek_time: int, output_path: str) -> bool:
            """
            FFmpeg ROBUSTO:
            - tolerancia a corrupción
            - analyzeduration/probesize altos (fallback)
            """
            try:
                vf = (
                    f"scale={TARGET_THUMB_WIDTH}:{TARGET_THUMB_HEIGHT}:force_original_aspect_ratio=increase,"
                    f"crop={TARGET_THUMB_WIDTH}:{TARGET_THUMB_HEIGHT}"
                )
                cmd = [
                    "ffmpeg", "-y",
                    "-hide_banner",
                    "-loglevel", "error",
                    "-nostdin",
                    "-fflags", "+genpts+discardcorrupt",
                    "-err_detect", "ignore_err",
                    "-analyzeduration", "100M",
                    "-probesize", "100M",
                    "-ss", str(int(seek_time)),
                    "-i", input_path,
                    "-map", "0:v:0",
                    "-an", "-sn",
                    "-frames:v", "1",
                    "-vf", vf,
                    "-q:v", "3",
                    "-f", "image2",
                    output_path,
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    timeout=7,
                )

                if result is None:
                    return False
                if not os.path.exists(output_path):
                    return False

                try:
                    with open(output_path, "rb") as f:
                        frame_data = f.read()
                except Exception:
                    return False

                if not _is_valid_image_bytes(frame_data):
                    return False

                return True
            except subprocess.TimeoutExpired:
                return False
            except Exception:
                return False

        async def _try_extract_from_bytes(video_bytes: bytes, seek_list: list[int]) -> bytes | None:
            nonlocal vf_path, out_path
            if not video_bytes:
                return None

            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as vf:
                vf.write(video_bytes)
                vf_path = vf.name

            out_path = vf_path + "_frame.jpg"

            # Intentos por puntos de tiempo (FAST primero; si falla, ROBUST)
            for t in seek_list:
                # Limpia salida anterior
                try:
                    if os.path.exists(out_path):
                        os.unlink(out_path)
                except Exception:
                    pass

                ok_fast = await asyncio.to_thread(_ffmpeg_extract_frame_fast, vf_path, int(t), out_path)
                ok = ok_fast
                if not ok:
                    ok = await asyncio.to_thread(_ffmpeg_extract_frame_robust, vf_path, int(t), out_path)

                if not ok:
                    continue

                # Leer bytes válidos
                try:
                    with open(out_path, "rb") as f:
                        data = f.read()
                    if _is_valid_image_bytes(data):
                        print(f"   🎞️  Frame extraído msg {message.id} @ {t}s")
                        return data
                except Exception:
                    continue

            return None

        # ------------------------------------------------------------
        # 2) Intentos con descarga progresiva
        # ------------------------------------------------------------
        for lim in progressive_limits:
            video_data = await _download_prefix_to(lim)
            if not video_data:
                continue

            try:
                frame = await _try_extract_from_bytes(video_data, seek_points)
                if frame and _is_valid_image_bytes(frame):
                    return frame
            except Exception as e:
                print(f"⚠️  Error intentando extraer frame con prefijo (lim={lim}) msg {getattr(message, 'id', '?')}: {e}")
                continue
            finally:
                # Limpieza de temporales intermedios
                for p in [vf_path, out_path]:
                    if p:
                        try:
                            if os.path.exists(p):
                                os.unlink(p)
                        except Exception:
                            pass
                vf_path = None
                out_path = None

        # ------------------------------------------------------------
        # 3) Último recurso: descarga completa y reintento
        # ------------------------------------------------------------
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as ffull:
                full_download_path = ffull.name

            await client.download_media(message.media, file=full_download_path)

            out_path = full_download_path + "_frame.jpg"
            for t in seek_points:
                try:
                    if os.path.exists(out_path):
                        os.unlink(out_path)
                except Exception:
                    pass

                ok_fast = await asyncio.to_thread(_ffmpeg_extract_frame_fast, full_download_path, int(t), out_path)
                ok = ok_fast
                if not ok:
                    ok = await asyncio.to_thread(_ffmpeg_extract_frame_robust, full_download_path, int(t), out_path)

                if not ok:
                    continue

                try:
                    with open(out_path, "rb") as f:
                        frame_data = f.read()
                    if _is_valid_image_bytes(frame_data):
                        print(f"   🎞️  Frame extraído (FULL) msg {message.id} @ {t}s")
                        return frame_data
                except Exception:
                    continue

        except Exception as e:
            print(f"⚠️  Error en fallback descarga completa (msg {getattr(message, 'id', '?')}): {e}")

        return None

    except asyncio.TimeoutError:
        print(f"⚠️  Timeout extrayendo frame del video (msg {getattr(message, 'id', '?')})")
        return None
    except Exception as e:
        print(f"⚠️  Error en _extract_video_frame (msg {getattr(message, 'id', '?')}): {e}")
        return None
    finally:
        # Limpieza de archivos temporales (robusta)
        for p in [vf_path, out_path, full_download_path]:
            if p:
                try:
                    if os.path.exists(p):
                        os.unlink(p)
                except Exception:
                    pass


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

    # ✅ NUEVO: control de tasks BG de thumbnails (no bloquear)
    app.state.thumb_gen_semaphore = asyncio.Semaphore(THUMB_GEN_CONCURRENCY)
    app.state.thumb_inflight      = set()
    app.state.thumb_inflight_lock = asyncio.Lock()

    # ✅ NUEVO: asegurar directorio persistente para thumbs
    _ensure_thumb_disk_dir_exists()

    app.state.meta_cache = await _load_persistent_cache()
    print(f"🧠 Caché persistente cargada: {len(app.state.meta_cache)} entradas")

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
        loaded = sum(1 for e in app.state.entities if e is not None)
        print(f"✅ Todos los canales cargados: {loaded} disponibles")

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
        await _save_persistent_cache(app.state.meta_cache)

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
    channels_up = sum(1 for e in getattr(app.state, "entities", []) if e is not None)
    return JSONResponse({
        "status":          "ok",
        "channels_ready":  getattr(app.state, "channels_ready", False),
        "channels_loaded": channels_up,
        "cache_entries":   len(getattr(app.state, "meta_cache", {})),
    })


# ---------------------------------------------------------------------------
# EXTRACCIÓN DE TÍTULO LIMPIA
# ---------------------------------------------------------------------------
_MAX_TITLE_LEN = 100


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


def extract_chapter_number(result: dict) -> int:
    title = result.get("title", "")
    match = re.search(
        r'(?:cap[ií]tulo|cap[.]?|ep(?:isodio)?[.]?|parte|vol(?:[.]|umen)?)\s*[:\-]?\s*(\d+)',
        title, re.IGNORECASE,
    )
    if match:
        return int(match.group(1))
    numbers = re.findall(r'\d+', title)
    return int(numbers[-1]) if numbers else 0


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
            "size":       "N/A",
            "stream_url": f"https://www.youtube.com/watch?v={video_id}",
        }]
    except Exception as e:
        print(f"⚠️  Error usando YouTube fallback: {e}")
        return []


# ---------------------------------------------------------------------------
# HELPERS: limpieza de títulos
# ---------------------------------------------------------------------------
_ROMAN_RE = r"(?:I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV|XVI|XVII|XVIII|XIX|XX)"

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
    if not title:
        return None
    m = re.search(r"(?:\(|\b)(19\d{2}|20\d{2})(?:\)|\b)", title)
    return m.group(1) if m else None


def _remove_bracketed_text(s: str) -> str:
    if not s:
        return s
    out = s
    for _ in range(3):
        out = re.sub(r"\([^()]*\)", " ", out)
        out = re.sub(r"\[[^\[\]]*\]", " ", out)
    return out


def _clean_title_for_api(title: str) -> str:
    t = _strip_decorations(title)
    t = _remove_bracketed_text(t)

    for pat in _NOISE_PATTERNS:
        t = re.sub(pat, " ", t, flags=re.IGNORECASE)

    t = re.sub(r"[|•·_]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    if len(t) > _MAX_TITLE_LEN:
        for sep in ('.', ',', ';', '!', '?', ' - '):
            idx = t.find(sep, 15)
            if 15 < idx < _MAX_TITLE_LEN:
                t = t[:idx].strip()
                break
        else:
            t = t[:_MAX_TITLE_LEN].strip()

    return t


def _build_tmdb_query_from_title(title: str):
    raw  = _strip_decorations(title)
    year = _extract_year_from_title(raw)
    q    = _clean_title_for_api(raw)
    q = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", q).strip()
    q = re.sub(
        rf"\b(?:cap[ií]tulo|cap[.]?|ep(?:isodio)?[.]?|parte|vol(?:[.]|umen)?|"
        rf"temporada|season)\s*[:\-]?\s*(?:\d+|{_ROMAN_RE})\b",
        " ", q, flags=re.IGNORECASE,
    )
    q = re.sub(r"\s+", " ", q).strip()
    return (q or raw), year


def _placeholder_image_for_title(title: str) -> str:
    return PLACEHOLDER_IMAGE_BASE


def _nn_str(v, default: str = "") -> str:
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
                    if message.file else "N/A"
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
# NORMALIZACIÓN DEL ESQUEMA JSON DE RESPUESTA
# 🔧 CAMBIO: elimina fallback a imagen genérica en schema (no altera estructura)
# ---------------------------------------------------------------------------
def _to_peliculas_json_schema(items: list) -> list:
    out = []
    for it in (items or []):
        titulo       = it.get("titulo") or it.get("title") or it.get("nombre") or "Película"
        imagen_url   = it.get("imagen_url") or ""
        pelicula_url = it.get("pelicula_url") or it.get("stream_url") or it.get("url") or ""
        desc         = (it.get("descripcion") or it.get("sinopsis") or "").strip()

        obj = {
            "titulo":       _nn_str(titulo,       "Película"),
            "imagen_url":   _nn_str(imagen_url,   ""),
            "pelicula_url": _nn_str(pelicula_url, ""),
        }
        if desc and desc != "Sin descripción disponible.":
            obj["descripcion"] = desc

        _fecha  = it.get("fecha_lanzamiento")
        _dur    = it.get("duracion")
        _idioma = it.get("idioma_original")
        _pop    = it.get("popularidad")
        _punt   = it.get("puntuacion")
        _gen    = it.get("generos")
        _anio   = it.get("año")

        if _fecha  and str(_fecha).strip()  not in ("", "N/A"):
            obj["fecha_lanzamiento"] = str(_fecha).strip()
        if _dur    and str(_dur).strip()    not in ("", "N/A"):
            obj["duracion"]          = str(_dur).strip()
        if _idioma and str(_idioma).strip() not in ("", "N/A"):
            obj["idioma_original"]   = str(_idioma).strip()
        if _pop  is not None and _pop  != 0:
            obj["popularidad"]       = _pop
        if _punt is not None and _punt != 0:
            obj["puntuacion"]        = _punt
        if _gen    and str(_gen).strip()    not in ("", "N/A"):
            obj["generos"]           = str(_gen).strip()
        if _anio   and str(_anio).strip()   not in ("", "N/A"):
            obj["año"]               = str(_anio).strip()

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
            if not r_year.startswith(year):
                continue

        if desde or hasta:
            r_year_str = str(r.get("año") or "")
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
            _tmp2 = unicodedata.normalize("NFD", r_gen_raw)
            r_gen_na = "".join(c for c in _tmp2 if unicodedata.category(c) != "Mn")
            if not (genre_norm in r_gen_raw or genre_norm_na in r_gen_na):
                continue

        if lang_upper:
            r_lang = (r.get("idioma_original") or "").upper()
            if lang_upper not in r_lang:
                continue

        filtered.append(r)

    return filtered


# ---------------------------------------------------------------------------
# COROUTINE NULA
# ---------------------------------------------------------------------------
async def _noop():
    return None


# ---------------------------------------------------------------------------
# GOOGLE KNOWLEDGE GRAPH
# ---------------------------------------------------------------------------
async def _google_kg_search(
    http,
    query_title: str,
    year,
):
    if not GOOGLE_KG_API_KEY:
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
        r = await http.get("https://kgsearch.googleapis.com/v1/entities:search", params=params)
        r.raise_for_status()
        data  = r.json()
        items = _safe_get(data, "itemListElement", []) or []
        if not isinstance(items, list) or not items:
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
        return {
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
    except Exception as e:
        print(f"⚠️  Google KG error ({query_title}): {e}")
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
# GEMINI AI: completa metadatos faltantes (SOLO EN /search, LIMITADO A 10)
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

    if _GEMINI_CALL_COUNTER["count"] >= 10:
        print(f"   ⚠️  Límite de IA (10) alcanzado, no se usa Gemini para '{title}'")
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

        r = await http.post(
            GEMINI_API_URL,
            params={"key": GEMINI_API_KEY},
            json=payload,
            timeout=5.0,
        )
        r.raise_for_status()

        data       = r.json()
        candidates = data.get("candidates") or []
        if not candidates:
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
            return None

        print(f"   🤖 Gemini ({_GEMINI_CALL_COUNTER['count']}/10) → completó metadatos para '{title}'")
        return result

    except json.JSONDecodeError:
        print(f"⚠️  Gemini devolvió JSON inválido para '{title}'")
        return None
    except Exception as e:
        print(f"⚠️  Gemini error ({title}): {e}")
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
            await _save_persistent_cache(app.state.meta_cache)
            app.state.meta_cache_dirty = False


# ---------------------------------------------------------------------------
# ENRIQUECIMIENTO PRINCIPAL
# 🔧 CAMBIO: elimina fallback a imagen genérica para Telegram (YouTube intacto)
# ---------------------------------------------------------------------------
async def enrich_results_with_tmdb(
    results: list,
    max_new=None,
    use_gemini: bool = False,
) -> list:
    request_cache: dict = {}
    semaphore     = asyncio.Semaphore(MAX_CONCURRENCY)
    new_counter   = {"n": 0}
    limit_new     = max_new if max_new is not None else len(results)
    _timeout      = httpx.Timeout(connect=2.0, read=3.5, write=2.0, pool=1.0)

    async with httpx.AsyncClient(timeout=_timeout) as http:

        async def enrich_one(r: dict) -> dict:
            title_raw            = r.get("title") or "Película"
            fallback_title       = _strip_decorations(title_raw)
            fallback_year_title  = _extract_year_from_title(title_raw)
            query_title, year    = _build_tmdb_query_from_title(title_raw)
            ck                   = _cache_key_from_query(query_title, year)

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
                            "descripcion":           "Sin descripción disponible.",
                            "fecha_lanzamiento":     "",
                            "duracion":              "",
                            "idioma_original":       "",
                            "popularidad":           0,
                            "puntuacion":            0,
                            "generos":               "",
                            "año":                   fallback_year_title or year or "N/A",
                            "id":                    r.get("id"),
                            "size":                  _nn_str(r.get("size"), "N/A"),
                            "descripcion_detallada": "",
                        }

                    new_counter["n"] += 1

                    kg = tmdb = tvmaze = None

                    async with semaphore:
                        kg_coro   = (_google_kg_search(http, query_title, year) if GOOGLE_KG_API_KEY else _noop())
                        tmdb_coro = (_tmdb_search_and_details(http, query_title, year) if TMDB_API_KEY else _noop())
                        kg, tmdb  = await asyncio.gather(kg_coro, tmdb_coro)

                        combined_has_image    = bool(
                            (isinstance(tmdb, dict) and tmdb.get("imagen_url")) or
                            (isinstance(kg,   dict) and kg.get("imagen_url"))
                        )
                        combined_has_synopsis = bool(
                            (isinstance(kg,   dict) and kg.get("sinopsis")) or
                            (isinstance(tmdb, dict) and tmdb.get("sinopsis"))
                        )
                        combined_has_year     = bool(
                            (isinstance(kg,   dict) and kg.get("año")) or
                            (isinstance(tmdb, dict) and tmdb.get("año"))
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

            thumb_img = _thumb_url_for_message(r.get("id"), pelicula_url)
            yt_img    = _youtube_thumb_from_stream_url(pelicula_url)

            # 🔧 IMPORTANTE:
            # - Telegram sin póster (TMDB/KG/TVMaze) => usar SIEMPRE /thumb (miniatura real del contenido).
            # - YouTube se mantiene igual (usa /ytthumb).
            # - Sin imagen disponible: NO se inyecta placeholder.
            imagen_url = meta_img or thumb_img or yt_img or ""

            descripcion = (meta.get("sinopsis") if isinstance(meta, dict) else None) or "Sin descripción disponible."
            year_out    = (meta.get("año") if isinstance(meta, dict) else None) or fallback_year_title or year or "N/A"

            return {
                "titulo":                _nn_str(meta.get("titulo") if isinstance(meta, dict) else None, fallback_title or "Película"),
                "imagen_url":            _nn_str(imagen_url, ""),
                "pelicula_url":          _nn_str(pelicula_url, ""),
                "descripcion":           _nn_str(descripcion,  "Sin descripción disponible."),
                "fecha_lanzamiento":     _nn_str(meta.get("fecha_lanzamiento") if isinstance(meta, dict) else None, ""),
                "duracion":              _nn_str(meta.get("duracion")           if isinstance(meta, dict) else None, ""),
                "idioma_original":       _nn_str(meta.get("idioma_original")    if isinstance(meta, dict) else None, ""),
                "popularidad":           _nn_num(meta.get("popularidad")        if isinstance(meta, dict) else None, 0),
                "puntuacion":            _nn_num(meta.get("puntuacion")         if isinstance(meta, dict) else None, 0),
                "generos":               _nn_str(meta.get("generos")            if isinstance(meta, dict) else None, ""),
                "año":                   _nn_str(year_out, "N/A"),
                "id":                    r.get("id"),
                "size":                  _nn_str(r.get("size"), "N/A"),
                "descripcion_detallada": _nn_str(meta.get("descripcion_detallada") if isinstance(meta, dict) else None, ""),
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
                    "imagen_url":            "",
                    "pelicula_url":          "",
                    "descripcion":           "Sin descripción disponible.",
                    "fecha_lanzamiento":     "",
                    "duracion":              "",
                    "idioma_original":       "",
                    "popularidad":           0,
                    "puntuacion":            0,
                    "generos":               "",
                    "año":                   "N/A",
                    "id":                    None,
                    "size":                  "N/A",
                    "descripcion_detallada": "",
                })
        return final


# ---------------------------------------------------------------------------
# FORMATO BÁSICO (sin APIs)
# 🔧 CAMBIO: elimina fallback a imagen genérica
# ---------------------------------------------------------------------------
def _format_results_without_apis(final_results: list) -> list:
    formatted = []
    for r in final_results:
        title_raw = r.get("title") or "Película"
        titulo    = _strip_decorations(title_raw) or "Película"
        year      = _extract_year_from_title(title_raw) or "N/A"

        pelicula_url = r.get("stream_url") or ""

        thumb_img = _thumb_url_for_message(r.get("id"), pelicula_url)
        yt_img    = _youtube_thumb_from_stream_url(pelicula_url)
        img_final = thumb_img or yt_img or ""

        formatted.append({
            "titulo":                titulo,
            "imagen_url":            img_final,
            "pelicula_url":          pelicula_url,
            "descripcion":           "Sin descripción disponible.",
            "fecha_lanzamiento":     "",
            "duracion":              "",
            "idioma_original":       "",
            "popularidad":           0,
            "puntuacion":            0,
            "generos":               "",
            "año":                   year,
            "id":                    r.get("id"),
            "size":                  _nn_str(r.get("size"), "N/A"),
            "descripcion_detallada": "",
        })
    return formatted


# ---------------------------------------------------------------------------
# ENDPOINT /search
# ---------------------------------------------------------------------------
@app.get("/search")
async def search(
    q:        str | None = Query(None,  description="Texto de búsqueda (mín. 3 caracteres)"),
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
    if q is not None and len(q.strip()) < 3:
        raise HTTPException(
            status_code=400,
            detail="El parámetro 'q' debe tener al menos 3 caracteres",
        )

    _GEMINI_CALL_COUNTER["count"] = 0

    try:
        # Esperar a que los canales estén listos
        if not getattr(app.state, "channels_ready", False):
            waited = 0.0
            while not getattr(app.state, "channels_ready", False) and waited < CHANNELS_READY_MAX_WAIT_SEARCH:
                await asyncio.sleep(0.3)
                waited += 0.3

        entities = getattr(app.state, "entities", [app.state.entity])
        all_entities_indexed = [(i, e) for i, e in enumerate(entities) if e is not None]

        # Determinar qué canales buscar
        if canal:
            canal_clean = canal.strip().lstrip('@').lower()
            entities_indexed = [
                (i, e) for i, e in all_entities_indexed
                if (getattr(e, 'username', '') or '').lower() == canal_clean
            ]
            if not entities_indexed:
                entities_indexed = all_entities_indexed[:1]
        elif genre:
            genre_channels = _get_genre_channels(genre)
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
                if q:
                    msg_iter = client.iter_messages(entity, search=q.strip())

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
                                    if message.file else "N/A"
                                ),
                                "stream_url": direct_link,
                            })
                            if len(results) >= 50:
                                break
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

        unique.sort(key=extract_chapter_number)
        final_results = unique[:50]

        print(f"🎯 Resultados: {len(final_results)} únicos (de {len(all_results)} totales)")

        # ✅ NUEVO: Complementar hasta mínimo MIN_CATEGORY_RESULTS cuando se busca por categoría
        if genre and len(final_results) < MIN_CATEGORY_RESULTS:
            print(
                f"⚠️  Categoría '{genre}': solo {len(final_results)} resultado(s). "
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

        if not final_results and q:
            print("🟦 Sin resultados en Telegram. Usando respaldo YouTube...")
            yt_results = await youtube_fallback(q.strip())
            if yt_results:
                try:
                    enriched = await asyncio.wait_for(
                        enrich_results_with_tmdb(yt_results, max_new=MAX_ENRICH_NEW, use_gemini=True),
                        timeout=4.0,
                    )
                except asyncio.TimeoutError:
                    print("⚠️  /search YouTube enrichment timeout")
                    enriched = _format_results_without_apis(yt_results)
                if any([year, genre, language, desde, hasta]):
                    enriched = _apply_advanced_filters(enriched, year, genre, language, desde, hasta)

                # ✅ NUEVO: precarga async (no bloquea) para /thumb si aplica
                asyncio.create_task(_prewarm_thumbnails_from_results(enriched))

                return _to_peliculas_json_schema(enriched)

        try:
            enriched = await asyncio.wait_for(
                enrich_results_with_tmdb(final_results, max_new=MAX_ENRICH_NEW, use_gemini=True),
                timeout=4.0,
            )
        except asyncio.TimeoutError:
            print("⚠️  /search enrichment timeout — devolviendo formato básico")
            enriched = _format_results_without_apis(final_results)

        if any([year, genre, language, desde, hasta]):
            enriched = _apply_advanced_filters(enriched, year, genre, language, desde, hasta)
            print(f"🔎 Filtros avanzados aplicados → {len(enriched)} resultado(s)")

        # ✅ NUEVO: disparar precarga de miniaturas en segundo plano (NO ESPERAR)
        asyncio.create_task(_prewarm_thumbnails_from_results(enriched))

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
                                        if message.file else "N/A"
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
                enrich_results_with_tmdb(sample, max_new=MAX_ENRICH_NEW),
                timeout=8.0,
            )
        except asyncio.TimeoutError:
            print("⚠️  /catalog enrichment timeout — devolviendo formato básico")
            enriched = _format_results_without_apis(sample)

        # (Opcional, sin bloquear): precargar thumbs si el catálogo usa /thumb
        asyncio.create_task(_prewarm_thumbnails_from_results(enriched))

        return _to_peliculas_json_schema(enriched)

    except Exception as e:
        print(f"❌ Error en /catalog: {e}")
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# 🔧 NUEVO ENDPOINT: /ytthumb/{video_id}
# Proxy que descarga la miniatura de YouTube y la recorta a 500x750
# (NO MODIFICADO: se mantiene exactamente igual)
# ---------------------------------------------------------------------------
@app.get("/ytthumb/{video_id}")
async def youtube_thumbnail_proxy(video_id: str):
    cache_key = f"yt:{video_id}"

    # Verificar caché
    thumb_cache = getattr(app.state, "thumb_cache", {})
    async with app.state.thumb_cache_lock:
        cached = thumb_cache.get(cache_key)
        if cached:
            ts, data, mime = cached
            if time.monotonic() - ts < THUMB_CACHE_TTL:
                return Response(content=data, media_type=mime)

    # Intentar descargar miniatura en diferentes calidades
    thumb_data = None
    for quality in ["maxresdefault", "sddefault", "hqdefault", "mqdefault", "default"]:
        url = f"https://img.youtube.com/vi/{video_id}/{quality}.jpg"
        try:
            async with httpx.AsyncClient(timeout=5.0) as http:
                r = await http.get(url)
                # YouTube devuelve imagen 120x90 "no disponible" para maxres/sd si no existe
                # La filtramos por tamaño mínimo de contenido
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

    # 🔧 Recortar a 500x750
    processed = _crop_cover_to_poster(thumb_data)
    mime      = "image/jpeg"

    # Guardar en caché
    async with app.state.thumb_cache_lock:
        _thumb_cache_prune(thumb_cache)
        thumb_cache[cache_key] = (time.monotonic(), processed, mime)

    return Response(content=processed, media_type=mime)


# ---------------------------------------------------------------------------
# 🔧 ENDPOINT /thumb/{message_id} — MODIFICADO
# NUEVO FLUJO ASÍNCRONO (sin bloquear):
# - Primero intenta RAM cache
# - Luego intenta /data/thumbs (persistente)
# - Si no existe aún:
#     - dispara generación en segundo plano (fire & forget)
#     - responde INMEDIATO con imagen temporal (redirect a placeholder)
# ---------------------------------------------------------------------------
@app.get("/thumb/{message_id}")
async def get_thumbnail(message_id: int, request: Request, ch: int = 0):
    try:
        # 1) RAM / DISCO
        data, mime = await _thumb_get_from_memory_or_disk(int(message_id), int(ch))
        if data:
            return Response(
                content=data,
                media_type=mime,
                headers={"Cache-Control": "public, max-age=3600"},
            )

        # 2) No existe aún => disparar generación BG (no esperar)
        asyncio.create_task(_ensure_thumb_bg(int(message_id), int(ch)))

        # 3) Respuesta inmediata con imagen temporal
        # (302 evita transferir bytes desde tu servidor y es casi instantáneo)
        return Response(
            status_code=302,
            headers={"Location": PLACEHOLDER_IMAGE_BASE, "Cache-Control": "no-store"},
        )

    except Exception as e:
        print(f"⚠️  Error en /thumb/{message_id}: {e}")
        # fallback rápido (sin bloquear)
        return Response(
            status_code=302,
            headers={"Location": PLACEHOLDER_IMAGE_BASE, "Cache-Control": "no-store"},
        )


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
                "Cache-Control":     "public, max-age=3600",
                "X-Accel-Buffering": "yes", # Permitir buffering de Nginx/Proxy
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
            "Cache-Control":     "public, max-age=3600",
            "X-Accel-Buffering": "yes",
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
# ENTRYPOINT
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
