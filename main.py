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
from urllib.parse import quote_plus, urlparse, parse_qs
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
import sqlite3

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

# ✅ Segunda cuenta — credenciales completamente independientes
API_ID_2           = int(os.getenv("API_ID_2", "0"))
API_HASH_2         = os.getenv("API_HASH_2", "")
SESSION_STRING_2   = os.getenv("SESSION_STRING_2", "")   # ✅ Segunda cuenta para rotación

# ✅ Tercera cuenta — credenciales completamente independientes
API_ID_3           = int(os.getenv("API_ID_3", "0"))
API_HASH_3         = os.getenv("API_HASH_3", "")
SESSION_STRING_3   = os.getenv("SESSION_STRING_3", "")   # ✅ Tercera cuenta para rotación

PUBLIC_URL         = os.getenv("PUBLIC_URL", "").rstrip('/')
# ✅ Canal principal eliminado — @PEELYE ahora es un canal más del pool (ID: -1003093196542)
CHANNEL_IDENTIFIER = None  # Sin canal principal prioritario

# --- PUERTO INTERNO ---
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
DATA_DIR   = "/data"                                     # ✅ Volumen persistente en Fly.io
DB_PATH    = "/data/peliprex.db"                         # ✅ Base de datos persistente en Fly.io
CACHE_FILE = os.path.join(DATA_DIR, "pelis_cache.json")
THUMBS_DIR = os.path.join(DATA_DIR, "thumbnails")

# Crear directorios persistentes si no existen
os.makedirs(DATA_DIR,   exist_ok=True)
os.makedirs(THUMBS_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# OPTIMIZACIÓN / LÍMITES
# ---------------------------------------------------------------------------
MAX_CONCURRENCY           = max(5,  min(15, int(os.getenv("MAX_CONCURRENCY",           "10"))))
CATALOG_POOL_TTL          = max(60,         int(os.getenv("CATALOG_POOL_TTL",          "600")))
CATALOG_FETCH_CONCURRENCY = max(1,  min(10, int(os.getenv("CATALOG_FETCH_CONCURRENCY", "5"))))
MAX_ENRICH_NEW            = max(10, min(80, int(os.getenv("MAX_ENRICH_NEW",            "25"))))

# ✅ ANTI-FLOOD: Configuración para evitar errores HTTP 429 de Telegram
# SEARCH_MAX_CHANNELS ya no limita la búsqueda — se revisan todos los canales disponibles.
# Se mantiene la variable por compatibilidad con variables de entorno existentes.
SEARCH_MAX_CHANNELS        = max(3, min(10, int(os.getenv("SEARCH_MAX_CHANNELS",        "5"))))
# Delay entre consultas secuenciales a canales. Previene que Telegram detecte flood.
SEARCH_INTER_CHANNEL_DELAY = float(os.getenv("SEARCH_INTER_CHANNEL_DELAY", "0.7"))
# TTL del caché de búsqueda SQLite en segundos (por defecto: 24 horas).
# Si el mismo término ya fue buscado recientemente, se devuelve desde caché sin tocar Telegram.
SEARCH_CACHE_TTL_S         = int(os.getenv("SEARCH_CACHE_TTL_S", str(24 * 3600)))

CACHE_SAVE_EVERY      = 10

# ✅ ANTI-FLOOD B: Delay entre cambios de cuenta (evita ráfagas de solicitudes detectadas por Telegram)
ACCOUNT_SWITCH_DELAY  = float(os.getenv("ACCOUNT_SWITCH_DELAY", "1.2"))

# ✅ SEPARACIÓN C: Intervalo (segundos) entre actualizaciones del catálogo en background
CATALOG_BACKGROUND_INTERVAL = int(os.getenv("CATALOG_BACKGROUND_INTERVAL", str(5 * 60)))

# ---------------------------------------------------------------------------
# STREAMING (PRE-BUFFER ASÍNCRONO — MÁXIMA ESTABILIDAD)
# ---------------------------------------------------------------------------
# STREAM_CHUNK_SIZE:    tamaño de cada chunk enviado al cliente.
#                       512 KB = buffer del player se llena suave y continuo.
# STREAM_REQUEST_SIZE:  tamaño de cada petición interna a Telegram.
#                       2 MB = menos round-trips → menos micro-cortes.
# STREAM_BUFFER_CHUNKS: número de chunks que el productor pre-descarga por adelantado.
#                       El consumidor siempre tendrá datos listos aunque Telegram tarde.
STREAM_CHUNK_SIZE = max(
    128 * 1024,
    min(2 * 1024 * 1024, int(os.getenv("STREAM_CHUNK_SIZE", str(512 * 1024))))
)
STREAM_REQUEST_SIZE = max(
    512 * 1024,
    min(8 * 1024 * 1024, int(os.getenv("STREAM_REQUEST_SIZE", str(2 * 1024 * 1024))))
)
STREAM_BUFFER_CHUNKS = max(
    2,
    min(16, int(os.getenv("STREAM_BUFFER_CHUNKS", "6")))
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
# CANALES DE RESPALDO
# ---------------------------------------------------------------------------
_REQUIRED_CHANNELS = [
    -1001649167769,
    -1001009019353,
    -1003023738060,
    -1002080388176,
    -1002366853704,
    -1003093196542,
    -1003077987400,
    -1002600296992,
    -1002988677372,
    -1002845267381,
    -1001847442450,
    -1002106205720,
    -1002162451586,
    -1001585888729,
    -1002753095284,
    -1002548061413,
    -1001297657191,
    -1002820678368,
    -1001506938988,
    -1003154373292,
    -1001426044427,
    -1001162451195,
    -1001651475854,
    -1001953876584,
    -1001961301185,
    -1002013327857,
    -1001935742091,
    -1002144710521,
    -1001972244999,
    -1001656397127,
    -1001363263111,
    -1001538835649,
    -1001632627579,
    -1001649308894,
    -1001386268428,
    -1001184598291,
    -1001582126975,
    -1001755310958,
    -1001204024132,
    -1001680866519,
    -1001476044771,
    -1001910043099,
    -1002219256214,
    -1001602634035,
    -1001632627579,
    -1001632627579,
]


def _dedupe_channels(channels: list) -> list:
    seen, out = set(), []
    for ch in channels:
        # ✅ Soporte para IDs numéricos y @username
        if isinstance(ch, int):
            if ch not in seen:
                seen.add(ch)
                out.append(ch)
            continue
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


# ---------------------------------------------------------------------------
# 🆕 CACHÉ DE BÚSQUEDAS EN SQLITE (volumen persistente /data/peliprex.db)
# ---------------------------------------------------------------------------
# Reduce hasta ~90% las consultas a Telegram para búsquedas repetidas.
#
# Flujo:
#   1. Usuario busca una película.
#   2. Se genera una clave única con los parámetros de búsqueda.
#   3. Si existe en SQLite y no expiró → se devuelve directamente. NO se consulta Telegram.
#   4. Si no existe → se consulta Telegram, se guardan los resultados en SQLite.
#   5. Próximas búsquedas idénticas responden desde caché.
# ---------------------------------------------------------------------------
def _db_init_search_cache() -> None:
    """Crea la tabla de caché de búsquedas en SQLite si no existe."""
    try:
        con = sqlite3.connect(DB_PATH, check_same_thread=False)
        con.execute("""
            CREATE TABLE IF NOT EXISTS search_cache (
                query_key    TEXT PRIMARY KEY,
                results_json TEXT NOT NULL,
                created_at   REAL NOT NULL
            )
        """)
        con.commit()
        con.close()
        print(f"✅ BD caché de búsquedas inicializada: {DB_PATH}")
    except Exception as _e:
        print(f"⚠️  Error inicializando BD de búsquedas: {_e}")


def _db_search_get(query_key: str):
    """
    Recupera resultados cacheados de la BD SQLite.
    Retorna la lista de resultados si existe y no expiró, o None en caso contrario.
    [] vacío también es un resultado válido (búsqueda sin resultados).
    """
    try:
        con = sqlite3.connect(DB_PATH, check_same_thread=False)
        cur = con.execute(
            "SELECT results_json, created_at FROM search_cache WHERE query_key = ?",
            (query_key,),
        )
        row = cur.fetchone()
        con.close()
        if row is None:
            return None  # No existe en caché
        results_json, created_at = row
        if (time.time() - float(created_at)) > SEARCH_CACHE_TTL_S:
            return None  # Expirado — se consultará Telegram nuevamente
        data = json.loads(results_json)
        return data if isinstance(data, list) else None
    except Exception as _e:
        print(f"⚠️  Error leyendo caché de búsqueda: {_e}")
        return None


def _db_search_set(query_key: str, results: list) -> None:
    """Guarda resultados de búsqueda en SQLite para evitar consultas futuras a Telegram."""
    try:
        con = sqlite3.connect(DB_PATH, check_same_thread=False)
        con.execute(
            """INSERT OR REPLACE INTO search_cache
               (query_key, results_json, created_at) VALUES (?, ?, ?)""",
            (query_key, json.dumps(results, ensure_ascii=False), time.time()),
        )
        con.commit()
        con.close()
    except Exception as _e:
        print(f"⚠️  Error guardando caché de búsqueda: {_e}")


# ---------------------------------------------------------------------------
# ✅ SEPARACIÓN C: CACHÉ DE CATÁLOGO EN SQLITE (proceso de fondo independiente)
# ---------------------------------------------------------------------------
def _db_init_catalog_cache() -> None:
    """Crea la tabla de caché de catálogo en SQLite si no existe."""
    try:
        con = sqlite3.connect(DB_PATH, check_same_thread=False)
        con.execute("""
            CREATE TABLE IF NOT EXISTS catalog_raw_cache (
                id           INTEGER PRIMARY KEY CHECK (id = 1),
                items_json   TEXT NOT NULL,
                updated_at   REAL NOT NULL
            )
        """)
        con.commit()
        con.close()
        print(f"✅ BD caché de catálogo inicializada: {DB_PATH}")
    except Exception as _e:
        print(f"⚠️  Error inicializando BD de catálogo: {_e}")


def _db_catalog_get() -> list | None:
    """
    Lee el pool de catálogo cacheado desde SQLite.
    Retorna la lista de items raw (sin enriquecer) o None si no existe.
    """
    try:
        con = sqlite3.connect(DB_PATH, check_same_thread=False)
        cur = con.execute("SELECT items_json, updated_at FROM catalog_raw_cache WHERE id = 1")
        row = cur.fetchone()
        con.close()
        if row is None:
            return None
        data = json.loads(row[0])
        return data if isinstance(data, list) else None
    except Exception as _e:
        print(f"⚠️  Error leyendo caché de catálogo: {_e}")
        return None


def _db_catalog_set(items: list) -> None:
    """Guarda el pool de catálogo (items raw) en SQLite."""
    try:
        con = sqlite3.connect(DB_PATH, check_same_thread=False)
        con.execute(
            """INSERT OR REPLACE INTO catalog_raw_cache
               (id, items_json, updated_at) VALUES (1, ?, ?)""",
            (json.dumps(items, ensure_ascii=False), time.time()),
        )
        con.commit()
        con.close()
        print(f"✅ Catálogo guardado en SQLite: {len(items)} items")
    except Exception as _e:
        print(f"⚠️  Error guardando caché de catálogo: {_e}")


def _db_search_cache_key(text, genre, year, language, desde, hasta) -> str:
    """Genera una clave única reproducible para los parámetros de búsqueda."""
    parts = [
        normalize_title(text   or ""),
        (genre    or "").strip().lower(),
        (year     or "").strip(),
        (language or "").strip().upper(),
        str(desde or ""),
        str(hasta or ""),
    ]
    return "|".join(parts)


def normalize_title(title: str) -> str:
    title = (title or "").strip().lower()
    title = unicodedata.normalize("NFD", title)
    title = "".join(c for c in title if unicodedata.category(c) != "Mn")
    title = re.sub(r"\s+", " ", title)
    return title


# ---------------------------------------------------------------------------
# 🔍 BÚSQUEDA FUZZY: Levenshtein + coincidencia tolerante a errores
# ---------------------------------------------------------------------------
def _levenshtein(s1: str, s2: str) -> int:
    """Calcula la distancia de edición (Levenshtein) entre dos strings."""
    if len(s1) < len(s2):
        s1, s2 = s2, s1
    if not s2:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for c1 in s1:
        curr = [prev[0] + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(curr[j] + 1, prev[j + 1] + 1, prev[j] + (c1 != c2)))
        prev = curr
    return prev[-1]


def _fuzzy_title_match(query: str, title: str, threshold: float = 0.62) -> bool:
    """
    Devuelve True si la query coincide aproximadamente con el título.
    Tolera errores de escritura, acentos distintos y palabras en diferente orden.

    Ejemplos:
    - "la era de yelo"  → "La Era del Hielo"   ✅
    - "ice age"         → "La Era del Hielo"   ✅ (palabras en inglés)
    - "hary poter"      → "Harry Potter"       ✅
    """
    q = normalize_title(query or "")
    t = normalize_title(title or "")
    if not q or not t:
        return False

    # 1. Coincidencia exacta por substring (sin acentos)
    if q in t or t in q:
        return True

    # 2. Comparación palabra a palabra con tolerancia a errores por word
    q_words = [w for w in q.split() if len(w) > 1]
    t_words = [w for w in t.split() if len(w) > 1]
    if not q_words or not t_words:
        return False

    matched = 0
    for qw in q_words:
        best_dist = min(
            (_levenshtein(qw, tw) for tw in t_words),
            default=999,
        )
        max_len = max(len(qw), max((len(tw) for tw in t_words), default=1))
        # Tolerancia: 1 error por cada 4 caracteres
        allowed = max(1, len(qw) // 4)
        if best_dist <= allowed:
            matched += 1

    if q_words and matched / len(q_words) >= threshold:
        return True

    # 3. Levenshtein global (strings cortos)
    if len(q) <= 30 and len(t) <= 30:
        dist = _levenshtein(q, t)
        max_len = max(len(q), len(t))
        if max_len > 0 and (1.0 - dist / max_len) >= threshold:
            return True

    return False


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
# 🔧 Recortar/redimensionar imagen a 500x750 con cover mode + mejora de calidad
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

        # ✅ MEJORA: Usar LANCZOS (mayor calidad) para escalar
        scale = max(target_w / src_w, target_h / src_h)
        new_w = max(int(src_w * scale), target_w)
        new_h = max(int(src_h * scale), target_h)

        img = img.resize((new_w, new_h), _PIL_Image.LANCZOS)

        # Recorte centrado a exactamente 500×750
        left = (new_w - target_w) // 2
        top  = (new_h - target_h) // 2
        img  = img.crop((left, top, left + target_w, top + target_h))

        # ✅ MEJORA: Aplicar siempre un pequeño aumento de brillo (+12 %)
        # para mejorar la visibilidad en la interfaz, independientemente
        # de si la imagen está oscura o no.
        try:
            from PIL import ImageStat, ImageEnhance
            stat = ImageStat.Stat(img)
            bands = stat.mean
            mean_brightness = sum(bands[:3]) / min(3, len(bands))

            # Boost suave siempre activo: +12 % de brillo base
            BASE_BRIGHTNESS_FACTOR = 1.12
            img = ImageEnhance.Brightness(img).enhance(BASE_BRIGHTNESS_FACTOR)

            # Boost adicional si la imagen sigue siendo muy oscura tras el boost base
            if mean_brightness * BASE_BRIGHTNESS_FACTOR < 60:
                extra_factor = min(2.2, 70.0 / max(mean_brightness * BASE_BRIGHTNESS_FACTOR, 1.0))
                img = ImageEnhance.Brightness(img).enhance(extra_factor)
                print(f"   🌟 Brillo extra aplicado: media={mean_brightness:.1f} extra={extra_factor:.2f}")
            else:
                print(f"   ✨ Brillo base +12% aplicado: media={mean_brightness:.1f}")
        except Exception:
            pass

        # ✅ MEJORA: Calidad 92 (antes 88) para reducir artefactos de compresión
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=92, optimize=True)
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
# ✅ HELPER: Obtener el mejor thumb nativo de Telegram de forma robusta
# ---------------------------------------------------------------------------
def _get_best_native_thumb(thumbs_list):
    """
    Dado una lista de thumbs de Telegram (PhotoSize, PhotoCachedSize,
    PhotoStrippedSize, VideoSize, etc.), devuelve el mejor objeto thumb
    para descargar, priorizando siempre por MAYOR resolución (w × h).

    ✅ MEJORA: Selecciona únicamente la versión de mayor tamaño disponible,
    descartando previews comprimidos y thumbnails de baja resolución.
    Retorna el objeto thumb seleccionado, o None si la lista está vacía.
    """
    if not thumbs_list:
        return None

    # Importar tipos de Telethon de forma segura
    try:
        from telethon.tl.types import (
            PhotoStrippedSize,
            PhotoPathSize,
            PhotoCachedSize,
            PhotoSize as _TLPhotoSize,
        )
        _stripped_types = (PhotoStrippedSize, PhotoPathSize)
        _downloadable_types = (_TLPhotoSize, PhotoCachedSize)
    except ImportError:
        _stripped_types = ()
        _downloadable_types = ()

    # --- Paso 1: filtrar tipos que no se pueden descargar como imagen completa ---
    valid = []
    for t in thumbs_list:
        if _stripped_types and isinstance(t, _stripped_types):
            continue
        type_attr = getattr(t, "type", "") or ""
        if type_attr in ("i", "p"):
            continue
        valid.append(t)

    if not valid:
        # Último recurso: devolver el último elemento de la lista original
        return thumbs_list[-1] if thumbs_list else None

    # --- Paso 2: ordenar por resolución descendente (w × h) ---
    # Se prioriza w × h; si no tiene dimensiones, se usa file_size como proxy.
    def _thumb_score(t) -> int:
        w = getattr(t, "w", 0) or 0
        h = getattr(t, "h", 0) or 0
        if w > 0 and h > 0:
            return w * h
        # Fallback: usar tamaño de archivo como indicador de calidad
        return getattr(t, "size", 0) or getattr(t, "file_size", 0) or 0

    try:
        valid.sort(key=_thumb_score, reverse=True)
    except Exception:
        pass

    # ✅ MEJORA: Preferir explícitamente PhotoSize/PhotoCachedSize descargables
    # sobre VideoSize u otros tipos cuando tienen resolución similar.
    if _downloadable_types:
        for t in valid:
            if isinstance(t, _downloadable_types):
                return t

    return valid[0]


# ---------------------------------------------------------------------------
# ✅ MEJORA: Limpiar descripción (eliminar links, @menciones, limitar longitud)
# ---------------------------------------------------------------------------
def _clean_description(text: str, max_len: int = 300) -> str:
    """
    Limpia el texto de descripción de un mensaje de Telegram:
    - Elimina links (http/https)
    - Elimina menciones (@usuario, @canal)
    - Elimina markdown básico
    - Limita la longitud al máximo indicado
    Retorna el texto limpiado, o cadena vacía si no queda nada.
    """
    if not text:
        return ""
    t = text.strip()
    # Eliminar links markdown: [texto](url)
    t = re.sub(r'\[([^\]]*)\]\(https?://[^\)]*\)', r'', t)
    # Eliminar URLs sueltas
    t = re.sub(r'https?://\S+', '', t)
    # Eliminar menciones @usuario
    t = re.sub(r'@\w+', '', t)
    # Eliminar markdown residual
    t = re.sub(r'[*_`#]+', '', t)
    # Colapsar espacios y saltos de línea múltiples
    t = re.sub(r'\n{3,}', '\n\n', t)
    t = re.sub(r'[ \t]+', ' ', t)
    t = t.strip()
    # Limitar longitud
    if len(t) > max_len:
        # Cortar en el último espacio antes del límite
        cut = t[:max_len].rsplit(' ', 1)[0]
        t = cut.rstrip('.,;:') + '…'
    return t


# ---------------------------------------------------------------------------
# ✅ MEJORA: Buscar póster en mensajes cercanos al video
# ---------------------------------------------------------------------------
async def _find_poster_in_nearby_messages(
    entity,
    message_id: int,
    tg_client,
    search_range: int = 2,    # ✅ FIX 6: Reducido de 4 a 2 para evitar mezclar posters de películas distintas
) -> bytes | None:
    """
    Busca una imagen (póster) en los mensajes CERCANOS al video.
    Muchos canales publican el póster como foto justo antes del video.
    ✅ FIX 6: Rango reducido a ±2 mensajes (antes era ±4) para evitar cruce de miniaturas.
    Solo se usa cuando no hay miniatura nativa incrustada en el archivo de video.
    """
    try:
        # Obtener el caption del video original para validación de coherencia
        _video_caption_raw = ""
        try:
            _vm = await asyncio.wait_for(tg_client.get_messages(entity, ids=[message_id]), timeout=2.0)
            if isinstance(_vm, list):
                _vm = _vm[0] if _vm else None
            _video_caption_raw = (getattr(_vm, "text", None) or "").strip()[:120] if _vm else ""
        except Exception:
            pass
        _video_caption_norm = normalize_title(_video_caption_raw)

        # Obtener IDs de mensajes cercanos (solo ANTES del video, no después)
        # Así evitamos confundir el poster de la siguiente película con la actual
        nearby_ids = list(range(
            max(1, message_id - search_range),
            message_id,  # Solo mensajes ANTERIORES al video
        ))

        if not nearby_ids:
            return None

        messages = await asyncio.wait_for(
            tg_client.get_messages(entity, ids=nearby_ids),
            timeout=3.0,
        )

        if not messages:
            return None

        # Normalizar a lista
        if not isinstance(messages, list):
            messages = [messages]

        # Priorizar mensajes más cercanos al ID original
        messages_with_id = []
        for msg in messages:
            if msg is None:
                continue
            msg_id = getattr(msg, "id", None)
            if msg_id is None:
                continue
            distance = abs(msg_id - message_id)
            messages_with_id.append((distance, msg))

        messages_with_id.sort(key=lambda x: x[0])

        for _, msg in messages_with_id:
            if hasattr(msg, "photo") and msg.photo:
                # ✅ FIX 6: Validar que el mensaje cercano tenga texto relacionado con el video
                # Si el caption del mensaje cercano es completamente diferente, no usar el poster
                _nearby_caption = normalize_title((getattr(msg, "text", None) or "").strip()[:120])
                if _video_caption_norm and _nearby_caption:
                    # Verificar si comparten al menos una palabra clave significativa (>3 chars)
                    _video_words = {w for w in _video_caption_norm.split() if len(w) > 3}
                    _nearby_words = {w for w in _nearby_caption.split() if len(w) > 3}
                    # Si ambos tienen palabras clave pero NO comparten ninguna → descartar
                    if _video_words and _nearby_words and not _video_words.intersection(_nearby_words):
                        print(f"   🚫 Póster cercano #{msg.id} descartado (captions no coinciden)")
                        continue
                try:
                    photo_data = await asyncio.wait_for(
                        tg_client.download_media(msg.photo, bytes),
                        timeout=3.0,
                    )
                    if photo_data and len(photo_data) > 500:
                        print(f"   🖼️  Póster cercano #{msg.id} (a {abs(msg.id - message_id)} msgs)")
                        return photo_data
                except Exception:
                    continue

        return None

    except asyncio.TimeoutError:
        return None
    except Exception as e:
        print(f"   ⚠️  Error buscando póster en mensajes cercanos: {e}")
        return None




# ---------------------------------------------------------------------------
# ✅ FORZAR RESOLUCIÓN DE CANALES — evita "Invalid channel object"
# ---------------------------------------------------------------------------
async def force_resolve_channels(tg_client: TelegramClient) -> None:
    """
    Fuerza a Telethon a resolver (get_entity) todos los canales de la lista
    antes de que se intenten usar para streaming o búsqueda.
    Esto carga los IDs reales en la caché interna de Telethon y evita
    el error "Invalid channel object" en sesiones recién iniciadas.
    ✅ Sin canal principal — todos los canales tienen igual prioridad.
    """
    channels_to_resolve = list(BACKUP_CHANNELS)
    sem = asyncio.Semaphore(3)
    ok = 0
    fail = 0

    async def _resolve_one(ch: str):
        nonlocal ok, fail
        async with sem:
            try:
                await tg_client.get_entity(ch)
                ok += 1
            except FloodWaitError as _fw:
                wait_s = getattr(_fw, "seconds", 10)
                print(f"   ⏳ FloodWait resolviendo canal {ch}: esperando {wait_s}s...")
                await asyncio.sleep(wait_s + 2)
                try:
                    await tg_client.get_entity(ch)
                    ok += 1
                except Exception as _ex2:
                    fail += 1
                    print(f"   ⚠️  No se pudo resolver canal {ch} tras espera: {_ex2}")
            except Exception as ex:
                fail += 1
                print(f"   ⚠️  No se pudo resolver canal {ch}: {ex}")

    await asyncio.gather(*[_resolve_one(ch) for ch in channels_to_resolve], return_exceptions=True)
    print(f"   📡 Resolución de canales: {ok} OK / {fail} fallidos")


# ---------------------------------------------------------------------------
# ✅ SEPARACIÓN C: TAREA DE FONDO — ACTUALIZACIÓN DEL CATÁLOGO
# Se ejecuta de forma independiente al usuario. Actualiza la BD SQLite
# cada CATALOG_BACKGROUND_INTERVAL segundos. El endpoint /catalog lee
# directamente de SQLite sin tocar Telegram en tiempo real.
# ---------------------------------------------------------------------------
async def _catalog_background_updater():
    """
    Proceso de fondo que actualiza el pool de catálogo periódicamente.
    - Espera a que los canales estén disponibles antes de la primera ejecución.
    - Agrega un delay entre canales y entre cambios de cuenta para evitar
      que Telegram detecte ráfagas de solicitudes (mejora B incluida aquí).
    - Guarda los resultados en SQLite; /catalog los lee sin consultar Telegram.
    """
    print(f"🔄 [Catálogo BG] Tarea iniciada. Intervalo: {CATALOG_BACKGROUND_INTERVAL}s")

    # Esperar a que los canales estén listos antes de la primera actualización
    _waited = 0.0
    while not getattr(app.state, "channels_ready", False) and _waited < 60.0:
        await asyncio.sleep(1.0)
        _waited += 1.0

    while True:
        try:
            print("🔄 [Catálogo BG] Iniciando actualización de catálogo en background...")
            CATALOG_LIMIT_PER_CHANNEL = 15

            entities = getattr(app.state, "entities", [])
            entities_indexed = [(i, e) for i, e in enumerate(entities) if e is not None]

            if not entities_indexed:
                print("⚠️  [Catálogo BG] No hay canales disponibles aún, reintentando más tarde...")
                await asyncio.sleep(60)
                continue

            fetch_sem = asyncio.Semaphore(CATALOG_FETCH_CONCURRENCY)
            pool: list = []
            seen_keys: set = set()

            for ch_index, entity in entities_indexed:
                try:
                    async with fetch_sem:
                        # ✅ B: Delay entre canales para evitar ráfagas
                        await asyncio.sleep(ACCOUNT_SWITCH_DELAY)

                        _active_client = _get_next_client()
                        random_offset = random.randint(0, 100)

                        # ✅ FIX: Resolver la entidad con el cliente activo antes de iterar.
                        # Cada cliente de Telegram tiene su propia sesión y necesita
                        # resolver el access_hash del canal para poder hacer peticiones.
                        # Sin esto, cuando el cliente rotado no tiene el canal en caché,
                        # Telethon lanza "Invalid channel object (caused by GetHistoryRequest)".
                        _resolved_entity = entity
                        try:
                            _channel_id = getattr(entity, "id", None) or entity
                            _resolved_entity = await _active_client.get_entity(_channel_id)
                        except Exception as _re:
                            # Si no se puede resolver, intentar con los demás clientes
                            for _cl_fb in _telegram_clients:
                                if _cl_fb is _active_client:
                                    continue
                                try:
                                    _channel_id = getattr(entity, "id", None) or entity
                                    _resolved_entity = await _cl_fb.get_entity(_channel_id)
                                    _active_client = _cl_fb
                                    break
                                except Exception:
                                    continue

                        results_ch: list = []
                        try:
                            async for message in _active_client.iter_messages(
                                _resolved_entity,
                                limit=CATALOG_LIMIT_PER_CHANNEL,
                                add_offset=random_offset,
                            ):
                                if message.media and (message.video or message.document):
                                    caption     = message.text or ""
                                    title       = _extract_title_from_caption(caption)
                                    description = _clean_description(caption, max_len=300)
                                    direct_link = (
                                        f"{PUBLIC_URL}/stream/{message.id}?ch={ch_index}"
                                        if PUBLIC_URL else f"/stream/{message.id}?ch={ch_index}"
                                    )
                                    results_ch.append({
                                        "id":          message.id,
                                        "title":       title,
                                        "description": description,
                                        "size":        (
                                            f"{round(message.file.size / (1024 * 1024), 2)} MB"
                                            if message.file else "n/a"
                                        ),
                                        "stream_url":  direct_link,
                                    })
                                    if len(results_ch) >= CATALOG_LIMIT_PER_CHANNEL:
                                        break
                        except FloodWaitError as _fw:
                            wait_s = getattr(_fw, "seconds", 30)
                            print(f"   ⏳ [Catálogo BG] FloodWait ch={ch_index}: esperando {wait_s}s...")
                            await asyncio.sleep(wait_s + 2)
                        except Exception as _ie:
                            print(f"   ⚠️  [Catálogo BG] Error en ch={ch_index}: {_ie}")

                        for r in results_ch:
                            key = normalize_title(r.get("title", ""))
                            if key not in seen_keys:
                                seen_keys.add(key)
                                pool.append(r)

                except Exception as _ce:
                    print(f"⚠️  [Catálogo BG] Error procesando canal {ch_index}: {_ce}")

            if pool:
                random.shuffle(pool)
                # Guardar en SQLite y también en memoria (pool_cache)
                await asyncio.to_thread(_db_catalog_set, pool)
                app.state.catalog_pool_cache = {"ts": time.monotonic(), "pool": list(pool)}
                print(f"✅ [Catálogo BG] Catálogo actualizado: {len(pool)} items en SQLite")
            else:
                print("⚠️  [Catálogo BG] Pool vacío, no se actualiza SQLite")

        except Exception as ex:
            print(f"❌ [Catálogo BG] Error inesperado: {ex}")

        # Esperar hasta la próxima actualización
        print(f"⏱️  [Catálogo BG] Próxima actualización en {CATALOG_BACKGROUND_INTERVAL}s")
        await asyncio.sleep(CATALOG_BACKGROUND_INTERVAL)


# ---------------------------------------------------------------------------
# LIFESPAN
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("📡 Conectando a Telegram...")
    # ✅ MULTI-CUENTA: Conectar todos los clientes disponibles de forma segura
    _clients_connected: list = []
    for _i, _cl in enumerate(_telegram_clients):
        try:
            await _cl.connect()
            if await _cl.is_user_authorized():
                _clients_connected.append(_cl)
                print(f"✅ Cliente {_i + 1} conectado y autorizado")
                # Forzar resolución de canales para este cliente
                print(f"   🔍 Resolviendo canales para cliente {_i + 1}...")
                await force_resolve_channels(_cl)
            else:
                print(f"⚠️  Cliente {_i + 1} conectado pero NO autorizado — se omite")
        except Exception as _e:
            print(f"⚠️  Error conectando cliente {_i + 1}: {_e}")

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

    app.state.ai_cache      = {}
    app.state.ai_cache_lock = asyncio.Lock()
    app.state.ai_sem        = asyncio.Semaphore(AI_SEM_LIMIT)

    app.state.last_persist_save_ts = 0.0

    # ✅ Inicializar caché de búsquedas SQLite (anti-flood)
    _db_init_search_cache()
    # ✅ SEPARACIÓN C: Inicializar caché de catálogo SQLite
    _db_init_catalog_cache()

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
    print(f"⚙️  IA semáforo: max={AI_SEM_LIMIT}")
    print(f"🔄 Cuentas Telegram activas: {len(_telegram_clients)}")
    print(f"✅ Sin canal principal — todos los canales tienen igual prioridad (pool plano)")

    async def _load_backup_channels():
        sem = asyncio.Semaphore(5)
        _total_cl = len(_telegram_clients)

        def _entity_name(entity) -> str:
            """Nombre seguro de entidad: funciona para canal, grupo, usuario."""
            return (
                getattr(entity, "title",      None) or
                getattr(entity, "username",   None) or
                (f"{getattr(entity,'first_name','') or ''} "
                 f"{getattr(entity,'last_name','') or ''}").strip() or
                str(getattr(entity, "id", "?"))
            )

        async def _load_one(ch_item, idx: int):
            async with sem:
                # ✅ Distribuir carga en round-robin entre TODOS los clientes disponibles
                _cl_primary = _telegram_clients[idx % _total_cl] if _total_cl > 0 else client
                entity = None
                try:
                    entity = await _cl_primary.get_entity(ch_item)
                    print(f"✅ Canal cargado [{idx % _total_cl + 1}]: {_entity_name(entity)}")
                except Exception as ex:
                    # Fallback: intentar con los demás clientes
                    for _cl_fb in _telegram_clients:
                        if _cl_fb is _cl_primary:
                            continue
                        try:
                            entity = await _cl_fb.get_entity(ch_item)
                            print(f"✅ Canal cargado (fallback): {_entity_name(entity)}")
                            break
                        except Exception:
                            continue
                    if entity is None:
                        print(f"⚠️  No se pudo cargar canal {ch_item}: {ex}")
                        return None

                # ✅ Asegurar que TODOS los demás clientes resuelvan este canal
                # para que sus sesiones tengan el access_hash en caché y evitar
                # "Invalid channel object" durante búsquedas paralelas.
                _ch_ref = getattr(entity, "id", None) or ch_item
                for _cl_other in _telegram_clients:
                    if _cl_other is _cl_primary:
                        continue
                    try:
                        await _cl_other.get_entity(_ch_ref)
                    except Exception:
                        pass  # best-effort; el acceso real fallará más tarde si no se pudo
                return entity

        backup_entities = await asyncio.gather(
            *[_load_one(ch, i) for i, ch in enumerate(BACKUP_CHANNELS)]
        )
        # ✅ Sin canal principal — el pool es plano: todos los canales tienen igual peso
        valid_entities    = [e for e in backup_entities if e is not None]
        app.state.entity  = valid_entities[0] if valid_entities else None
        app.state.entities = list(backup_entities)
        app.state.channels_ready = True
        loaded_n = sum(1 for e in app.state.entities if e is not None)
        print(f"✅ Todos los canales cargados: {loaded_n} disponibles (sin canal principal)")

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

        # ✅ SEPARACIÓN C: Iniciar tarea de fondo para actualizar catálogo
        asyncio.create_task(_catalog_background_updater())

    asyncio.create_task(_load_backup_channels())

    yield

    if getattr(app.state, "meta_cache_dirty", False):
        print("💾 Guardando caché pendiente antes de apagar...")
        to_save = dict(getattr(app.state, "meta_cache", {}) or {})
        to_save[AI_CACHE_KEY] = dict(getattr(app.state, "ai_cache", {}) or {})
        await _save_persistent_cache(to_save)

    # ✅ MULTI-CUENTA: Desconectar todos los clientes al apagar
    for _cl in _telegram_clients:
        try:
            await _cl.disconnect()
        except Exception:
            pass


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# ✅ MULTI-CUENTA: Inicialización segura de clientes Telegram
# ---------------------------------------------------------------------------
_telegram_clients: list = []

try:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    _telegram_clients.append(client)
    print(f"✅ Cliente principal (cuenta 1) inicializado con API_ID={API_ID}")
except Exception as _e1:
    print(f"❌ Error crítico inicializando cliente principal: {_e1}")
    # Crear cliente vacío para evitar NameError en el resto del código
    client = TelegramClient(StringSession(""), API_ID, API_HASH)

# Segunda cuenta — solo si tiene sus 3 credenciales propias
if SESSION_STRING_2.strip() and API_ID_2 and API_HASH_2.strip():
    try:
        _client2 = TelegramClient(StringSession(SESSION_STRING_2), API_ID_2, API_HASH_2)
        _telegram_clients.append(_client2)
        print(f"✅ Segunda cuenta Telegram configurada correctamente con API_ID_2={API_ID_2}")
        print(f"🔄 Multi-cuenta activada: {len(_telegram_clients)} cuentas de Telegram disponibles")
    except Exception as _e2:
        print(f"⚠️  No se pudo inicializar cliente 2, se ignora: {_e2}")
else:
    _missing = []
    if not SESSION_STRING_2.strip(): _missing.append("SESSION_STRING_2")
    if not API_ID_2:                 _missing.append("API_ID_2")
    if not API_HASH_2.strip():       _missing.append("API_HASH_2")
    if _missing:
        print(f"ℹ️  Segunda cuenta no configurada (faltan: {', '.join(_missing)}). Usando cuenta única.")

# Tercera cuenta — solo si tiene sus 3 credenciales propias
if SESSION_STRING_3.strip() and API_ID_3 and API_HASH_3.strip():
    try:
        _client3 = TelegramClient(StringSession(SESSION_STRING_3), API_ID_3, API_HASH_3)
        _telegram_clients.append(_client3)
        print(f"✅ Tercera cuenta Telegram configurada correctamente con API_ID_3={API_ID_3}")
        print(f"🔄 Multi-cuenta activada: {len(_telegram_clients)} cuentas de Telegram disponibles")
    except Exception as _e3:
        print(f"⚠️  No se pudo inicializar cliente 3, se ignora: {_e3}")
else:
    _missing3 = []
    if not SESSION_STRING_3.strip(): _missing3.append("SESSION_STRING_3")
    if not API_ID_3:                 _missing3.append("API_ID_3")
    if not API_HASH_3.strip():       _missing3.append("API_HASH_3")
    if _missing3:
        print(f"ℹ️  Tercera cuenta no configurada (faltan: {', '.join(_missing3)}). Usando cuentas disponibles.")

# Índice global para rotación Round Robin
_client_rr_index: dict = {"idx": 0}


async def get_active_client() -> TelegramClient | None:
    """
    Retorna un cliente Telegram activo y conectado usando rotación Round Robin.
    Si el cliente seleccionado está desconectado, intenta reconectarlo
    automáticamente antes de retornarlo. Prueba todos los clientes disponibles.
    """
    if not _telegram_clients:
        return None

    total = len(_telegram_clients)
    for _ in range(total):
        idx      = _client_rr_index["idx"]
        selected = _telegram_clients[idx % total]
        _client_rr_index["idx"] = (idx + 1) % total

        try:
            if selected.is_connected():
                # ✅ ANTI-FLOOD B: pequeño delay al rotar entre cuentas
                if total > 1:
                    await asyncio.sleep(ACCOUNT_SWITCH_DELAY)
                return selected
            # Cliente desconectado → intentar reconectar
            print(f"🔄 Cliente {idx} desconectado — reconectando...")
            await selected.connect()
            if await selected.is_user_authorized():
                print(f"✅ Cliente {idx} reconectado correctamente")
                return selected
        except Exception as _ce:
            print(f"⚠️  No se pudo reconectar cliente {idx}: {_ce}")
            continue

    print("❌ Ningún cliente de Telegram disponible")
    return None


def _get_next_client() -> TelegramClient:
    """
    Versión síncrona de rotación (usada en contextos no-async).
    No verifica conexión — usar get_active_client() cuando sea posible.
    """
    if len(_telegram_clients) == 1:
        return _telegram_clients[0]
    idx = _client_rr_index["idx"]
    selected = _telegram_clients[idx % len(_telegram_clients)]
    _client_rr_index["idx"] = (idx + 1) % len(_telegram_clients)
    return selected


# ---------------------------------------------------------------------------
# HEALTH CHECK
# ---------------------------------------------------------------------------
@app.get("/health")
async def health_check():
    channels_up       = sum(1 for e in getattr(app.state, "entities", []) if e is not None)
    return JSONResponse({
        "status":               "ok",
        "channels_ready":       getattr(app.state, "channels_ready", False),
        "channels_loaded":      channels_up,
        "cache_entries":        len(getattr(app.state, "meta_cache", {})),
        "thumb_cache_entries":  len(getattr(app.state, "thumb_cache", {})),
        "internal_port":        INTERNAL_PORT,
        "telegram_accounts":    len(_telegram_clients),
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

    # 1.5. Eliminar menciones de canales/usuarios (@canal) y extensiones de video
    t = re.sub(r'@\w+', '', t)
    t = re.sub(
        r'\.(mp4|mkv|avi|mov|wmv|flv|webm|m4v|ts|3gp|mpeg|mpg)(\b|$)',
        '', t, flags=re.IGNORECASE
    )

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
    # ✅ MEJORA: Aplicar limpieza final de símbolos antes de enviar a APIs
    clean = _sanitize_title_for_api_query(clean)
    return clean or title


# ---------------------------------------------------------------------------
# ✅ NUEVA FUNCIÓN: Limpieza profunda de título antes de consultar APIs externas
# Elimina emojis, símbolos, abreviaturas y texto decorativo que confunde a
# TMDB, Google Knowledge Graph, TVMaze y Gemini.
#
# Ejemplos de entrada → salida:
#   "### EL EVANGELIO SEGÚN MATEO - LA VIDA DE JESÚS"  → "EL EVANGELIO SEGÚN MATEO"
#   "El gato con botas: El último deseo LAT 🎦"         → "El gato con botas El último deseo"
#   "RAMBO IV 🎬 FULL HD 1080p LATINO @canal"           → "RAMBO IV"
#   "Spider-Man: No Way Home (2021) 4K HDR ESP"         → "Spider-Man No Way Home"
# ---------------------------------------------------------------------------
def _sanitize_title_for_api_query(title: str) -> str:
    """
    Limpieza profunda del título de cara a consultas externas (TMDB, KG, TVMaze, Gemini).
    Opera sobre un título YA limpiado por _advanced_clean_title / _clean_title_for_api.

    Pasos:
    1. Eliminar todos los emojis y símbolos Unicode decorativos.
    2. Eliminar prefijos de sección tipo ### o ==.
    3. Eliminar abreviaturas de idioma/calidad sueltas al final o inicio.
    4. Eliminar los caracteres especiales: # ! @ $ + / \\ % ^ & * " ' ; , = < > ~ ` |
    5. Eliminar guiones que queden sueltos (no los del interior de palabras).
    6. Colapsar espacios múltiples.
    """
    if not title:
        return title

    t = title.strip()

    # --- 1. Eliminar emojis y símbolos Unicode decorativos ---
    # Categorías Unicode que corresponden a símbolos/emojis:
    # So = Other Symbol, Sm = Math Symbol, Sk = Modifier Symbol, Sc = Currency Symbol
    # Cn = Unassigned (incluye bloques emoji), Cs = Surrogate
    def _remove_emoji(s: str) -> str:
        result = []
        for ch in s:
            cp = ord(ch)
            # Bloque emoji principal: U+1F000–U+1FFFF
            if 0x1F000 <= cp <= 0x1FFFF:
                result.append(' ')
                continue
            # Símbolos misceláneos y flechas: U+2600–U+27FF
            if 0x2600 <= cp <= 0x27FF:
                result.append(' ')
                continue
            # Suplemento de símbolos y pictogramas: U+1F900–U+1F9FF (ya cubierto arriba)
            # Variantes de texto/emoji: U+FE00–U+FEFF
            if 0xFE00 <= cp <= 0xFEFF:
                continue  # Eliminar silenciosamente
            # Caracteres de control y formato: U+0000–U+001F, U+007F–U+009F
            if cp <= 0x1F or 0x7F <= cp <= 0x9F:
                continue
            cat = unicodedata.category(ch)
            # So (Other Symbol) cubre la mayoría de emojis en BMP
            if cat in ('So', 'Mn') and cp > 0x2FF:
                result.append(' ')
                continue
            result.append(ch)
        return ''.join(result)

    t = _remove_emoji(t)

    # --- 2. Eliminar prefijos de sección markdown: ###, ##, #, ===, --- ---
    t = re.sub(r'^[#=\-]{1,6}\s*', '', t).strip()

    # --- 3. Eliminar abreviaturas de idioma/calidad sueltas (al inicio o al final) ---
    # Estos tokens no aportan al nombre real de la película
    _QUALITY_LANG_TOKENS = (
        r'\bLAT\b', r'\bESP\b', r'\bENG\b', r'\bSPA\b', r'\bSUB\b',
        r'\bDUB\b', r'\bHDR\b', r'\bSDR\b', r'\bUHD\b', r'\bHEVC\b',
        r'\bAVC\b', r'\bAAC\b', r'\bAC3\b', r'\bDDP\b', r'\bDTS\b',
        r'\bH264\b', r'\bH265\b', r'\bX264\b', r'\bX265\b',
        r'\bHD\b', r'\b4K\b', r'\b2K\b', r'\b8K\b',
        r'\b1080[pi]?\b', r'\b720[pi]?\b', r'\b480[pi]?\b',
        r'\bFULL\s*HD\b', r'\bBLU\s*RAY\b', r'\bBDRIP\b',
        r'\bWEB\s*DL\b', r'\bWEB\s*RIP\b', r'\bHDRIP\b',
        r'\bCAM\b', r'\bTS\b', r'\bDVDRIP\b',
        r'\bLATINO\b', r'\bCASTELLANO\b', r'\bDOBLADO\b', r'\bDOBLAJE\b',
        r'\bSUBTITULADO\b', r'\bSUBTITULOS?\b',
        r'\bCOMPLETO\b', r'\bCOMPLETA\b', r'\bGRATIS\b', r'\bONLINE\b',
        r'\bTRAILER\b', r'\bTEASER\b', r'\bOFFICIAL\b', r'\bOFICIAL\b',
        r'\bFULL\b', r'\bDUAL\b', r'\bREMASTER(?:ED|IZADO)?\b',
        r'\bEXTENDED\b', r'\bUNRATED\b', r'\bSCREENER\b',
    )
    for _pat in _QUALITY_LANG_TOKENS:
        t = re.sub(_pat, ' ', t, flags=re.IGNORECASE)

    # --- 4. Eliminar caracteres especiales que no forman parte de títulos ---
    # Se conservan: letras, dígitos, espacios, guión normal, punto, dos puntos,
    # paréntesis (ya limpiados antes) y letras acentuadas/especiales del español.
    # Se eliminan: # ! @ $ + / \ % ^ & * " ; , = < > ~ ` | ¡ ¿ [ ]
    t = re.sub(r'[#!@$+/%^&*";,=<>~`|¡¿\\\[\]]', ' ', t)

    # --- 5. Normalizar guiones: eliminar guiones sueltos (no entre letras) ---
    # Eliminar guiones al inicio/final de palabra o rodeados de espacios
    t = re.sub(r'(?<![A-Za-zÀ-ÿ\d])-|-(?![A-Za-zÀ-ÿ\d])', ' ', t)

    # --- 6. Colapsar espacios múltiples y limpiar bordes ---
    t = re.sub(r'\s+', ' ', t).strip()
    # Eliminar guión o punto solitario que pudiera quedar al inicio/final
    t = re.sub(r'^[\s.\-]+|[\s.\-]+$', '', t).strip()

    return t or title


def _build_tmdb_query_from_title(title: str):
    """
    Versión mejorada: usa limpieza avanzada + sanitización profunda para APIs.
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

    # ✅ MEJORA: Sanitización profunda — elimina emojis, símbolos y abreviaturas
    # que confunden a TMDB/KG/TVMaze/Gemini antes de enviarles la consulta.
    q = _sanitize_title_for_api_query(q)

    # Fallback al método original si el título quedó muy corto
    if len(q.strip()) < 3:
        raw  = _strip_decorations(title)
        year_fallback = _extract_year_advanced(raw)
        q    = _clean_title_for_api(raw)
        q = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", q).strip()
        q = _sanitize_title_for_api_query(q)
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
    # ✅ MULTI-CUENTA: Usar cliente rotativo para repartir carga entre cuentas
    _active_client = _get_next_client()
    # ✅ ANTI-FLOOD: Pausa aumentada para evitar que Telegram detecte flood
    await asyncio.sleep(SEARCH_INTER_CHANNEL_DELAY)
    # ✅ Forzar resolución del canal para evitar "Invalid channel object"
    try:
        _channel_ref = getattr(entity, "id", None) or entity
        entity = await _active_client.get_entity(_channel_ref)
    except FloodWaitError as _fw:
        wait_s = getattr(_fw, "seconds", 15)
        print(f"   ⏳ FloodWait resolviendo entidad ch={ch_index}: esperando {wait_s}s...")
        await asyncio.sleep(wait_s + 2)
        try:
            entity = await _active_client.get_entity(_channel_ref)
        except Exception as _ex2:
            print(f"   ⚠️  No se pudo re-resolver entidad ch={ch_index} tras espera: {_ex2}")
            return []
    except Exception as _re:
        print(f"   ⚠️  No se pudo re-resolver entidad en fetch_recent ch={ch_index}: {_re}")
    try:
        async for message in _active_client.iter_messages(entity, limit=limit):
            if message.media and (message.video or message.document):
                caption     = message.text or ""
                title       = _extract_title_from_caption(caption)
                # ✅ MEJORA: Limpiar descripción (eliminar links, menciones, limitar longitud)
                description = _clean_description(caption, max_len=300)
                direct_link = (
                    f"{PUBLIC_URL}/stream/{message.id}?ch={ch_index}"
                    if PUBLIC_URL else f"/stream/{message.id}?ch={ch_index}"
                )
                results.append({
                    "id":          message.id,
                    "title":       title,
                    "description": description,
                    "size":        (
                        f"{round(message.file.size / (1024 * 1024), 2)} MB"
                        if message.file else "n/a"
                    ),
                    "stream_url":  direct_link,
                })
                if len(results) >= 50:
                    break
    except FloodWaitError as _fw:
        wait_s = getattr(_fw, "seconds", 30)
        print(f"   ⏳ FloodWait en iter_messages ch={ch_index}: esperando {wait_s}s...")
        await asyncio.sleep(wait_s + 2)
        # Retornar lo que se recopiló antes del flood (puede ser lista vacía)
    except Exception as _ie:
        print(f"   ⚠️  Error en iter_messages ch={ch_index}: {_ie}")
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

    # Lista de palabras clave de género para parseo de queries combinadas
    _GENRE_KEYWORDS_PARSE = [
        "ciencia ficcion", "ciencia ficción", "drama coreano", "k-drama",
        "animacion", "animación", "anime", "terror", "horror", "miedo",
        "cristiana", "cristiano", "religion", "religión", "infantil",
        "niños", "ninos", "kids", "familia", "clasica", "clásica",
        "vintage", "antigua", "adultos", "adulto", "accion", "acción",
        "aventura", "drama", "comedia", "romance", "romantica", "romántica",
        "sci-fi", "scifi", "ficcion", "ficción", "suspenso", "thriller",
        "documental", "documentary", "documentales", "lgbt", "lgbtq", "pride",
        "musical", "western", "fantasia", "fantasía", "fantastica", "fantástica",
        "policial", "crimen", "criminal", "guerra", "historia", "historica",
        "histórica", "misterio", "mystery", "kdrama", "deportes", "sports",
        "futbol", "fútbol", "general",
    ]

    # Ordenar géneros por longitud descendente (para detectar primero los más específicos)
    genre_keys_sorted = sorted(_GENRE_KEYWORDS_PARSE, key=len, reverse=True)

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
            "limit":     5,                              # ✅ Más resultados para mejor match
            "indent":    "false",
            "types":     ["Movie", "TVSeries", "CreativeWork"],
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

        # ✅ Fallback: si la búsqueda con año no devuelve resultados, reintentar sin año
        if (not isinstance(items, list) or not items) and year:
            params_no_year = {**params, "query": query_title}
            async with getattr(app.state, "ai_sem", asyncio.Semaphore(1)):
                r2 = await http.get("https://kgsearch.googleapis.com/v1/entities:search", params=params_no_year)
            if r2.status_code == 200:
                data2  = r2.json()
                items  = _safe_get(data2, "itemListElement", []) or []

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
            f'Actúa como experto en cine. Identifica la película o serie: "{title}"{year_hint}.\n'
            f'Responde estrictamente en formato JSON con estas claves exactas:\n'
            f'{{\n'
            f'  "titulo_real": "Nombre oficial de la película/serie",\n'
            f'  "sinopsis": "Resumen breve en español (máx 180 palabras)",\n'
            f'  "año": "año de estreno como string de 4 dígitos (ej: \\"2019\\")",\n'
            f'  "poster_url": "URL de imagen de poster oficial si la conoces, o null",\n'
            f'  "generos": "géneros separados por coma (ej: Acción, Aventura)",\n'
            f'  "idioma_original": "código ISO 639-1 en mayúsculas (ES, EN, JA, KO, FR, etc.)",\n'
            f'  "duracion": "duración en formato \\"120 minutos\\" o null si es serie",\n'
            f'  "fecha_lanzamiento": "fecha en formato YYYY-MM-DD o null si no se conoce exactamente"\n'
            f'}}\n'
            f'Reglas:\n'
            f'- Si no la conoces, responde: {{"error": "not_found"}}\n'
            f'- Solo incluye campos que conoces con certeza; usa null para los desconocidos.\n'
            f'- Responde ÚNICAMENTE con el JSON válido, sin explicaciones, sin markdown, sin texto adicional.'
        )

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature":     0.1,
                "maxOutputTokens": 512,
            },
        }

        # ✅ Delay de 1.5s entre llamadas a Gemini para evitar error 429
        await asyncio.sleep(1.5)

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

        # Verificar si Gemini no encontró el título
        if result.get("error") == "not_found":
            await _ai_cache_set("gemini", ck, None, "none")
            return None

        # Normalizar: titulo_real → titulo, poster_url → imagen_url (si no hay imagen ya)
        if result.get("titulo_real") and not result.get("titulo"):
            result["titulo"] = result.pop("titulo_real")
        elif "titulo_real" in result:
            result.pop("titulo_real")

        if result.get("poster_url") and not result.get("imagen_url"):
            result["imagen_url"] = result.pop("poster_url")
        elif "poster_url" in result:
            result.pop("poster_url")

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
                            # ✅ También tomar titulo e imagen de Gemini si faltan
                            if not meta.get("titulo") and gemini_data.get("titulo"):
                                meta["titulo"] = gemini_data["titulo"]
                            if _is_placeholder_image(meta.get("imagen_url")) and gemini_data.get("imagen_url"):
                                meta["imagen_url"] = gemini_data["imagen_url"]

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

    # ✅ ANTI-FLOOD: Verificar caché SQLite antes de consultar Telegram.
    # Si la misma búsqueda ya fue hecha, se devuelve inmediatamente sin tocar Telegram.
    _search_cache_key = _db_search_cache_key(
        effective_text, effective_genre, effective_year,
        language, effective_desde, effective_hasta,
    )
    _cached_results = await asyncio.to_thread(_db_search_get, _search_cache_key)
    if _cached_results is not None:
        print(f"⚡ Caché SQLite hit → devolviendo {len(_cached_results)} resultado(s) sin consultar Telegram")
        return _cached_results

    try:
        if not getattr(app.state, "channels_ready", False):
            waited = 0.0
            while not getattr(app.state, "channels_ready", False) and waited < CHANNELS_READY_MAX_WAIT_SEARCH:
                await asyncio.sleep(0.3)
                waited += 0.3

        entities = getattr(app.state, "entities", [app.state.entity])
        all_entities_indexed = [(i, e) for i, e in enumerate(entities) if e is not None]

        # 🔧 Selección de canales: canal específico o todos los disponibles
        if canal:
            canal_clean = canal.strip().lstrip('@').lower()
            entities_indexed = [
                (i, e) for i, e in all_entities_indexed
                if (getattr(e, 'username', '') or '').lower() == canal_clean
            ]
            if not entities_indexed:
                entities_indexed = all_entities_indexed[:1]
        else:
            # ✅ Sin restricción de canales — se revisan TODOS los disponibles
            entities_indexed = all_entities_indexed

        # ✅ BÚSQUEDA PARALELA: asignar cada canal a una cuenta específica
        # para distribuir la carga y reducir el tiempo de respuesta a ~10s.
        num_accounts = len(_telegram_clients)

        async def search_in_channel(ch_index: int, entity, assigned_client) -> list:
            if entity is None:
                return []
            results = []

            # ✅ FIX 4: Helper seguro para nombre de entidad (canal, grupo, usuario, supergrupo)
            def _safe_entity_name(e) -> str:
                return (
                    getattr(e, "title",    None) or
                    getattr(e, "username", None) or
                    (f"{getattr(e,'first_name','') or ''} "
                     f"{getattr(e,'last_name','') or ''}").strip() or
                    str(getattr(e, "id", ch_index))
                )

            try:
                _active_client = assigned_client
                # ✅ ANTI-FLOOD: Pausa mínima entre consultas paralelas
                await asyncio.sleep(SEARCH_INTER_CHANNEL_DELAY * 0.3)
                # ✅ Forzar resolución del canal para evitar "Invalid channel object"
                try:
                    _channel_ref = getattr(entity, "id", None) or entity
                    entity = await _active_client.get_entity(_channel_ref)
                except FloodWaitError as _fw:
                    wait_s = getattr(_fw, "seconds", 15)
                    print(f"   ⏳ FloodWait resolviendo entidad search ch={ch_index}: esperando {wait_s}s...")
                    await asyncio.sleep(wait_s + 2)
                    try:
                        entity = await _active_client.get_entity(_channel_ref)
                    except Exception as _ex2:
                        print(f"   ⚠️  No se pudo re-resolver entidad search ch={ch_index} tras espera: {_ex2}")
                        return []
                except Exception as _re:
                    print(f"   ⚠️  No se pudo re-resolver entidad en search ch={ch_index}: {_re}")

                if effective_text:
                    # ✅ FIX 2: Normalizar el texto de búsqueda antes de enviarlo a Telegram
                    # Esto convierte "hera de hielo" → "era de hielo", elimina tildes, etc.
                    _text_normalized = normalize_title(effective_text).strip()

                    def _build_result(message) -> dict | None:
                        """Construye el dict de resultado a partir de un mensaje de Telegram."""
                        if not (message.media and (message.video or message.document)):
                            return None
                        caption     = message.text or ""
                        title       = _extract_title_from_caption(caption)
                        description = _clean_description(caption, max_len=300)
                        direct_link = (
                            f"{PUBLIC_URL}/stream/{message.id}?ch={ch_index}"
                            if PUBLIC_URL else f"/stream/{message.id}?ch={ch_index}"
                        )
                        return {
                            "id":          message.id,
                            "title":       title,
                            "description": description,
                            "size":        (
                                f"{round(message.file.size / (1024 * 1024), 2)} MB"
                                if message.file else "n/a"
                            ),
                            "stream_url":  direct_link,
                        }

                    try:
                        # ✅ FIX 2: Buscar con texto normalizado (sin tildes, minúsculas)
                        msg_iter = _active_client.iter_messages(entity, search=_text_normalized)
                        async for message in msg_iter:
                            r = _build_result(message)
                            if r:
                                results.append(r)
                    except FloodWaitError as _fw:
                        wait_s = getattr(_fw, "seconds", 30)
                        print(f"   ⏳ FloodWait en búsqueda ch={ch_index}: esperando {wait_s}s y reintentando...")
                        await asyncio.sleep(wait_s + 2)
                        # Retornar lo parcialmente recopilado antes del flood

                    # ✅ FIX 2: Si la búsqueda normalizada difiere de la original, buscar también con la original
                    if _text_normalized != effective_text.strip().lower():
                        try:
                            existing_ids = {r.get("id") for r in results}
                            async for message in _active_client.iter_messages(entity, search=effective_text.strip()):
                                if message.id not in existing_ids:
                                    r = _build_result(message)
                                    if r:
                                        results.append(r)
                                        existing_ids.add(message.id)
                        except Exception:
                            pass

                    # ✅ FIX 5: Buscar también con el nombre base de saga para encontrar
                    # TODAS las partes (ej: "era de hielo 1", "era de hielo 2", etc.)
                    _saga_base = _detect_saga_name(effective_text or "")
                    if _saga_base and _saga_base.strip() not in (_text_normalized, effective_text.strip().lower()):
                        try:
                            existing_ids = {r.get("id") for r in results}
                            async for message in _active_client.iter_messages(entity, search=_saga_base):
                                if message.id not in existing_ids:
                                    r = _build_result(message)
                                    if r:
                                        results.append(r)
                                        existing_ids.add(message.id)
                        except Exception:
                            pass

                    # ✅ FUZZY SEARCH: escanear caché reciente con distancia Levenshtein.
                    # Permite encontrar "La Era del Hielo" cuando el usuario escribe "la era de yelo".
                    try:
                        cached_media = await asyncio.wait_for(
                            _get_recent_media_cached(ch_index, entity),
                            timeout=SEARCH_CHANNEL_FETCH_TIMEOUT,
                        )
                        existing_ids = {r.get("id") for r in results}
                        for item in cached_media:
                            if _fuzzy_title_match(effective_text, item.get("title", "")):
                                if item.get("id") not in existing_ids:
                                    results.append(item)
                                    existing_ids.add(item.get("id"))
                            # ✅ FIX 5: también fuzzy-match con el texto normalizado
                            elif _text_normalized and _fuzzy_title_match(_text_normalized, item.get("title", "")):
                                if item.get("id") not in existing_ids:
                                    results.append(item)
                                    existing_ids.add(item.get("id"))
                    except (asyncio.TimeoutError, Exception):
                        pass  # fuzzy scan es best-effort

                else:
                    results = await _get_recent_media_cached(ch_index, entity)

                _ename = _safe_entity_name(entity)
                print(f"   📺 Canal [{ch_index}] ({_ename}): {len(results)} resultado(s)")
            except FloodWaitError as _fw:
                wait_s = getattr(_fw, "seconds", 30)
                print(f"⏳ FloodWait en canal [{ch_index}]: esperando {wait_s}s automáticamente...")
                await asyncio.sleep(wait_s + 2)
            except Exception as e:
                _ename = (getattr(entity, 'title', None) or getattr(entity, 'username', None) or str(ch_index))
                print(f"⚠️  Error en canal [{ch_index}] ({_ename}): {e}")
            return results

        # ✅ FIX 3: BÚSQUEDA PARALELA REAL — cada canal dentro del grupo se busca en paralelo.
        # Antes los canales dentro de cada grupo se procesaban de forma secuencial.
        # Ahora con asyncio.gather se procesan TODOS al mismo tiempo.
        async def search_group(group_channels: list, account_client) -> list:
            """Busca en un grupo de canales en PARALELO usando una cuenta específica."""
            tasks = [
                search_in_channel(ch_idx, ch_entity, account_client)
                for ch_idx, ch_entity in group_channels
            ]
            raw = await asyncio.gather(*tasks, return_exceptions=True)
            group_results = []
            for r in raw:
                if isinstance(r, list):
                    group_results.extend(r)
                elif isinstance(r, Exception):
                    print(f"⚠️  Canal en grupo falló (paralelo): {r}")
            return group_results

        # Dividir canales en grupos iguales según número de cuentas disponibles
        groups: list = [[] for _ in range(max(1, num_accounts))]
        for pos, (ch_idx, ch_entity) in enumerate(entities_indexed):
            groups[pos % len(groups)].append((ch_idx, ch_entity))

        # Seleccionar un cliente activo y disponible para cada grupo
        async def _get_client_for_group(group_index: int) -> TelegramClient:
            total = len(_telegram_clients)
            idx = group_index % total
            cl = _telegram_clients[idx]
            try:
                if cl.is_connected():
                    return cl
                await cl.connect()
                if await cl.is_user_authorized():
                    return cl
            except Exception:
                pass
            # Fallback: usar el cliente principal
            return _telegram_clients[0]

        # Lanzar todos los grupos en paralelo con timeout global de 9 segundos
        group_tasks = []
        for g_idx, g_channels in enumerate(groups):
            if not g_channels:
                continue
            g_client = await _get_client_for_group(g_idx)
            group_tasks.append(
                asyncio.wait_for(
                    search_group(g_channels, g_client),
                    timeout=9.0,
                )
            )

        print(f"⚡ Búsqueda paralela: {len(groups)} grupos × {num_accounts} cuenta(s) — {len(entities_indexed)} canales en total")

        raw_group_results = await asyncio.gather(*group_tasks, return_exceptions=True)

        all_results: list = []
        for gr in raw_group_results:
            if isinstance(gr, list):
                all_results.extend(gr)
            elif isinstance(gr, Exception):
                print(f"⚠️  Grupo de búsqueda falló: {gr}")

        # ✅ Deduplicación: usar id del mensaje como clave primaria para evitar
        # colapsar títulos genéricos como "Película" entre sí.
        seen, unique = set(), []
        for result in all_results:
            msg_id  = result.get("id")
            title_k = normalize_title(result.get("title", ""))
            # Clave compuesta: id + título (evita colapso por título genérico)
            if msg_id:
                key = f"id:{msg_id}"
            elif title_k:
                key = f"t:{title_k}"
            else:
                key = f"raw:{id(result)}"
            if key not in seen:
                seen.add(key)
                unique.append(result)

        # 🔧 MEJORA 3: Ordenar por capítulos/sagas
        unique = _sort_results_by_saga_and_chapter(unique)
        final_results = unique

        print(f"🎯 Resultados: {len(final_results)} únicos (de {len(all_results)} totales)")

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

        final_schema = _to_peliculas_json_schema(enriched)

        # ✅ ANTI-FLOOD: Guardar en caché SQLite SOLO si la búsqueda es completa y tiene resultados.
        # Se evita cachear búsquedas parciales (usuario todavía escribiendo) o sin resultados reales.
        _text_complete = (not effective_text) or (len((effective_text or "").strip()) >= 3)
        _has_results   = bool(final_schema) and len(final_schema) >= 1
        _worth_caching = _text_complete and _has_results and (
            (effective_text and len(effective_text.strip()) >= 3) or
            effective_year or effective_desde or effective_hasta
        )
        if _worth_caching:
            await asyncio.to_thread(_db_search_set, _search_cache_key, final_schema)
            print(f"💾 Caché SQLite guardada: {len(final_schema)} resultado(s) para clave '{_search_cache_key[:60]}'")

        return final_schema

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
        # ✅ SEPARACIÓN C: Leer el pool de catálogo desde SQLite (actualizado por background task).
        # El scraping de Telegram ocurre en _catalog_background_updater() de forma independiente.
        # El usuario obtiene una respuesta rápida desde la BD sin consultar Telegram en tiempo real.

        # 1. Intentar obtener pool desde SQLite (fuente de verdad del background)
        pool = await asyncio.to_thread(_db_catalog_get)

        # 2. Si SQLite no tiene datos, hacer fallback al pool en memoria
        if not pool:
            now         = time.monotonic()
            pool_cache  = getattr(app.state, "catalog_pool_cache", None) or {"ts": 0.0, "pool": []}
            cached_pool = pool_cache.get("pool") or []
            if isinstance(cached_pool, list) and len(cached_pool) > 0:
                pool = cached_pool
                print("ℹ️  /catalog: usando pool en memoria (SQLite vacío aún)")
            else:
                print("⚠️  /catalog: pool vacío — background aún no completó primera actualización")
                return []

        # ✅ OPT: Muestra máxima de 15 resultados para respuesta rápida y menor carga
        sample_size = min(15, len(pool))
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
        mime        = "image/jpeg"

        # ─────────────────────────────────────────────────────────────────────
        # PASO 1: Verificar si ya existe en disco (persistencia Fly.io)
        # ─────────────────────────────────────────────────────────────────────
        disk_path = os.path.join(THUMBS_DIR, f"{message_id}_{ch}.jpg")
        if os.path.isfile(disk_path):
            try:
                def _read_disk():
                    with open(disk_path, "rb") as _f:
                        return _f.read()
                disk_data = await asyncio.to_thread(_read_disk)
                if disk_data and len(disk_data) > 200:
                    # Refrescar caché RAM también
                    async with app.state.thumb_cache_lock:
                        _thumb_cache_prune(thumb_cache)
                        thumb_cache[cache_key] = (time.monotonic(), disk_data, mime)
                    return Response(content=disk_data, media_type=mime)
            except Exception as _de:
                print(f"   ⚠️  Error leyendo miniatura desde disco ({disk_path}): {_de}")

        # ─────────────────────────────────────────────────────────────────────
        # Verificar caché RAM (segunda capa, más rápida que disco)
        # ─────────────────────────────────────────────────────────────────────
        async with app.state.thumb_cache_lock:
            cached = thumb_cache.get(cache_key)
            if cached:
                ts, data, _m = cached
                if time.monotonic() - ts < THUMB_CACHE_TTL:
                    return Response(content=data, media_type=_m)

        # ─────────────────────────────────────────────────────────────────────
        # PASO 2: No existe en disco → descargar desde Telegram
        # ─────────────────────────────────────────────────────────────────────
        # ✅ Usar cliente activo con reconexión automática
        active_client = await get_active_client()
        if not active_client:
            raise HTTPException(status_code=503, detail="Telegram desconectado")

        entities = getattr(app.state, "entities", [app.state.entity])
        entity   = (
            entities[ch]
            if (0 <= ch < len(entities) and entities[ch] is not None)
            else app.state.entity
        )

        # ✅ Forzar resolución del canal antes de get_messages para evitar
        # "Invalid channel object" cuando Telethon no tiene el canal en caché
        try:
            _channel_ref = getattr(entity, "id", None) or entity
            entity = await active_client.get_entity(_channel_ref)
        except Exception as _re:
            print(f"   ⚠️  No se pudo re-resolver entidad en /thumb ch={ch}: {_re}")

        message = await asyncio.wait_for(
            active_client.get_messages(entity, ids=message_id),
            timeout=3.0,
        )
        if not message:
            raise HTTPException(status_code=404, detail="Miniatura no disponible (mensaje no encontrado)")

        thumb_data: bytes | None = None

        # ✅ PRIORIDAD 1 — miniatura nativa desde message.document.thumbs
        # Intenta primero obtener la imagen completa (thumb=-1 = mayor resolución disponible),
        # y si falla, usa el objeto PhotoSize de mayor resolución como fallback.
        # Es SIEMPRE específica del video correcto → va PRIMERO para evitar mezclar thumbnails.
        if not thumb_data and message.document and message.document.thumbs:
            best = _get_best_native_thumb(message.document.thumbs)
            if best:
                # Intento 1: descargar usando índice -1 (Telethon: mayor thumb disponible)
                try:
                    raw = await asyncio.wait_for(
                        active_client.download_media(message.document, bytes, thumb=-1),
                        timeout=3.0,
                    )
                    if raw and len(raw) > 200:
                        thumb_data = raw
                        print(f"   📎 Miniatura ALTA RES (document thumb=-1) para msg {message_id} [{len(raw)//1024}KB]")
                except Exception:
                    thumb_data = None
                # Intento 2 (fallback): descargar usando el objeto PhotoSize de mayor resolución
                if not thumb_data:
                    try:
                        raw = await asyncio.wait_for(
                            active_client.download_media(message.document, bytes, thumb=best),
                            timeout=3.0,
                        )
                        if raw and len(raw) > 200:
                            thumb_data = raw
                            print(f"   📎 Miniatura nativa (document.thumbs best) para msg {message_id} [{len(raw)//1024}KB]")
                    except Exception:
                        thumb_data = None

        # ✅ PRIORIDAD 2 — miniatura nativa desde message.video.thumbs
        if not thumb_data:
            video_obj = getattr(message, "video", None)
            if video_obj and hasattr(video_obj, "thumbs") and video_obj.thumbs:
                best = _get_best_native_thumb(video_obj.thumbs)
                if best:
                    # Intento 1: índice -1 (mayor resolución)
                    try:
                        raw = await asyncio.wait_for(
                            active_client.download_media(video_obj, bytes, thumb=-1),
                            timeout=3.0,
                        )
                        if raw and len(raw) > 200:
                            thumb_data = raw
                            print(f"   🎬 Miniatura ALTA RES (video thumb=-1) para msg {message_id} [{len(raw)//1024}KB]")
                    except Exception:
                        thumb_data = None
                    # Intento 2 (fallback): objeto PhotoSize
                    if not thumb_data:
                        try:
                            raw = await asyncio.wait_for(
                                active_client.download_media(video_obj, bytes, thumb=best),
                                timeout=3.0,
                            )
                            if raw and len(raw) > 200:
                                thumb_data = raw
                                print(f"   🎬 Miniatura nativa (video.thumbs best) para msg {message_id} [{len(raw)//1024}KB]")
                        except Exception:
                            thumb_data = None

        # ✅ PRIORIDAD 3 — miniatura desde message.media.thumbs (acceso directo al media)
        if not thumb_data:
            media_obj = getattr(message, "media", None)
            media_thumbs = getattr(media_obj, "thumbs", None) if media_obj else None
            if media_thumbs:
                best = _get_best_native_thumb(media_thumbs)
                if best:
                    # Intento 1: índice -1 (mayor resolución)
                    try:
                        raw = await asyncio.wait_for(
                            active_client.download_media(media_obj, bytes, thumb=-1),
                            timeout=3.0,
                        )
                        if raw and len(raw) > 200:
                            thumb_data = raw
                            print(f"   🖼️  Miniatura ALTA RES (media thumb=-1) para msg {message_id} [{len(raw)//1024}KB]")
                    except Exception:
                        thumb_data = None
                    # Intento 2 (fallback): objeto PhotoSize
                    if not thumb_data:
                        try:
                            raw = await asyncio.wait_for(
                                active_client.download_media(media_obj, bytes, thumb=best),
                                timeout=3.0,
                            )
                            if raw and len(raw) > 200:
                                thumb_data = raw
                                print(f"   🖼️  Miniatura nativa (media.thumbs best) para msg {message_id} [{len(raw)//1024}KB]")
                        except Exception:
                            thumb_data = None

        # ✅ PRIORIDAD 4 — imagen completa desde message.photo (máxima calidad disponible)
        # download_media sin thumb descarga la versión original completa de la foto.
        if not thumb_data and hasattr(message, "photo") and message.photo:
            try:
                thumb_data = await asyncio.wait_for(
                    active_client.download_media(message.photo, bytes),
                    timeout=4.0,
                )
                if thumb_data:
                    print(f"   📷 Foto completa (photo) obtenida para msg {message_id} [{len(thumb_data)//1024}KB]")
            except Exception:
                thumb_data = None

        # ✅ PRIORIDAD 5 — buscar póster en mensajes CERCANOS (última opción).
        # Solo se usa si no hay ninguna miniatura nativa disponible en el propio mensaje.
        if not thumb_data:
            try:
                poster = await _find_poster_in_nearby_messages(entity, message_id, active_client)
                if poster and len(poster) > 500:
                    thumb_data = poster
                    print(f"   🖼️  Póster cercano obtenido para msg {message_id} [{len(poster)//1024}KB]")
            except Exception:
                thumb_data = None

        if thumb_data:
            processed = _crop_cover_to_poster(thumb_data)
            # ✅ Guardar en disco (volumen persistente) para reusar tras reinicios
            try:
                def _write_disk(data: bytes):
                    os.makedirs(THUMBS_DIR, exist_ok=True)
                    tmp = disk_path + ".tmp"
                    with open(tmp, "wb") as _f:
                        _f.write(data)
                    os.replace(tmp, disk_path)
                await asyncio.to_thread(_write_disk, processed)
                print(f"   💾 Miniatura guardada en disco: {disk_path}")
            except Exception as _we:
                print(f"   ⚠️  No se pudo guardar miniatura en disco: {_we}")
            # Actualizar caché RAM
            async with app.state.thumb_cache_lock:
                _thumb_cache_prune(thumb_cache)
                thumb_cache[cache_key] = (time.monotonic(), processed, mime)
            return Response(content=processed, media_type=mime)

        # ✅ Sin miniatura en Telegram → usar placeholder para no devolver error
        print(f"   ℹ️  Sin miniatura disponible para msg {message_id} — usando placeholder")
        try:
            async with httpx.AsyncClient(timeout=4.0) as _ph:
                _r = await _ph.get(PLACEHOLDER_IMAGE_BASE)
                if _r.status_code == 200 and _r.content:
                    processed = _crop_cover_to_poster(_r.content)
                    async with app.state.thumb_cache_lock:
                        _thumb_cache_prune(thumb_cache)
                        thumb_cache[cache_key] = (time.monotonic(), processed, mime)
                    return Response(content=processed, media_type=mime)
        except Exception:
            pass

        raise HTTPException(status_code=404, detail="Miniatura no disponible")

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
        # ✅ Usar cliente activo con reconexión automática
        active_client = await get_active_client()
        if not active_client:
            raise HTTPException(status_code=503, detail="Telegram desconectado")

        entities = getattr(app.state, "entities", [app.state.entity])
        entity   = (
            entities[ch]
            if (0 <= ch < len(entities) and entities[ch] is not None)
            else app.state.entity
        )

        # ✅ Forzar resolución de entidad antes de get_messages para evitar
        # "Invalid channel object" cuando Telethon no tiene el canal en caché
        try:
            target_channel = getattr(entity, "id", None) or entity
            entity = await active_client.get_entity(target_channel)
        except Exception as _re:
            print(f"⚠️  No se pudo re-resolver entidad ch={ch}: {_re}")
            # Continuar con el objeto entity original como fallback

        message = await active_client.get_messages(entity, ids=message_id)
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
                """
                Productor/consumidor con pre-buffer asíncrono (asyncio.Queue).
                El productor descarga chunks de Telegram en segundo plano y los
                encola. El consumidor los envía al cliente sin esperar a Telegram,
                eliminando micro-cortes por latencia variable.
                """
                queue: asyncio.Queue = asyncio.Queue(maxsize=STREAM_BUFFER_CHUNKS)
                _SENTINEL = object()

                async def _producer():
                    try:
                        buf = bytearray()
                        async for chunk in active_client.iter_download(
                            message.media,
                            offset=offset,
                            limit=limit,
                            chunk_size=STREAM_CHUNK_SIZE,
                            request_size=STREAM_REQUEST_SIZE,
                        ):
                            buf.extend(chunk)
                            while len(buf) >= STREAM_CHUNK_SIZE:
                                data = bytes(buf[:STREAM_CHUNK_SIZE])
                                del buf[:STREAM_CHUNK_SIZE]
                                await queue.put(data)
                        if buf:
                            await queue.put(bytes(buf))
                    except asyncio.CancelledError:
                        pass
                    except Exception as _pe:
                        print(f"⚠️  Productor interrumpido (full) msg {message_id}: {_pe}")
                    finally:
                        await queue.put(_SENTINEL)

                producer_task = asyncio.ensure_future(_producer())
                try:
                    while True:
                        item = await queue.get()
                        if item is _SENTINEL:
                            break
                        yield item
                except asyncio.CancelledError:
                    producer_task.cancel()
                    return
                except Exception as _ce:
                    print(f"⚠️  Consumidor interrumpido (full) msg {message_id}: {_ce}")
                    producer_task.cancel()
                    return

            headers = {
                "Content-Type":      content_type,
                "Accept-Ranges":     "bytes",
                "Content-Length":    str(content_length),
                "Cache-Control":     "private, max-age=3600",
                "Connection":        "keep-alive",
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
            """
            Productor/consumidor con pre-buffer asíncrono (asyncio.Queue).
            Idéntico al generador full pero para peticiones Range parciales.
            Previene pausas del player cuando Telegram responde con latencia variable.
            """
            queue: asyncio.Queue = asyncio.Queue(maxsize=STREAM_BUFFER_CHUNKS)
            _SENTINEL = object()

            async def _producer():
                try:
                    buf = bytearray()
                    async for chunk in active_client.iter_download(
                        message.media,
                        offset=offset,
                        limit=limit,
                        chunk_size=STREAM_CHUNK_SIZE,
                        request_size=STREAM_REQUEST_SIZE,
                    ):
                        buf.extend(chunk)
                        while len(buf) >= STREAM_CHUNK_SIZE:
                            data = bytes(buf[:STREAM_CHUNK_SIZE])
                            del buf[:STREAM_CHUNK_SIZE]
                            await queue.put(data)
                    if buf:
                        await queue.put(bytes(buf))
                except asyncio.CancelledError:
                    pass
                except Exception as _pe:
                    print(f"⚠️  Productor interrumpido (range) msg {message_id}: {_pe}")
                finally:
                    await queue.put(_SENTINEL)

            producer_task = asyncio.ensure_future(_producer())
            try:
                while True:
                    item = await queue.get()
                    if item is _SENTINEL:
                        break
                    yield item
            except asyncio.CancelledError:
                producer_task.cancel()
                return
            except Exception as _ce:
                print(f"⚠️  Consumidor interrumpido (range) msg {message_id}: {_ce}")
                producer_task.cancel()
                return

        headers = {
            "Content-Type":      content_type,
            "Accept-Ranges":     "bytes",
            "Content-Range":     f"bytes {start}-{end}/{file_size}",
            "Content-Length":    str(content_length),
            "Cache-Control":     "private, max-age=3600",
            "Connection":        "keep-alive",
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
