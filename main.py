
import os
import asyncio
import sqlite3
import unicodedata
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel
from telethon.errors import FloodWaitError

# ===============================
# TELEGRAM CONFIG (3 ACCOUNTS)
# ===============================

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")

API_ID_2 = int(os.getenv("API_ID_2", "0"))
API_HASH_2 = os.getenv("API_HASH_2", "")
SESSION_STRING_2 = os.getenv("SESSION_STRING_2", "")

API_ID_3 = int(os.getenv("API_ID_3", "0"))
API_HASH_3 = os.getenv("API_HASH_3", "")
SESSION_STRING_3 = os.getenv("SESSION_STRING_3", "")

# ===============================
# CHANNELS TO INDEX
# ===============================

CHANNELS = [
    "https://t.me/animadasssss",
    "https://t.me/pelisdeterror2",
    "https://t.me/cinecalidad",
]

# ===============================
# DATABASE
# ===============================

DB_PATH = "movies_index.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        '''
        CREATE TABLE IF NOT EXISTS movies(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            title_norm TEXT,
            saga TEXT,
            channel TEXT,
            msg_id INTEGER,
            size INTEGER
        )
        '''
    )

    conn.commit()
    conn.close()

# ===============================
# TEXT NORMALIZATION
# ===============================

def normalize(text: str) -> str:
    if not text:
        return ""

    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")

    return text

def detect_saga(title: str):

    t = normalize(title)

    if "shrek" in t:
        return "Shrek"

    if "harry potter" in t:
        return "Harry Potter"

    if "lord of the rings" in t or "señor de los anillos" in t:
        return "El Señor de los Anillos"

    if "ice age" in t or "era de hielo" in t:
        return "Ice Age"

    return None

# ===============================
# TELEGRAM CLIENT POOL
# ===============================

clients: List[TelegramClient] = []
client_index = 0
client_lock = asyncio.Lock()

async def get_client():

    global client_index

    async with client_lock:

        c = clients[client_index]
        client_index = (client_index + 1) % len(clients)

        return c

# ===============================
# INDEXER
# ===============================

async def index_channel(channel):

    client = await get_client()

    try:
        entity = await client.get_entity(channel)

        if not isinstance(entity, Channel):
            return

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        async for msg in client.iter_messages(entity):

            if not msg.media:
                continue

            title = msg.text or ""

            title_norm = normalize(title)

            saga = detect_saga(title)

            size = 0

            if msg.file:
                size = msg.file.size

            cur.execute(
                "INSERT INTO movies(title,title_norm,saga,channel,msg_id,size) VALUES(?,?,?,?,?,?)",
                (title, title_norm, saga, channel, msg.id, size)
            )

        conn.commit()
        conn.close()

        print("Indexed:", channel)

    except FloodWaitError as e:
        print("FloodWait:", e.seconds)
        await asyncio.sleep(e.seconds)

async def full_index():

    tasks = []

    for ch in CHANNELS:
        tasks.append(index_channel(ch))

    await asyncio.gather(*tasks)

# ===============================
# FAST SEARCH
# ===============================

def search_movies(query: str):

    q = normalize(query)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT title,channel,msg_id,saga FROM movies WHERE title_norm LIKE ? LIMIT 50",
        ("%" + q + "%",)
    )

    rows = cur.fetchall()

    conn.close()

    results = []

    for r in rows:

        results.append({
            "title": r[0],
            "channel": r[1],
            "msg_id": r[2],
            "saga": r[3],
            "stream": f"/stream/{r[2]}?ch={r[1]}"
        })

    return results

# ===============================
# FASTAPI APP
# ===============================

app = FastAPI()

@app.on_event("startup")
async def startup():

    init_db()

    client1 = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    client2 = TelegramClient(StringSession(SESSION_STRING_2), API_ID_2, API_HASH_2)
    client3 = TelegramClient(StringSession(SESSION_STRING_3), API_ID_3, API_HASH_3)

    await client1.start()
    await client2.start()
    await client3.start()

    clients.extend([client1, client2, client3])

    print("Telegram accounts connected")

# ===============================
# ENDPOINTS
# ===============================

@app.get("/search")
async def search(q: str):

    results = search_movies(q)

    return JSONResponse(results)

@app.get("/catalog")
async def catalog():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT title,channel,msg_id FROM movies ORDER BY id DESC LIMIT 15"
    )

    rows = cur.fetchall()

    conn.close()

    return [
        {
            "title": r[0],
            "stream": f"/stream/{r[2]}?ch={r[1]}"
        }
        for r in rows
    ]

@app.get("/stream/{msg_id}")
async def stream(msg_id: int, ch: str):

    client = await get_client()

    try:
        entity = await client.get_entity(ch)
        msg = await client.get_messages(entity, ids=msg_id)

        if not msg:
            raise HTTPException(404)

        data = await client.download_media(msg.media, file=bytes)

        async def iterator():
            yield data

        return StreamingResponse(iterator(), media_type="video/mp4")

    except FloodWaitError as e:
        await asyncio.sleep(e.seconds)
        raise HTTPException(429)

@app.get("/index")
async def run_index():

    await full_index()

    return {"status": "index completed"}
