// index.js — API completa de Películas con sistema de usuarios + MaguisTV style
// ¡MEJORADO con Respaldo en GitHub para Historial y Favoritos!
// ¡MEJORADO con integración de API externa Peliprex para ampliar el catálogo!
// ¡MEJORADO con Proxy/Pasarela hacia Python (streaming, /search, /catalog, etc.)!

import express from "express";
import cors from "cors";
import fs from "fs";
import fetch from "node-fetch";
import http from "http";
import path from "path";

const app = express();
app.use(cors());

// ------------------- GITHUB CONFIGURACIÓN DE RESPALDO -------------------
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_REPO = process.env.GITHUB_REPO;
const BACKUP_FILE_NAME = "users_data.json";
const PELIS_FILE_NAME = "peliculas.json";

// ------------------- 🆕 API EXTERNA PELIPREX -------------------
const EXTERNAL_API_BASE = "http://127.0.0.1:8081";
const EXTERNAL_API_URL = `${EXTERNAL_API_BASE}/catalog`;
const EXTERNAL_API_SEARCH = `${EXTERNAL_API_BASE}/search`;
const EXTERNAL_API_REFRESH_MS = 2 * 60 * 60 * 1000;
const EXTERNAL_API_TIMEOUT_MS = 120_000;

// 📂 Archivos locales
const PELIS_FILE = path.join(process.cwd(), "peliculas.json");
const USERS_FILE = path.join(process.cwd(), BACKUP_FILE_NAME);

// ------------------- 🆕 "LO MÁS VISTO HOY" -------------------
const VISTO_HOY_FILE_NAME = "visto_hoy.json";
const VISTO_HOY_RESET_MS  = 12 * 60 * 60 * 1000;

let vistosHoy = {};
let vistosHoyResetAt = Date.now();

// ------------------- FUNCIONES AUXILIARES -------------------

function cleanPeliculaUrl(url) {
  if (!url) return url;
  return url.replace(/\/prepreview([?#]|$)/, '/preview$1');
}

function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = EXTERNAL_API_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(timer);
    return resp;
  } catch (err) {
    clearTimeout(timer);
    const reason = err.name === "AbortError" ? `timeout (>${timeoutMs / 1000}s sin respuesta)` : err.message;
    console.warn(`⚠️ [API Externa] Petición fallida silenciosamente: ${reason}`);
    return { ok: false, status: 0, _timedOut: true };
  }
}

async function fetchExternalSearch(params = {}) {
  try {
    const url = new URL(EXTERNAL_API_SEARCH);
    if (params.q)     url.searchParams.set("q",     params.q);
    if (params.genre) url.searchParams.set("genre", params.genre);
    if (params.year)  url.searchParams.set("year",  params.year);

    console.log(`🔍 [API Externa] Consultando búsqueda: ${url.toString()} — esperando respuesta...`);
    const resp = await fetchWithTimeout(url.toString());

    if (!resp.ok) {
      if (resp.status === 502 || resp.status === 504) {
        console.warn(`⚠️ [API Externa] Error ${resp.status} en búsqueda — usando solo resultados locales.`);
      } else if (resp.status !== 0) {
        console.warn(`⚠️ [API Externa] HTTP ${resp.status} en búsqueda — usando solo resultados locales.`);
      }
      return [];
    }

    const data = await resp.json();
    if (!Array.isArray(data)) {
      console.warn(`⚠️ [API Externa] Respuesta inesperada (no es array) en búsqueda.`);
      return [];
    }
    console.log(`✅ [API Externa] Búsqueda respondió con ${data.length} resultado(s).`);
    return data.map(p => ({ ...p, _fromExternalApi: true }));
  } catch (err) {
    console.warn(`⚠️ [API Externa] Error inesperado en búsqueda: ${err.message} — usando solo resultados locales.`);
    return [];
  }
}

function mergeResults(localResults, externalResults) {
  const combined = [...localResults];
  const localIds = new Set(localResults.map(p => String(p.id)).filter(Boolean));
  const localTitles = new Set(localResults.map(p => (p.titulo || "").toLowerCase()));

  for (const ext of externalResults) {
    const extId = ext.id ? String(ext.id) : null;
    const extTitle = (ext.titulo || "").toLowerCase();
    if (extId && localIds.has(extId)) continue;
    if (localTitles.has(extTitle)) continue;
    combined.push(ext);
  }
  return combined;
}

function mergeExternalIntoPeliculasFile(externalMovies) {
  try {
    if (!Array.isArray(externalMovies) || externalMovies.length === 0) return;

    const existingIds    = new Set(localPeliculas.map(p => p.id ? String(p.id) : null).filter(Boolean));
    const existingTitles = new Set(localPeliculas.map(p => (p.titulo || "").toLowerCase()));

    const nuevas = externalMovies.filter(p => {
      const id    = p.id ? String(p.id) : null;
      const title = (p.titulo || "").toLowerCase();
      if (id && existingIds.has(id))       return false;
      if (existingTitles.has(title))       return false;
      return true;
    }).map(p => {
      const { _fromExternalApi, ...rest } = p;
      return rest;
    });

    if (nuevas.length === 0) {
      console.log("ℹ️ [peliculas.json] Sin películas nuevas de la API externa para guardar.");
      return;
    }

    localPeliculas = [...localPeliculas, ...nuevas];

    const content = JSON.stringify(localPeliculas, null, 2);
    fs.writeFileSync(PELIS_FILE, content, "utf8");

    console.log(`💾 [peliculas.json] ${nuevas.length} película(s) nueva(s) guardadas localmente. Total local: ${localPeliculas.length}`);

    savePeliculasToGitHub(content)
      .then(ok => {
        if (ok) console.log(`✅ [peliculas.json] Respaldo en GitHub completado (${localPeliculas.length} películas).`);
      })
      .catch(err => console.error(`❌ [peliculas.json] Error en respaldo GitHub: ${err.message}`));

  } catch (err) {
    console.error(`❌ [peliculas.json] Error al guardar películas externas: ${err.message}`);
  }
}

// ------------------- FUNCIONES DE GITHUB -------------------

async function getFileSha(filePath) {
  if (!GITHUB_TOKEN || !GITHUB_REPO) return null;
  const url = `https://api.github.com/repos/${GITHUB_REPO}/contents/${filePath}`;
  try {
    const resp = await fetch(url, {
      headers: {
        'Authorization': `token ${GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github.v3+json',
      },
    });

    if (resp.status === 404) return null;
    if (!resp.ok) {
      console.error(`❌ Error al obtener SHA de GitHub (Status ${resp.status}): ${await resp.text()}`);
      return null;
    }

    const data = await resp.json();
    return data.sha;
  } catch (error) {
    console.error("❌ Excepción al obtener SHA de GitHub:", error.message);
    return null;
  }
}

async function saveUsersDataToGitHub(content) {
  if (!GITHUB_TOKEN || !GITHUB_REPO) {
    console.log("⚠️ GitHub no configurado (Faltan GITHUB_TOKEN o GITHUB_REPO). Solo guardado local.");
    return false;
  }

  console.log(`💾 Iniciando respaldo de ${BACKUP_FILE_NAME} en GitHub...`);
  try {
    const sha = await getFileSha(BACKUP_FILE_NAME);
    const contentBase64 = Buffer.from(content).toString('base64');
    const commitMessage = `Automated backup: Update ${BACKUP_FILE_NAME} at ${new Date().toISOString()}`;

    const url = `https://api.github.com/repos/${GITHUB_REPO}/contents/${BACKUP_FILE_NAME}`;

    const resp = await fetch(url, {
      method: 'PUT',
      headers: {
        'Authorization': `token ${GITHUB_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        message: commitMessage,
        content: contentBase64,
        sha: sha,
      }),
    });

    if (!resp.ok) {
      console.error(`❌ Error al guardar en GitHub (Status: ${resp.status}): ${await resp.text()}`);
      return false;
    }

    console.log("✅ Datos de usuario respaldados en GitHub con éxito.");
    return true;

  } catch (error) {
    console.error("❌ Excepción al guardar en GitHub:", error.message);
    return false;
  }
}

async function savePeliculasToGitHub(content) {
  if (!GITHUB_TOKEN || !GITHUB_REPO) {
    console.log("⚠️ GitHub no configurado (Faltan GITHUB_TOKEN o GITHUB_REPO). Solo guardado local de películas.");
    return false;
  }

  console.log(`💾 Iniciando respaldo de ${PELIS_FILE_NAME} en GitHub...`);
  try {
    const sha = await getFileSha(PELIS_FILE_NAME);
    const contentBase64 = Buffer.from(content).toString('base64');
    const commitMessage = `Automated backup: Update ${PELIS_FILE_NAME} at ${new Date().toISOString()}`;

    const url = `https://api.github.com/repos/${GITHUB_REPO}/contents/${PELIS_FILE_NAME}`;

    const resp = await fetch(url, {
      method: 'PUT',
      headers: {
        'Authorization': `token ${GITHUB_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        message: commitMessage,
        content: contentBase64,
        sha: sha,
      }),
    });

    if (!resp.ok) {
      console.error(`❌ Error al guardar películas en GitHub (Status: ${resp.status}): ${await resp.text()}`);
      return false;
    }

    console.log(`✅ Catálogo de películas respaldado en GitHub con éxito.`);
    return true;

  } catch (error) {
    console.error("❌ Excepción al guardar películas en GitHub:", error.message);
    return false;
  }
}

// ------------------- 🆕 FUNCIONES DE "LO MÁS VISTO HOY" -------------------

function registrarVistaHoy({ titulo, imagen_url, pelicula_url, api_id }) {
  const key = api_id ? `api:${api_id}` : (pelicula_url || titulo);
  if (!key) return;

  if (vistosHoy[key]) {
    vistosHoy[key].count += 1;
    vistosHoy[key].lastSeen = new Date().toISOString();
    if (imagen_url) vistosHoy[key].imagen_url = imagen_url;
  } else {
    vistosHoy[key] = {
      titulo,
      imagen_url: imagen_url || "",
      pelicula_url: api_id ? null : (pelicula_url || null),
      api_id: api_id ? String(api_id) : null,
      _fromExternalApi: !!api_id,
      count: 1,
      lastSeen: new Date().toISOString()
    };
  }
}

function getVistosHoyArray() {
  return Object.values(vistosHoy)
    .filter(v => v.count >= 1)
    .sort((a, b) => b.count - a.count);
}

async function saveVistosHoyToGitHub() {
  if (!GITHUB_TOKEN || !GITHUB_REPO) return false;
  try {
    const payload = {
      resetAt: new Date(vistosHoyResetAt).toISOString(),
      generadoEn: new Date().toISOString(),
      datos: getVistosHoyArray()
    };
    const content = JSON.stringify(payload, null, 2);
    const sha = await getFileSha(VISTO_HOY_FILE_NAME);
    const contentBase64 = Buffer.from(content).toString("base64");
    const url = `https://api.github.com/repos/${GITHUB_REPO}/contents/${VISTO_HOY_FILE_NAME}`;
    const resp = await fetch(url, {
      method: "PUT",
      headers: {
        "Authorization": `token ${GITHUB_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        message: `Automated: Update ${VISTO_HOY_FILE_NAME} at ${new Date().toISOString()}`,
        content: contentBase64,
        sha: sha
      })
    });
    if (!resp.ok) {
      console.error(`\u274C [visto_hoy] Error al guardar en GitHub (Status ${resp.status}): ${await resp.text()}`);
      return false;
    }
    console.log(`\u2705 [visto_hoy] Snapshot guardado en GitHub (${payload.datos.length} peliculas).`);
    return true;
  } catch (err) {
    console.error(`\u274C [visto_hoy] Excepcion al guardar en GitHub: ${err.message}`);
    return false;
  }
}

async function loadVistosHoyFromGitHub() {
  if (!GITHUB_TOKEN || !GITHUB_REPO) return;
  try {
    const url = `https://api.github.com/repos/${GITHUB_REPO}/contents/${VISTO_HOY_FILE_NAME}`;
    const resp = await fetch(url, {
      headers: {
        "Authorization": `token ${GITHUB_TOKEN}`,
        "Accept": "application/vnd.github.v3.raw"
      }
    });
    if (resp.status === 404) {
      console.log("i [visto_hoy] No existe archivo previo en GitHub. Empezando conteo desde cero.");
      return;
    }
    if (!resp.ok) {
      console.warn(`[visto_hoy] No se pudo cargar desde GitHub (Status ${resp.status}).`);
      return;
    }
    const text = await resp.text();
    const payload = JSON.parse(text);
    const resetAt = new Date(payload.resetAt).getTime();
    const age = Date.now() - resetAt;
    if (age >= VISTO_HOY_RESET_MS) {
      console.log(`i [visto_hoy] Datos expirados (>${VISTO_HOY_RESET_MS / 3600000}h). Se empieza desde cero.`);
      vistosHoy = {};
      vistosHoyResetAt = Date.now();
      return;
    }
    vistosHoyResetAt = resetAt;
    vistosHoy = {};
    for (const item of (payload.datos || [])) {
      const key = item.api_id ? `api:${item.api_id}` : (item.pelicula_url || item.titulo);
      if (key) vistosHoy[key] = item;
    }
    console.log(`\u2705 [visto_hoy] ${Object.keys(vistosHoy).length} entradas restauradas desde GitHub.`);
  } catch (err) {
    console.warn(`[visto_hoy] Error al cargar desde GitHub: ${err.message}. Empezando desde cero.`);
    vistosHoy = {};
    vistosHoyResetAt = Date.now();
  }
}

setInterval(async () => {
  console.log("Reseteando conteo de 'Lo mas visto hoy' (12 horas cumplidas)...");
  await saveVistosHoyToGitHub();
  vistosHoy = {};
  vistosHoyResetAt = Date.now();
  console.log("\u2705 [visto_hoy] Conteo reiniciado. Nuevo periodo comenzando.");
}, VISTO_HOY_RESET_MS);


async function loadUsersDataFromGitHub() {
  if (!GITHUB_TOKEN || !GITHUB_REPO) return false;

  const url = `https://api.github.com/repos/${GITHUB_REPO}/contents/${BACKUP_FILE_NAME}`;
  console.log(`📡 Intentando cargar ${BACKUP_FILE_NAME} desde GitHub...`);

  try {
    const resp = await fetch(url, {
      headers: {
        'Authorization': `token ${GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github.v3.raw',
      },
    });

    if (resp.status === 404) {
      console.log(`ℹ️ Archivo no encontrado en GitHub. Se creará un nuevo archivo local si es necesario.`);
      return false;
    }

    if (!resp.ok) {
      console.error(`❌ Error al cargar de GitHub (Status: ${resp.status}): ${await resp.text()}`);
      return false;
    }

    const content = await resp.text();
    fs.writeFileSync(USERS_FILE, content, 'utf8');
    console.log(`✅ Datos de usuario cargados y restaurados localmente desde GitHub.`);
    return true;

  } catch (error) {
    console.error("❌ Excepción al cargar de GitHub:", error.message);
    return false;
  }
}


// ------------------- CARGAR PELÍCULAS LOCALES -------------------
let localPeliculas = [];
try {
  localPeliculas = JSON.parse(fs.readFileSync(PELIS_FILE, "utf8"));
  console.log(`✅ Cargadas ${localPeliculas.length} películas desde peliculas.json`);
} catch (err) {
  console.error("❌ Error cargando peliculas.json:", err.message);
  localPeliculas = [];
}

// ------------------- 🆕 CARGAR PELÍCULAS DE API EXTERNA (PELIPREX) -------------------
let externalApiMovies = [];

async function loadExternalApiMovies() {
  try {
    console.log("📡 Cargando películas desde API externa Peliprex — esperando respuesta...");
    const resp = await fetchWithTimeout(EXTERNAL_API_URL);

    if (!resp.ok) {
      if (resp.status === 502 || resp.status === 504) {
        console.warn(`⚠️ [API Externa] Error ${resp.status} al cargar catálogo — manteniendo caché anterior (${externalApiMovies.length} películas).`);
      } else if (resp.status !== 0) {
        console.warn(`⚠️ [API Externa] HTTP ${resp.status} al cargar catálogo — manteniendo caché anterior.`);
      }
      return externalApiMovies;
    }

    const data = await resp.json();
    if (!Array.isArray(data)) {
      console.warn(`⚠️ [API Externa] Respuesta inesperada al cargar catálogo (no es array) — manteniendo caché anterior.`);
      return externalApiMovies;
    }
    externalApiMovies = data.map(p => ({ ...p, _fromExternalApi: true }));
    mergeExternalIntoPeliculasFile(externalApiMovies);
    peliculas = [...localPeliculas, ...externalApiMovies];
    console.log(`✅ Cargadas ${externalApiMovies.length} películas desde API externa Peliprex`);
    console.log(`📚 Catálogo total actualizado: ${peliculas.length} películas`);
    return externalApiMovies;
  } catch (err) {
    console.warn(`⚠️ [API Externa] Error inesperado al cargar catálogo: ${err.message} — manteniendo caché anterior.`);
    return externalApiMovies;
  }
}

let peliculas = [...localPeliculas];

setInterval(async () => {
  console.log("🔄 Refrescando catálogo de API externa Peliprex (renovando URLs expiradas)...");
  await loadExternalApiMovies();
}, EXTERNAL_API_REFRESH_MS);


// ------------------- FUNCIONES DE USUARIOS -------------------
function ensureUsersFile() {
  if (!fs.existsSync(USERS_FILE)) {
    console.log(`ℹ️ Creando archivo local: ${BACKUP_FILE_NAME}`);
    const initialData = JSON.stringify({ users: {} }, null, 2);
    fs.writeFileSync(USERS_FILE, initialData);
    saveUsersDataToGitHub(initialData);
  }
}
function readUsersData() {
  ensureUsersFile();
  return JSON.parse(fs.readFileSync(USERS_FILE, "utf8"));
}
function writeUsersData(data) {
  const content = JSON.stringify(data, null, 2);
  fs.writeFileSync(USERS_FILE, content);
  saveUsersDataToGitHub(content);
}
function getOrCreateUser(email) {
  if (!email) return null;
  const data = readUsersData();
  if (!data.users[email]) {
    data.users[email] = {
      email,
      tipoPlan: "creditos",
      credits: 0,
      favorites: [],
      history: [],
      resume: {},
      lastActivityTimestamp: new Date().toISOString()
    };
    writeUsersData(data);
  }

  if (!data.users[email].resume) data.users[email].resume = {};
  if (!data.users[email].lastActivityTimestamp) data.users[email].lastActivityTimestamp = new Date().toISOString();

  return data.users[email];
}
function saveUser(email, userObj) {
  const data = readUsersData();
  data.users[email] = userObj;
  writeUsersData(data);
}

// ------------------- CONTROL DE INACTIVIDAD DEL SERVIDOR -------------------
let ultimaPeticion = Date.now();
const TIEMPO_INACTIVIDAD = 3 * 60 * 1000;

setInterval(async () => {
  if (Date.now() - ultimaPeticion >= TIEMPO_INACTIVIDAD) {
    console.log("🕒 Sin tráfico por 3 minutos. Iniciando cierre y respaldo final...");

    try {
      const data = readUsersData();
      const content = JSON.stringify(data, null, 2);
      const saved = await saveUsersDataToGitHub(content);
      console.log(`✅ Respaldo final ${saved ? 'exitoso' : 'fallido'}. Cerrando servidor.`);
    } catch (e) {
      console.error("❌ Error durante el cierre y respaldo final:", e.message);
    }

    process.exit(0);
  }
}, 60 * 1000);

app.use((req, res, next) => {
  ultimaPeticion = Date.now();
  next();
});


// ------------------- TAREA PROGRAMADA: ELIMINACIÓN DE ACTIVIDAD CADA 24 HRS -------------------
const MS_IN_24_HOURS = 24 * 60 * 60 * 1000;

setInterval(() => {
    console.log("🧹 Iniciando chequeo de limpieza de actividad de 24 horas...");
    const data = readUsersData();
    let usersModified = false;
    const now = Date.now();

    for (const email in data.users) {
        const user = data.users[email];
        let userActivityModified = false;

        const historyLengthBefore = user.history.length;
        user.history = user.history.filter(h => {
            const historyDate = new Date(h.fecha).getTime();
            return now - historyDate < MS_IN_24_HOURS;
        });
        if (user.history.length !== historyLengthBefore) {
            console.log(`   [${email}] Historial: Eliminados ${historyLengthBefore - user.history.length} elementos por antigüedad (>24h).`);
            userActivityModified = true;
        }

        const resumeKeysBefore = Object.keys(user.resume).length;
        const newResume = {};
        for (const url in user.resume) {
            const resumeEntry = user.resume[url];
            const lastHeartbeatDate = new Date(resumeEntry.lastHeartbeat).getTime();
            if (now - lastHeartbeatDate < MS_IN_24_HOURS) {
                newResume[url] = resumeEntry;
            }
        }
        user.resume = newResume;
        const resumeKeysAfter = Object.keys(user.resume).length;

        if (resumeKeysAfter !== resumeKeysBefore) {
            console.log(`   [${email}] Resumen: Eliminados ${resumeKeysBefore - resumeKeysAfter} elementos por inactividad (>24h).`);
            userActivityModified = true;
        }

        if (userActivityModified) {
            usersModified = true;
        }
    }

    if (usersModified) {
        writeUsersData(data);
        console.log("✅ Limpieza de actividad completada y datos guardados.");
    } else {
        console.log("ℹ️ No se encontraron actividades para limpiar.");
    }

}, MS_IN_24_HOURS);


// =============================================================================
// 🆕 PROXY/PASARELA HACIA PYTHON (puerto 8081)
// =============================================================================
// Estas rutas capturan las peticiones antes de que lleguen a las rutas de Express
// y las redirigen directamente al servidor Python interno (FastAPI en :8081).
//
// Rutas cubiertas:
//   /stream/:id          → streaming de video desde Telegram
//   /archive-stream/:id  → proxy de streaming desde Archive.org
//   /thumb/:id           → miniaturas de mensajes
//   /ytthumb/:id         → proxy de thumbnails de YouTube
//   /search              → búsqueda avanzada con Telegram + TMDB + Archive
//   /catalog             → catálogo aleatorio desde canales Telegram
//   /health              → health check del servicio Python
//
// El pipe es bidireccional: headers, body y status code se transmiten tal cual.
// =============================================================================

/**
 * Construye la URL de destino en Python a partir de la URL de Express,
 * preservando la ruta completa (path) y la query string.
 */
function buildPythonUrl(req) {
  const query = req.url.includes("?") ? req.url.slice(req.url.indexOf("?")) : "";
  return `http://127.0.0.1:8081${req.path}${query}`;
}

/**
 * Middleware genérico de proxy hacia Python.
 * Hace pipe completo de la respuesta (streaming, binario y JSON incluidos).
 */
function proxyToPython(req, res) {
  const targetUrl = buildPythonUrl(req);
  console.log(`🔀 [Proxy→Python] ${req.method} ${req.path} → ${targetUrl}`);

  // Construir los headers que enviamos a Python (reenviar los del cliente)
  const proxyHeaders = {};
  const forwardHeaders = ["range", "accept", "accept-encoding", "cache-control", "user-agent", "referer", "origin"];
  for (const h of forwardHeaders) {
    if (req.headers[h]) proxyHeaders[h] = req.headers[h];
  }

  const options = {
    hostname: "127.0.0.1",
    port: 8081,
    path: `${req.path}${req.url.includes("?") ? req.url.slice(req.url.indexOf("?")) : ""}`,
    method: req.method,
    headers: proxyHeaders,
  };

  const proxyReq = http.request(options, (proxyRes) => {
    // Reenviar status y headers de Python al cliente original
    res.status(proxyRes.statusCode);

    // Headers seguros para reenviar al cliente
    const passHeaders = [
      "content-type",
      "content-length",
      "content-range",
      "accept-ranges",
      "cache-control",
      "etag",
      "last-modified",
      "x-accel-buffering",
      "content-disposition",
    ];
    for (const h of passHeaders) {
      if (proxyRes.headers[h]) res.setHeader(h, proxyRes.headers[h]);
    }

    // Pipe directo del cuerpo de la respuesta (soporta streaming binario)
    proxyRes.pipe(res);
  });

  proxyReq.on("error", (err) => {
    console.error(`❌ [Proxy→Python] Error en ${req.path}: ${err.message}`);
    if (!res.headersSent) {
      res.status(502).json({ error: "Servicio Python no disponible temporalmente. Reintenta en unos segundos." });
    }
  });

  // Si la petición del cliente tiene body (POST, PUT), hacer pipe también
  req.pipe(proxyReq);
}

// --- Rutas de streaming y recursos desde Python ---
app.get("/stream/:messageId",        proxyToPython);
app.get("/archive-stream/:identifier", proxyToPython);
app.get("/thumb/:messageId",         proxyToPython);
app.get("/ytthumb/:videoId",         proxyToPython);

// --- Rutas de catálogo y búsqueda desde Python ---
app.get("/search",                   proxyToPython);
app.get("/catalog",                  proxyToPython);

// --- Health check de Python ---
app.get("/health",                   proxyToPython);

// =============================================================================
// FIN PROXY/PASARELA
// =============================================================================


// ------------------- RUTAS PRINCIPALES -------------------
app.get("/", (req, res) => {
  res.json({
    mensaje: "🎬 API de Películas funcionando correctamente",
    total: peliculas.length,
    ejemplo: "/peliculas o /peliculas/El%20Padrino"
  });
});

app.get("/peliculas", (req, res) => res.json(peliculas));

app.get("/peliculas/:titulo", async (req, res) => {
  const tituloRaw = decodeURIComponent(req.params.titulo || "");
  const titulo = tituloRaw.toLowerCase();

  const [externalResults] = await Promise.all([
    fetchExternalSearch({ q: tituloRaw })
  ]);

  const localResults = localPeliculas.filter(p =>
    (p.titulo || "").toLowerCase().includes(titulo)
  );

  const combinado = mergeResults(localResults, externalResults);

  if (combinado.length > 0) {
    return res.json({ fuente: "combinado", total: combinado.length, resultados: combinado });
  }

  return res.json({
    fuente: "combinado",
    total: 0,
    resultados: [],
    error: "Película no encontrada en el catálogo."
  });
});

app.get("/buscar", async (req, res) => {
  const { año, genero, idioma, desde, hasta, q } = req.query;

  let localResultados = localPeliculas;

  if (q) {
    const ql = q.toLowerCase();
    localResultados = localResultados.filter(p =>
      (p.titulo || "").toLowerCase().includes(ql) ||
      (p.descripcion || "").toLowerCase().includes(ql)
    );
  }
  if (año) localResultados = localResultados.filter(p => String(p.año) === String(año));
  if (genero)
    localResultados = localResultados.filter(p =>
      (p.generos || "").toLowerCase().includes(String(genero).toLowerCase())
    );
  if (idioma)
    localResultados = localResultados.filter(
      p => (p.idioma_original || "").toLowerCase() === String(idioma).toLowerCase()
    );
  if (desde && hasta)
    localResultados = localResultados.filter(
      p =>
        parseInt(p.año) >= parseInt(desde) &&
        parseInt(p.año) <= parseInt(hasta)
    );

  const externalParams = {};
  if (q)      externalParams.q     = q;
  if (genero) externalParams.genre = genero;
  if (año)    externalParams.year  = año;
  if (!q && genero) externalParams.q = genero;
  if (!q && !genero && desde) externalParams.q = desde;

  const externalResults = await fetchExternalSearch(externalParams);

  const combinado = mergeResults(localResultados, externalResults);

  if (combinado.length > 0) {
    return res.json({ fuente: "combinado", total: combinado.length, resultados: combinado });
  }

  res.json({
    fuente: "combinado",
    total: 0,
    resultados: [],
    error: "No se encontraron películas con los criterios de búsqueda."
  });
});

app.get("/peliculas/categoria/:genero", (req, res) => {
    const generoRaw = decodeURIComponent(req.params.genero || "");
    const generoBuscado = generoRaw.toLowerCase();

    const resultados = localPeliculas.filter(p =>
        (p.generos || "").toLowerCase().includes(generoBuscado)
    );

    if (resultados.length > 0) {
        return res.json({
            fuente: "local",
            total: resultados.length,
            resultados: shuffleArray(resultados)
        });
    }

    return res.json({
        fuente: "local",
        total: 0,
        resultados: [],
        error: "No se encontraron películas en esa categoría."
    });
});

app.get("/nuevo", async (req, res) => {
  try {
    console.log(`📡 [/nuevo] Consultando API externa — esperando respuesta completa...`);
    const resp = await fetchWithTimeout(EXTERNAL_API_URL);
    if (!resp.ok) {
      console.warn(`⚠️ [/nuevo] API externa respondió con ${resp.status || "timeout"}.`);
      return res.status(503).json({ error: "La API externa no está disponible en este momento. Intenta de nuevo más tarde." });
    }
    const data = await resp.json();
    console.log(`✅ [/nuevo] API externa respondió con ${Array.isArray(data) ? data.length : "?"} elemento(s).`);
    res.json(data);
  } catch (err) {
    console.warn(`⚠️ [/nuevo] Error inesperado: ${err.message}`);
    res.status(503).json({ error: "La API externa no está disponible en este momento. Intenta de nuevo más tarde." });
  }
});

app.get("/vistohoy", (req, res) => {
  const limit = req.query.limit ? parseInt(req.query.limit) : undefined;
  let resultados = getVistosHoyArray();
  if (limit && limit > 0) resultados = resultados.slice(0, limit);
  res.json({
    fuente: "vistohoy",
    periodoDesde: new Date(vistosHoyResetAt).toISOString(),
    generadoEn: new Date().toISOString(),
    total: resultados.length,
    resultados
  });
});

app.get("/sugerencias", (req, res) => {
  const tituloRaw  = (req.query.titulo  || "").trim();
  const generosRaw = (req.query.generos || "").trim();
  const idActual   = req.query.id ? String(req.query.id) : null;
  const limit      = Math.min(parseInt(req.query.limit) || 15, 50);

  if (!tituloRaw) {
    return res.status(400).json({ error: "Falta el parámetro 'titulo'." });
  }

  const catalogo = peliculas;

  const tituloNorm = tituloRaw.toLowerCase();
  function esLaMisma(p) {
    if (idActual && p.id && String(p.id) === idActual) return true;
    if ((p.titulo || "").toLowerCase() === tituloNorm) return true;
    return false;
  }

  const palabrasClave = tituloNorm
    .split(/\s+/)
    .filter(w => w.length >= 4);

  let relacionadas = [];

  if (palabrasClave.length > 0) {
    relacionadas = catalogo.filter(p => {
      if (esLaMisma(p)) return false;
      const tP = (p.titulo || "").toLowerCase();
      return palabrasClave.some(w => tP.includes(w));
    });
  }

  const generosActuales = generosRaw
    .split(/[,|/]/)
    .map(g => g.trim().toLowerCase())
    .filter(Boolean);

  if (relacionadas.length < limit && generosActuales.length > 0) {
    const yaIncluidas = new Set(relacionadas.map(p => p.id ? String(p.id) : (p.titulo || "").toLowerCase()));

    const porGenero = catalogo.filter(p => {
      if (esLaMisma(p)) return false;
      const clave = p.id ? String(p.id) : (p.titulo || "").toLowerCase();
      if (yaIncluidas.has(clave)) return false;
      const genP = (p.generos || "").toLowerCase();
      return generosActuales.some(g => genP.includes(g));
    });

    shuffleArray(porGenero);
    relacionadas = [...relacionadas, ...porGenero].slice(0, limit);
  }

  if (relacionadas.length < limit) {
    const yaIncluidas = new Set(relacionadas.map(p => p.id ? String(p.id) : (p.titulo || "").toLowerCase()));

    const resto = catalogo.filter(p => {
      if (esLaMisma(p)) return false;
      const clave = p.id ? String(p.id) : (p.titulo || "").toLowerCase();
      return !yaIncluidas.has(clave);
    });

    shuffleArray(resto);
    relacionadas = [...relacionadas, ...resto].slice(0, limit);
  }

  shuffleArray(relacionadas);
  const resultados = relacionadas.slice(0, limit);

  console.log(`🎬 [/sugerencias] titulo="${tituloRaw}" → ${resultados.length} sugerencia(s) devuelta(s).`);

  res.json({
    fuente: "sugerencias",
    total: resultados.length,
    resultados
  });
});

app.get("/pelicula/url", async (req, res) => {
  const id = req.query.id;
  if (!id) return res.status(400).json({ error: "Falta parámetro id" });

  try {
    console.log(`📡 [/pelicula/url] Consultando API externa para id=${id} — esperando respuesta completa...`);
    const resp = await fetchWithTimeout(EXTERNAL_API_URL);
    if (!resp.ok) {
      console.warn(`⚠️ [/pelicula/url] API externa respondió con ${resp.status || "timeout"}.`);
      return res.status(503).json({ error: "La API externa no está disponible. Intenta de nuevo más tarde." });
    }
    const data = await resp.json();
    const pelicula = data.find(p => String(p.id) === String(id));
    if (!pelicula) {
      return res.status(404).json({ error: "Película no encontrada en la API externa." });
    }
    res.json({
      id: pelicula.id,
      titulo: pelicula.titulo,
      imagen_url: pelicula.imagen_url || "",
      pelicula_url: pelicula.pelicula_url
    });
  } catch (err) {
    console.warn(`⚠️ [/pelicula/url] Error inesperado: ${err.message}`);
    res.status(503).json({ error: "La API externa no está disponible. Intenta de nuevo más tarde." });
  }
});


// ------------------- RUTAS DE USUARIOS -------------------
app.get("/user/get", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  if (!email) return res.status(400).json({ error: "Falta parámetro email" });
  res.json(getOrCreateUser(email));
});

app.get("/user/setplan", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const tipoPlan = req.query.tipoPlan;
  const credits = req.query.credits ? parseInt(req.query.credits) : undefined;
  if (!email || !tipoPlan) return res.status(400).json({ error: "Falta email o tipoPlan" });

  const user = getOrCreateUser(email);
  user.tipoPlan = tipoPlan;
  if (typeof credits === "number") user.credits = credits;
  saveUser(email, user);
  res.json({ ok: true, user });
});

app.get("/user/add_favorite", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const { titulo, imagen_url, pelicula_url: raw_pelicula_url, api_id } = req.query;

  const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);

  if (!email || !titulo || (!pelicula_url && !api_id))
    return res.status(400).json({ error: "Faltan parámetros" });

  const user = getOrCreateUser(email);

  if (api_id) {
    if (!user.favorites.some(f => String(f.api_id) === String(api_id))) {
      user.favorites.unshift({
        titulo,
        imagen_url,
        api_id: String(api_id),
        _fromExternalApi: true,
        addedAt: new Date().toISOString()
      });
      saveUser(email, user);
    }
  } else {
    if (!user.favorites.some(f => f.pelicula_url === pelicula_url)) {
      user.favorites.unshift({ titulo, imagen_url, pelicula_url, addedAt: new Date().toISOString() });
      saveUser(email, user);
    }
  }

  res.json({ ok: true, favorites: user.favorites });
});

app.get("/user/favorites", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  if (!email) return res.status(400).json({ error: "Falta email" });
  const user = getOrCreateUser(email);
  res.json({ total: user.favorites.length, favorites: user.favorites });
});

app.get("/user/favorites/clear", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  if (!email) return res.status(400).json({ error: "Falta email" });

  const user = getOrCreateUser(email);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado" });

  user.favorites = [];
  saveUser(email, user);

  res.json({ ok: true, message: "Lista de favoritos eliminada." });
});

app.get("/user/favorites/remove", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const api_id = req.query.api_id;
  const raw_pelicula_url = req.query.pelicula_url;
  const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);

  if (!email || (!pelicula_url && !api_id))
    return res.status(400).json({ error: "Faltan email o pelicula_url/api_id" });

  const user = getOrCreateUser(email);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado" });

  const initialLength = user.favorites.length;

  if (api_id) {
    user.favorites = user.favorites.filter(f => String(f.api_id) !== String(api_id));
  } else {
    user.favorites = user.favorites.filter(f => f.pelicula_url !== pelicula_url);
  }

  if (user.favorites.length < initialLength) {
    saveUser(email, user);
    return res.json({ ok: true, message: "Película eliminada de favoritos." });
  }
  res.status(404).json({ ok: false, message: "Película no encontrada en favoritos." });
});

app.get("/user/add_history", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const { titulo, pelicula_url: raw_pelicula_url, imagen_url, api_id } = req.query;

  const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);

  if (!email || !titulo || (!pelicula_url && !api_id))
    return res.status(400).json({ error: "Faltan parámetros" });

  const user = getOrCreateUser(email);

  if (api_id) {
    user.history = user.history.filter(h => String(h.api_id) !== String(api_id));
    user.history.unshift({
      titulo,
      api_id: String(api_id),
      imagen_url,
      _fromExternalApi: true,
      fecha: new Date().toISOString()
    });
  } else {
    user.history = user.history.filter(h => h.pelicula_url !== pelicula_url);
    user.history.unshift({ titulo, pelicula_url, imagen_url, fecha: new Date().toISOString() });
  }

  if (user.history.length > 200) user.history = user.history.slice(0, 200);
  saveUser(email, user);

  registrarVistaHoy({ titulo, imagen_url, pelicula_url, api_id });

  res.json({ ok: true, total: user.history.length });
});

app.get("/user/history", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  if (!email) return res.status(400).json({ error: "Falta email" });
  const user = getOrCreateUser(email);
  res.json({ total: user.history.length, history: user.history });
});

app.get("/user/history/clear", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  if (!email) return res.status(400).json({ error: "Falta email" });

  const user = getOrCreateUser(email);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado" });

  user.history = [];
  saveUser(email, user);

  res.json({ ok: true, message: "Historial de películas eliminado." });
});

app.get("/user/history/remove", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const api_id = req.query.api_id;
  const raw_pelicula_url = req.query.pelicula_url;
  const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);

  if (!email || (!pelicula_url && !api_id))
    return res.status(400).json({ error: "Faltan email o pelicula_url/api_id" });

  const user = getOrCreateUser(email);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado" });

  const initialLength = user.history.length;

  if (api_id) {
    user.history = user.history.filter(h => String(h.api_id) !== String(api_id));
  } else {
    user.history = user.history.filter(h => h.pelicula_url !== pelicula_url);
  }

  if (user.history.length < initialLength) {
    saveUser(email, user);
    return res.json({ ok: true, message: "Película eliminada del historial." });
  }
  res.status(404).json({ ok: false, message: "Película no encontrada en el historial." });
});


// ------------------- NUEVOS ENDPOINTS -------------------

app.get("/user/history/refresh", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const user = getOrCreateUser(email);
  if (!user) return res.status(400).json({ error: "Usuario no encontrado" });
  res.json({ ok: true, refreshed: user.history });
});

app.get("/user/favorites/refresh", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const user = getOrCreateUser(email);
  if (!user) return res.status(400).json({ error: "Usuario no encontrado" });
  res.json({ ok: true, refreshed: user.favorites });
});

app.get("/user/profile", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const user = getOrCreateUser(email);
  if (!user) return res.status(400).json({ error: "Usuario no encontrado" });

  const perfil = {
    email: user.email,
    tipoPlan: user.tipoPlan,
    credits: user.credits,
    totalFavoritos: user.favorites.length,
    totalHistorial: user.history.length,
    ultimaActividad:
      user.history[0]?.fecha || user.favorites[0]?.addedAt || "Sin actividad",
    ultimaActividadHeartbeat: user.lastActivityTimestamp || "Sin latidos",
  };
  res.json({ perfil });
});

app.get("/user/activity", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const user = getOrCreateUser(email);
  if (!user) return res.status(400).json({ error: "Usuario no encontrado" });

  const historial = user.history.map(h => ({
    tipo: "historial",
    titulo: h.titulo,
    fecha: h.fecha
  }));
  const favoritos = user.favorites.map(f => ({
    tipo: "favorito",
    titulo: f.titulo,
    fecha: f.addedAt
  }));

  const resumen = Object.values(user.resume).map(r => ({
    tipo: "reproduccion_resumen",
    titulo: r.titulo,
    fecha: r.lastHeartbeat,
    progreso: `${Math.round((r.currentTime / r.totalDuration) * 100)}%`,
    vistaCompleta: r.isComplete,
  }));

  const actividad = [...historial, ...favoritos, ...resumen].sort(
    (a, b) => new Date(b.fecha) - new Date(a.fecha)
  );

  res.json({ total: actividad.length, actividad });
});


// ------------------- ENDPOINTS DE SEGUIMIENTO DE STREAMING (LATIDOS) -------------------

app.get("/user/heartbeat", (req, res) => {
    const email = (req.query.email || "").toLowerCase();
    const raw_pelicula_url = req.query.pelicula_url;
    const api_id = req.query.api_id;
    const currentTime = parseInt(req.query.currentTime);
    const totalDuration = parseInt(req.query.totalDuration);
    const titulo = req.query.titulo;

    const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);
    const key = api_id ? `api:${api_id}` : pelicula_url;

    if (!email || !key || isNaN(currentTime) || isNaN(totalDuration) || !titulo) {
        return res.status(400).json({ error: "Faltan parámetros válidos (email, pelicula_url o api_id, currentTime, totalDuration, titulo)." });
    }

    const user = getOrCreateUser(email);
    user.lastActivityTimestamp = new Date().toISOString();

    const percentage = (currentTime / totalDuration) * 100;
    const IS_COMPLETE_THRESHOLD = 90;
    const isComplete = percentage >= IS_COMPLETE_THRESHOLD;

    user.resume[key] = {
        titulo: titulo,
        pelicula_url: pelicula_url,
        api_id: api_id ? String(api_id) : null,
        _fromExternalApi: !!api_id,
        currentTime: currentTime,
        totalDuration: totalDuration,
        percentage: Math.round(percentage),
        isComplete: isComplete,
        lastHeartbeat: new Date().toISOString()
    };

    saveUser(email, user);

    res.json({
        ok: true,
        message: "Latido registrado.",
        progress: user.resume[key]
    });
});

app.get("/user/consume_credit", (req, res) => {
    const email = (req.query.email || "").toLowerCase();
    const raw_pelicula_url = req.query.pelicula_url;
    const api_id = req.query.api_id;

    const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);
    const key = api_id ? `api:${api_id}` : pelicula_url;

    if (!email || !key) {
        return res.status(400).json({ error: "Faltan parámetros (email, pelicula_url o api_id)." });
    }

    const user = getOrCreateUser(email);

    if (user.tipoPlan !== 'creditos') {
        return res.json({
            ok: true,
            consumed: false,
            message: `El plan del usuario es '${user.tipoPlan}', no se requiere consumo de crédito.`
        });
    }

    const resumeEntry = user.resume[key];

    if (!resumeEntry) {
        return res.status(404).json({
            ok: false,
            consumed: false,
            message: "No se encontró el resumen de reproducción para esta película."
        });
    }

    if (!resumeEntry.isComplete) {
        return res.json({
            ok: false,
            consumed: false,
            progress: resumeEntry.percentage,
            message: "La película no ha sido vista completamente (requiere >90%)."
        });
    }

    if (user.credits <= 0) {
        return res.json({
            ok: false,
            consumed: false,
            message: "Créditos insuficientes."
        });
    }

    user.credits -= 1;
    resumeEntry.creditConsumed = true;
    saveUser(email, user);

    res.json({
        ok: true,
        consumed: true,
        remaining_credits: user.credits,
        message: "Crédito consumido exitosamente. La película se marcó como vista completa."
    });
});


// ------------------- INICIAR SERVIDOR -------------------
const PORT = process.env.PORT || 8080;

app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Servidor corriendo en http://0.0.0.0:${PORT}`);
  console.log(`📚 Catálogo local disponible de inmediato: ${localPeliculas.length} películas`);
  console.log(`🔀 Proxy/Pasarela activo: /stream, /archive-stream, /thumb, /ytthumb, /search, /catalog, /health → Python :8081`);
  console.log(`⏳ Sincronizando datos en segundo plano (GitHub + API externa)...`);
});

setImmediate(() => {
  loadUsersDataFromGitHub()
    .then(() => console.log("✅ [Background] Datos de usuario sincronizados desde GitHub."))
    .catch(err => console.error("❌ [Background] Error al cargar datos de GitHub:", err.message));
});

setImmediate(() => {
  loadExternalApiMovies()
    .then(() => console.log("✅ [Background] Catálogo externo Peliprex cargado correctamente."))
    .catch(err => console.error("❌ [Background] Error al cargar catálogo externo:", err.message));
});

setImmediate(() => {
  loadVistosHoyFromGitHub()
    .then(() => console.log("✅ [Background] Datos de 'Lo más visto hoy' sincronizados desde GitHub."))
    .catch(err => console.error("❌ [Background] Error al cargar 'Lo más visto hoy' desde GitHub:", err.message));
});
