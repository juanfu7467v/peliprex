// index.js — API completa de Películas con sistema de usuarios + MaguisTV style
// ¡MEJORADO con Respaldo en GitHub para Historial y Favoritos!
// ¡MEJORADO con integración de API externa Peliprex para ampliar el catálogo!

import express from "express";
import cors from "cors";
import fs from "fs";
import fetch from "node-fetch";
import path from "path";

const app = express();
app.use(cors());

// ------------------- GITHUB CONFIGURACIÓN DE RESPALDO -------------------
// ¡ASEGÚRATE de configurar las variables de entorno GITHUB_TOKEN y GITHUB_REPO!
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_REPO = process.env.GITHUB_REPO; // Formato: 'usuario/nombre-del-repositorio'
const BACKUP_FILE_NAME = "users_data.json";
const PELIS_FILE_NAME = "peliculas.json"; // 🆕 Nombre del archivo de películas en GitHub

// ------------------- 🆕 API EXTERNA PELIPREX -------------------
// Las URLs de esta API son temporales (expiran). Por eso:
// - Se guardan los IDs, no las URLs, en historial/favoritos.
// - Se refresca el catálogo cada 2 horas para mantener URLs válidas en memoria.
const EXTERNAL_API_BASE = "https://peliprex-31wrsa.fly.dev";
const EXTERNAL_API_URL = `${EXTERNAL_API_BASE}/catalog`;
const EXTERNAL_API_SEARCH = `${EXTERNAL_API_BASE}/search`;
const EXTERNAL_API_REFRESH_MS = 2 * 60 * 60 * 1000; // Refrescar cada 2 horas

// ⏱️ Tiempo máximo de espera para la API externa.
// La API puede tardar varios segundos bajo carga — se espera TODO el tiempo necesario.
// Si responde [] significa que no hay resultados (válido). Solo se corta si supera este límite.
const EXTERNAL_API_TIMEOUT_MS = 120_000; // 2 minutos — espera generosa para alta carga

// 📂 Archivos locales (Mantenidos)
const PELIS_FILE = path.join(process.cwd(), "peliculas.json");
const USERS_FILE = path.join(process.cwd(), BACKUP_FILE_NAME);

// ------------------- 🆕 "LO MÁS VISTO HOY" -------------------
const VISTO_HOY_FILE_NAME = "visto_hoy.json";           // Archivo GitHub
const VISTO_HOY_RESET_MS  = 12 * 60 * 60 * 1000;        // 12 horas en ms

/**
 * Almacén en memoria de vistas del día actual.
 * Clave: pelicula_url limpia (local) o "api:<api_id>" (externa).
 * Valor: { titulo, imagen_url, pelicula_url|null, api_id|null, count, lastSeen }
 */
let vistosHoy = {};
let vistosHoyResetAt = Date.now(); // Marca de cuándo empezó el periodo actual

// ------------------- FUNCIONES AUXILIARES -------------------

/** Limpia la URL de la película eliminando la duplicidad '/prepreview' para corregir a '/preview'. */
function cleanPeliculaUrl(url) {
  if (!url) return url;
  return url.replace(/\/prepreview([?#]|$)/, '/preview$1');
}

/** Devuelve un array con elementos aleatorios y desordenados. */
function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

/**
 * 🛡️ Fetch resiliente con timeout mediante AbortController.
 *
 * COMPORTAMIENTO IMPORTANTE:
 * - Si la API responde con [] → resultado válido (sin películas). Se retorna inmediatamente.
 * - Si la API aún no ha respondido → se sigue esperando. NO se interpreta como fallo.
 * - Solo se cancela si supera EXTERNAL_API_TIMEOUT_MS (2 minutos) sin ninguna respuesta.
 * - Si hay error de red, timeout o 502/504 → devuelve { ok: false } sin lanzar excepción.
 * - El llamador decide qué hacer con la respuesta.
 */
async function fetchWithTimeout(url, options = {}, timeoutMs = EXTERNAL_API_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(timer);
    return resp;
  } catch (err) {
    clearTimeout(timer);
    // AbortError = timeout superado; FetchError = red caída
    const reason = err.name === "AbortError" ? `timeout (>${timeoutMs / 1000}s sin respuesta)` : err.message;
    console.warn(`⚠️ [API Externa] Petición fallida silenciosamente: ${reason}`);
    // Devolvemos un objeto "falso" con ok:false para que el llamador lo trate igual que un 5xx
    return { ok: false, status: 0, _timedOut: true };
  }
}

/**
 * 🛡️ Realiza una búsqueda resiliente en la API externa Peliprex.
 * Acepta parámetros: q, genre, year.
 *
 * COMPORTAMIENTO:
 * - Espera la respuesta completa de la API, sin importar cuánto tarde (hasta 2 minutos).
 * - Si la API responde [] → no hay resultados, se devuelve [] correctamente.
 * - Si la API devuelve 502, 504 o supera el timeout → devuelve [] silenciosamente.
 * - En ningún caso se corta la espera antes de recibir una respuesta válida.
 */
async function fetchExternalSearch(params = {}) {
  try {
    const url = new URL(EXTERNAL_API_SEARCH);
    if (params.q)     url.searchParams.set("q",     params.q);
    if (params.genre) url.searchParams.set("genre", params.genre);
    if (params.year)  url.searchParams.set("year",  params.year);

    console.log(`🔍 [API Externa] Consultando búsqueda: ${url.toString()} — esperando respuesta...`);
    const resp = await fetchWithTimeout(url.toString());

    // 502, 504 u otro error HTTP → fallo silencioso, usar solo datos locales
    if (!resp.ok) {
      if (resp.status === 502 || resp.status === 504) {
        console.warn(`⚠️ [API Externa] Error ${resp.status} en búsqueda — usando solo resultados locales.`);
      } else if (resp.status !== 0) {
        console.warn(`⚠️ [API Externa] HTTP ${resp.status} en búsqueda — usando solo resultados locales.`);
      }
      // status 0 = timeout ya logueado por fetchWithTimeout
      return [];
    }

    const data = await resp.json();
    // Array vacío [] es una respuesta válida: significa "sin resultados"
    if (!Array.isArray(data)) {
      console.warn(`⚠️ [API Externa] Respuesta inesperada (no es array) en búsqueda.`);
      return [];
    }
    console.log(`✅ [API Externa] Búsqueda respondió con ${data.length} resultado(s).`);
    // Marcar cada resultado como proveniente de la API externa
    return data.map(p => ({ ...p, _fromExternalApi: true }));
  } catch (err) {
    // Captura cualquier error inesperado (p.ej. JSON malformado)
    console.warn(`⚠️ [API Externa] Error inesperado en búsqueda: ${err.message} — usando solo resultados locales.`);
    return [];
  }
}

/**
 * Combina resultados locales y externos eliminando duplicados por id o titulo.
 * Los resultados externos ya vienen marcados con _fromExternalApi: true.
 */
function mergeResults(localResults, externalResults) {
  const combined = [...localResults];
  const localIds = new Set(localResults.map(p => String(p.id)).filter(Boolean));
  const localTitles = new Set(localResults.map(p => (p.titulo || "").toLowerCase()));

  for (const ext of externalResults) {
    const extId = ext.id ? String(ext.id) : null;
    const extTitle = (ext.titulo || "").toLowerCase();
    // Evitar duplicados por id o por título exacto
    if (extId && localIds.has(extId)) continue;
    if (localTitles.has(extTitle)) continue;
    combined.push(ext);
  }
  return combined;
}


/**
 * 💾 Fusiona las películas externas nuevas en localPeliculas y las persiste en peliculas.json
 * tanto en disco local como en GitHub.
 *
 * COMPORTAMIENTO:
 * - Solo agrega películas que NO existan ya en localPeliculas (por id o por título exacto).
 * - Antes de guardar, elimina el campo _fromExternalApi para que queden como películas locales.
 * - Si no hay películas nuevas, no escribe el archivo (evita escrituras innecesarias).
 * - Actualiza localPeliculas en memoria para que las búsquedas locales las incluyan de inmediato.
 * - Tras guardar en disco, dispara el respaldo en GitHub de forma asíncrona (fire-and-forget).
 */
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
      // Guardar sin el marcador interno _fromExternalApi
      const { _fromExternalApi, ...rest } = p;
      return rest;
    });

    if (nuevas.length === 0) {
      console.log("ℹ️ [peliculas.json] Sin películas nuevas de la API externa para guardar.");
      return;
    }

    // Actualizar la lista en memoria
    localPeliculas = [...localPeliculas, ...nuevas];

    // Persistir en disco local
    const content = JSON.stringify(localPeliculas, null, 2);
    fs.writeFileSync(PELIS_FILE, content, "utf8");

    console.log(`💾 [peliculas.json] ${nuevas.length} película(s) nueva(s) guardadas localmente. Total local: ${localPeliculas.length}`);

    // 🆕 Guardar también en GitHub de forma asíncrona (fire-and-forget)
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

/** Obtiene el SHA de la última versión del archivo en GitHub, necesario para actualizar. */
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

/** Guarda los datos de usuario en GitHub. */
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

/**
 * 🆕 Guarda el catálogo de películas (peliculas.json) en GitHub.
 * Usa las mismas variables de entorno GITHUB_TOKEN y GITHUB_REPO que users_data.json.
 */
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

/**
 * Registra o incrementa la vista de una película en el contador del día.
 * Se llama cada vez que se agrega una entrada al historial de cualquier usuario.
 */
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

/**
 * Devuelve el array de películas más vistas hoy, ordenadas de mayor a menor vistas.
 */
function getVistosHoyArray() {
  return Object.values(vistosHoy)
    .filter(v => v.count >= 1)
    .sort((a, b) => b.count - a.count);
}

/**
 * Guarda el snapshot actual de "visto hoy" en GitHub como visto_hoy.json.
 */
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

/**
 * Carga el snapshot de "visto hoy" desde GitHub al arrancar.
 * Si tiene mas de 12 horas, se descarta (datos expirados).
 */
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

/**
 * Reset automatico cada 12 horas:
 * 1. Guarda snapshot en GitHub.
 * 2. Limpia el contador en memoria.
 * 3. Restablece la marca de tiempo del nuevo periodo.
 */
setInterval(async () => {
  console.log("Reseteando conteo de 'Lo mas visto hoy' (12 horas cumplidas)...");
  await saveVistosHoyToGitHub();
  vistosHoy = {};
  vistosHoyResetAt = Date.now();
  console.log("\u2705 [visto_hoy] Conteo reiniciado. Nuevo periodo comenzando.");
}, VISTO_HOY_RESET_MS);


/** Carga los datos de usuario desde GitHub al iniciar el servidor. */
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

/**
 * 🛡️ Carga y refresca las películas desde la API externa Peliprex de forma resiliente.
 *
 * COMPORTAMIENTO:
 * - Espera la respuesta completa (hasta 2 minutos). No se corta antes.
 * - Si la API responde [] → catálogo externo vacío. Válido, se actualiza en memoria.
 * - Si la API falla (502, 504, timeout real) → se mantiene la caché anterior sin romper el servidor.
 * - Marca cada película con _fromExternalApi: true.
 */
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
      // status 0 = timeout ya logueado por fetchWithTimeout
      return externalApiMovies;
    }

    const data = await resp.json();
    if (!Array.isArray(data)) {
      console.warn(`⚠️ [API Externa] Respuesta inesperada al cargar catálogo (no es array) — manteniendo caché anterior.`);
      return externalApiMovies;
    }
    // Marcar cada película como proveniente de la API externa
    externalApiMovies = data.map(p => ({ ...p, _fromExternalApi: true }));
    // 💾 Guardar automáticamente las películas nuevas en peliculas.json (local + GitHub)
    mergeExternalIntoPeliculasFile(externalApiMovies);
    // Reconstruir el catálogo combinado con las URLs frescas
    peliculas = [...localPeliculas, ...externalApiMovies];
    console.log(`✅ Cargadas ${externalApiMovies.length} películas desde API externa Peliprex`);
    console.log(`📚 Catálogo total actualizado: ${peliculas.length} películas`);
    return externalApiMovies;
  } catch (err) {
    console.warn(`⚠️ [API Externa] Error inesperado al cargar catálogo: ${err.message} — manteniendo caché anterior.`);
    // En caso de error, devolver la caché actual sin romper el servidor
    return externalApiMovies;
  }
}

// Catálogo combinado: comienza con las locales, las externas se añaden al arrancar.
let peliculas = [...localPeliculas];

// Refresco periódico del catálogo externo para renovar las URLs que expiran.
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
// 🔧 Aumentado a 3 minutos para reducir reinicios en frío en Fly.io
let ultimaPeticion = Date.now();
const TIEMPO_INACTIVIDAD = 3 * 60 * 1000; // 3 minutos

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
}, 60 * 1000); // Verificar cada 1 minuto

app.use((req, res, next) => {
  ultimaPeticion = Date.now();
  next();
});


// ------------------- TAREA PROGRAMADA: ELIMINACIÓN DE ACTIVIDAD CADA 24 HRS -------------------
/**
 * Tarea programada para limpiar historial y resumen de películas
 * que tienen más de 24 horas de la última actividad/latido.
 */
const MS_IN_24_HOURS = 24 * 60 * 60 * 1000;

setInterval(() => {
    console.log("🧹 Iniciando chequeo de limpieza de actividad de 24 horas...");
    const data = readUsersData();
    let usersModified = false;
    const now = Date.now();

    for (const email in data.users) {
        const user = data.users[email];
        let userActivityModified = false;

        // --- Limpieza de Historial ---
        const historyLengthBefore = user.history.length;
        user.history = user.history.filter(h => {
            const historyDate = new Date(h.fecha).getTime();
            return now - historyDate < MS_IN_24_HOURS;
        });
        if (user.history.length !== historyLengthBefore) {
            console.log(`   [${email}] Historial: Eliminados ${historyLengthBefore - user.history.length} elementos por antigüedad (>24h).`);
            userActivityModified = true;
        }

        // --- Limpieza de Resumen de Reproducción ---
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


// ------------------- RUTAS PRINCIPALES -------------------
app.get("/", (req, res) => {
  res.json({
    mensaje: "🎬 API de Películas funcionando correctamente",
    total: peliculas.length,
    ejemplo: "/peliculas o /peliculas/El%20Padrino"
  });
});

app.get("/peliculas", (req, res) => res.json(peliculas));

// 🔎 Búsqueda por nombre/título — combina peliculas.json + API externa en paralelo
// 🛡️ Resiliente: espera la respuesta completa de la API externa antes de responder.
// Si la API externa falla definitivamente, devuelve solo resultados locales sin error.
app.get("/peliculas/:titulo", async (req, res) => {
  const tituloRaw = decodeURIComponent(req.params.titulo || "");
  const titulo = tituloRaw.toLowerCase();

  // Buscar en local y en API externa en paralelo
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

// 🔎 Búsqueda avanzada — combina peliculas.json + API externa
// 🛡️ Resiliente: espera la respuesta completa de la API externa antes de responder.
// Si la API externa falla definitivamente, devuelve solo resultados locales sin error.
app.get("/buscar", async (req, res) => {
  const { año, genero, idioma, desde, hasta, q } = req.query;

  // --- Búsqueda local ---
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

  // --- Construir parámetros para la API externa ---
  const externalParams = {};
  if (q)      externalParams.q     = q;
  if (genero) externalParams.genre = genero;
  if (año)    externalParams.year  = año;
  // Para búsqueda por palabra clave sin q, usar genero o año como query
  if (!q && genero) externalParams.q = genero;
  if (!q && !genero && desde) externalParams.q = desde;

  // --- Esperar la respuesta completa de la API externa antes de responder al usuario ---
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

// 🆕 ENDPOINT: Búsqueda por Categoría (Género) — solo peliculas.json (sin API externa)
app.get("/peliculas/categoria/:genero", (req, res) => {
    const generoRaw = decodeURIComponent(req.params.genero || "");
    const generoBuscado = generoRaw.toLowerCase();

    // Filtrar únicamente desde peliculas.json (sin llamada a API externa)
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


// ------------------- 🆕 ENDPOINT: /nuevo — Catálogo completo de API externa -------------------
/**
 * Obtiene y sirve el catálogo completo de la API externa tal como llega.
 *
 * COMPORTAMIENTO:
 * - Espera la respuesta completa (hasta 2 minutos). No se corta antes de recibir respuesta.
 * - Si la API responde [] → se devuelve [] al usuario (respuesta válida, sin resultados).
 * - Si la API falla o supera el timeout → responde con error 503 claro.
 * GET /nuevo
 */
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


// ------------------- 🆕 ENDPOINT: /vistohoy — Lo más visto hoy -------------------
/**
 * Devuelve las películas más vistas en las últimas 12 horas,
 * ordenadas de mayor a menor número de vistas.
 *
 * Respuesta:
 *  {
 *    fuente: "vistohoy",
 *    periodoDesde: <ISO>,
 *    generadoEn: <ISO>,
 *    total: <n>,
 *    resultados: [ { titulo, imagen_url, pelicula_url|null, api_id|null, count, lastSeen }, ... ]
 *  }
 *
 * GET /vistohoy
 * GET /vistohoy?limit=20   (opcional, por defecto devuelve todos)
 */
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



// ------------------- 🆕 ENDPOINT: /sugerencias — "También podría gustarte" -------------------
/**
 * Devuelve hasta 15 películas relacionadas con la película que el usuario está viendo.
 *
 * LÓGICA EN CAPAS:
 *   1. Películas que comparten palabras clave del título (ej. "Rambo" → busca "Rambo" en todos los títulos).
 *   2. Si hay menos de 15, completa con películas del mismo género (excluye la actual y las ya añadidas).
 *   3. Si aún faltan, rellena con películas aleatorias del catálogo combinado.
 *   La película actual siempre queda excluida del resultado.
 *
 * Parámetros:
 *   titulo  (requerido) — título de la película que se está viendo
 *   generos (opcional)  — géneros de la película (ej. "accion,aventura"), para el fallback
 *   id      (opcional)  — id de la película actual, para excluirla con precisión
 *   limit   (opcional)  — cantidad de sugerencias a devolver (por defecto 15)
 *
 * Uso: GET /sugerencias?titulo=Rambo&generos=accion,aventura&id=123
 */
app.get("/sugerencias", (req, res) => {
  const tituloRaw  = (req.query.titulo  || "").trim();
  const generosRaw = (req.query.generos || "").trim();
  const idActual   = req.query.id ? String(req.query.id) : null;
  const limit      = Math.min(parseInt(req.query.limit) || 15, 50); // máximo 50

  if (!tituloRaw) {
    return res.status(400).json({ error: "Falta el parámetro 'titulo'." });
  }

  // Catálogo disponible en este momento (local + externas en caché)
  const catalogo = peliculas;

  // ── Helper: excluir la película actual por id o por título exacto ──
  const tituloNorm = tituloRaw.toLowerCase();
  function esLaMisma(p) {
    if (idActual && p.id && String(p.id) === idActual) return true;
    if ((p.titulo || "").toLowerCase() === tituloNorm) return true;
    return false;
  }

  // ── CAPA 1: películas que contienen palabras del título actual ──
  // Se extraen palabras de 4+ letras para evitar artículos ("los", "las", "the"…)
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

  // ── CAPA 2: fallback por género(s) ──
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

    // Mezclar para variedad y añadir los que faltan
    shuffleArray(porGenero);
    relacionadas = [...relacionadas, ...porGenero].slice(0, limit);
  }

  // ── CAPA 3: relleno aleatorio si todavía hay huecos ──
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

  // ── Mezclar la capa 1 para no mostrar siempre el mismo orden ──
  shuffleArray(relacionadas);
  const resultados = relacionadas.slice(0, limit);

  console.log(`🎬 [/sugerencias] titulo="${tituloRaw}" → ${resultados.length} sugerencia(s) devuelta(s).`);

  res.json({
    fuente: "sugerencias",
    total: resultados.length,
    resultados
  });
});


// ------------------- 🆕 ENDPOINT: Obtener URL Fresca por ID (API Externa) -------------------
/**
 * Obtiene una URL de reproducción actualizada para películas de la API externa Peliprex.
 * Las URLs expiran después de algunas horas. Este endpoint las renueva usando el ID estable.
 *
 * COMPORTAMIENTO:
 * - Espera la respuesta completa de la API (hasta 2 minutos). No se corta antes.
 * - Si la API responde, busca el ID y devuelve la URL fresca inmediatamente.
 * - Si la API falla, responde con error 503 claro.
 * Uso: GET /pelicula/url?id=<id_de_la_pelicula>
 */
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

// Favoritos
// Acepta api_id para películas de API externa (no guarda pelicula_url que expira)
app.get("/user/add_favorite", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const { titulo, imagen_url, pelicula_url: raw_pelicula_url, api_id } = req.query;

  // Para películas locales se limpia la URL; para externas se usa el api_id
  const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);

  if (!email || !titulo || (!pelicula_url && !api_id))
    return res.status(400).json({ error: "Faltan parámetros" });

  const user = getOrCreateUser(email);

  if (api_id) {
    // Película de API externa: guardar por api_id, NO guardar pelicula_url (expira)
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
    // Película local: comportamiento original sin cambios
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

// ELIMINAR TODOS LOS FAVORITOS
app.get("/user/favorites/clear", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  if (!email) return res.status(400).json({ error: "Falta email" });

  const user = getOrCreateUser(email);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado" });

  user.favorites = [];
  saveUser(email, user);

  res.json({ ok: true, message: "Lista de favoritos eliminada." });
});

// ELIMINAR UNA PELÍCULA DE FAVORITOS
// Acepta api_id para eliminar películas de API externa
app.get("/user/favorites/remove", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const api_id = req.query.api_id; // Para películas de API externa
  const raw_pelicula_url = req.query.pelicula_url;
  const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);

  if (!email || (!pelicula_url && !api_id))
    return res.status(400).json({ error: "Faltan email o pelicula_url/api_id" });

  const user = getOrCreateUser(email);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado" });

  const initialLength = user.favorites.length;

  if (api_id) {
    // Remover por api_id (películas de API externa)
    user.favorites = user.favorites.filter(f => String(f.api_id) !== String(api_id));
  } else {
    // Comportamiento original: remover por pelicula_url
    user.favorites = user.favorites.filter(f => f.pelicula_url !== pelicula_url);
  }

  if (user.favorites.length < initialLength) {
    saveUser(email, user);
    return res.json({ ok: true, message: "Película eliminada de favoritos." });
  }
  res.status(404).json({ ok: false, message: "Película no encontrada en favoritos." });
});

// Historial
// Acepta api_id para películas de API externa (no guarda pelicula_url que expira)
// ✅ FIX: Si la película ya existe en el historial, se mueve al inicio (sin duplicar).
app.get("/user/add_history", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const { titulo, pelicula_url: raw_pelicula_url, imagen_url, api_id } = req.query;

  // Para películas locales se limpia la URL; para externas se usa el api_id
  const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);

  if (!email || !titulo || (!pelicula_url && !api_id))
    return res.status(400).json({ error: "Faltan parámetros" });

  const user = getOrCreateUser(email);

  if (api_id) {
    // Película de API externa: guardar por api_id, NO guardar pelicula_url (expira)
    // Si ya existe, eliminarla de su posición actual para luego agregarla al inicio
    user.history = user.history.filter(h => String(h.api_id) !== String(api_id));
    user.history.unshift({
      titulo,
      api_id: String(api_id),
      imagen_url,
      _fromExternalApi: true,
      fecha: new Date().toISOString()
    });
  } else {
    // Película local: si ya existe, eliminarla antes de reinsertar al inicio
    user.history = user.history.filter(h => h.pelicula_url !== pelicula_url);
    user.history.unshift({ titulo, pelicula_url, imagen_url, fecha: new Date().toISOString() });
  }

  if (user.history.length > 200) user.history = user.history.slice(0, 200);
  saveUser(email, user);

  // ✅ Registrar vista en "Lo más visto hoy"
  registrarVistaHoy({ titulo, imagen_url, pelicula_url, api_id });

  res.json({ ok: true, total: user.history.length });
});

app.get("/user/history", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  if (!email) return res.status(400).json({ error: "Falta email" });
  const user = getOrCreateUser(email);
  res.json({ total: user.history.length, history: user.history });
});

// ELIMINAR TODO EL HISTORIAL
app.get("/user/history/clear", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  if (!email) return res.status(400).json({ error: "Falta email" });

  const user = getOrCreateUser(email);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado" });

  user.history = [];
  saveUser(email, user);

  res.json({ ok: true, message: "Historial de películas eliminado." });
});

// ELIMINAR UNA PELÍCULA DEL HISTORIAL
// Acepta api_id para eliminar películas de API externa
app.get("/user/history/remove", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const api_id = req.query.api_id; // Para películas de API externa
  const raw_pelicula_url = req.query.pelicula_url;
  const pelicula_url = api_id ? null : cleanPeliculaUrl(raw_pelicula_url);

  if (!email || (!pelicula_url && !api_id))
    return res.status(400).json({ error: "Faltan email o pelicula_url/api_id" });

  const user = getOrCreateUser(email);
  if (!user) return res.status(404).json({ error: "Usuario no encontrado" });

  const initialLength = user.history.length;

  if (api_id) {
    // Remover por api_id (películas de API externa)
    user.history = user.history.filter(h => String(h.api_id) !== String(api_id));
  } else {
    // Comportamiento original: remover por pelicula_url
    user.history = user.history.filter(h => h.pelicula_url !== pelicula_url);
  }

  if (user.history.length < initialLength) {
    saveUser(email, user);
    return res.json({ ok: true, message: "Película eliminada del historial." });
  }
  res.status(404).json({ ok: false, message: "Película no encontrada en el historial." });
});


// ------------------- NUEVOS ENDPOINTS -------------------

// 🔁 Refrescar historial (uno o todos)
app.get("/user/history/refresh", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const user = getOrCreateUser(email);
  if (!user) return res.status(400).json({ error: "Usuario no encontrado" });
  // Sin servicio de respaldo externo activo, se devuelve el historial actual sin cambios.
  res.json({ ok: true, refreshed: user.history });
});

// 🔁 Refrescar favoritos (uno o todos)
app.get("/user/favorites/refresh", (req, res) => {
  const email = (req.query.email || "").toLowerCase();
  const user = getOrCreateUser(email);
  if (!user) return res.status(400).json({ error: "Usuario no encontrado" });
  // Sin servicio de respaldo externo activo, se devuelve la lista actual sin cambios.
  res.json({ ok: true, refreshed: user.favorites });
});

// 📊 Perfil con estadísticas
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

// 🧾 Actividad combinada
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

/**
 * Sistema de seguimiento de latidos (heartbeat) para el progreso de streaming.
 * Acepta api_id para películas de API externa.
 * La clave del resumen será 'api:<id>' para externas o la URL para locales.
 */
app.get("/user/heartbeat", (req, res) => {
    const email = (req.query.email || "").toLowerCase();
    const raw_pelicula_url = req.query.pelicula_url;
    const api_id = req.query.api_id; // Para películas de API externa
    const currentTime = parseInt(req.query.currentTime);
    const totalDuration = parseInt(req.query.totalDuration);
    const titulo = req.query.titulo;

    // La clave del resumen: URL limpia (local) o 'api:id' (externa)
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

/**
 * Endpoint para verificar si una película ha sido vista y consumir 1 crédito.
 * Acepta api_id para películas de API externa.
 * Usa la misma clave que heartbeat para localizar el resumen.
 */
app.get("/user/consume_credit", (req, res) => {
    const email = (req.query.email || "").toLowerCase();
    const raw_pelicula_url = req.query.pelicula_url;
    const api_id = req.query.api_id; // Para películas de API externa

    // La clave debe coincidir exactamente con la usada en heartbeat
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


// ------------------- INICIAR SERVIDOR (NO BLOQUEANTE — FIX PM05) -------------------
/**
 * ✅ Arranque no bloqueante para Fly.io — elimina el error PM05.
 *
 * ORDEN CORRECTO:
 *   1. app.listen() abre el puerto INMEDIATAMENTE → Fly.io lo detecta en < 1 segundo.
 *   2. Las tareas pesadas (GitHub + API externa) se lanzan en segundo plano
 *      con setImmediate + .then()/.catch(), SIN ningún await que bloquee
 *      el hilo principal.
 *
 * El catálogo local (peliculas.json) ya está disponible desde el primer
 * instante como respaldo, por lo que la API responde de forma correcta
 * incluso antes de que termine la sincronización en segundo plano.
 */
const PORT = process.env.PORT || 8080;

// 1️⃣ Abrir el puerto de inmediato — Fly.io lo detecta en < 1 segundo
app.listen(PORT, "0.0.0.0", () => {
  console.log(`✅ Servidor corriendo en http://0.0.0.0:${PORT}`);
  console.log(`📚 Catálogo local disponible de inmediato: ${localPeliculas.length} películas`);
  console.log(`⏳ Sincronizando datos en segundo plano (GitHub + API externa)...`);
});

// 2️⃣ Cargar datos de GitHub en segundo plano (sin bloquear el arranque)
setImmediate(() => {
  loadUsersDataFromGitHub()
    .then(() => console.log("✅ [Background] Datos de usuario sincronizados desde GitHub."))
    .catch(err => console.error("❌ [Background] Error al cargar datos de GitHub:", err.message));
});

// 3️⃣ Cargar películas de API externa en segundo plano (sin bloquear el arranque)
setImmediate(() => {
  loadExternalApiMovies()
    .then(() => console.log("✅ [Background] Catálogo externo Peliprex cargado correctamente."))
    .catch(err => console.error("❌ [Background] Error al cargar catálogo externo:", err.message));
});

// 4️⃣ Cargar "Lo más visto hoy" desde GitHub en segundo plano
setImmediate(() => {
  loadVistosHoyFromGitHub()
    .then(() => console.log("✅ [Background] Datos de 'Lo más visto hoy' sincronizados desde GitHub."))
    .catch(err => console.error("❌ [Background] Error al cargar 'Lo más visto hoy' desde GitHub:", err.message));
});
