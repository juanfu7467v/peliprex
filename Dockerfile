# ==========================================
# 1. ENTORNO BASE HÍBRIDO (Node.js 22 + Python 3.11)
# ==========================================
FROM node:22.21.1-slim AS base

LABEL fly_launch_runtime="Node.js + Python Streaming"
WORKDIR /app
ENV NODE_ENV="production"

# Evitar archivos .pyc y forzar logs en tiempo real para Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instalación de dependencias del sistema indispensables para ambos (Herramientas de compilación + FFmpeg)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    node-gyp \
    pkg-config \
    python3 \
    python3-pip \
    python3-dev \
    python3-venv \
    gcc \
    g++ \
    libjpeg-dev \
    zlib1g-dev \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio de datos para la caché persistente que requiere Python
RUN mkdir -p /data && chmod 777 /data


# ==========================================
# 2. CONSTRUCCIÓN Y DEPENDENCIAS DE NODE.JS (Backend)
# ==========================================
FROM base AS node_build
WORKDIR /app/backend
COPY backend/package.json ./
RUN npm install
COPY backend/ ./


# ==========================================
# 3. ENTORNO FINAL DE EJECUCIÓN (Híbrido)
# ==========================================
FROM base AS final
WORKDIR /app

# Copiamos el backend de Node preparado en la etapa anterior
COPY --from=node_build /app/backend ./backend

# Configuración e instalación de dependencias de Python (Streaming)
WORKDIR /app/streaming
COPY streaming/requirements.txt ./
# Creamos un entorno virtual para aislar las librerías de Python de manera segura
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
COPY streaming/ ./


# ==========================================
# 4. CONFIGURACIÓN DE RED Y PROCESOS
# ==========================================
WORKDIR /app

# Exponemos el puerto 8080 que es el que tu actual fly.toml usa para dar la cara a internet (Node.js)
EXPOSE 8080

# Comando final: 
# Lanzamos Python (Streaming) en segundo plano usando el puerto interno 8001 para que no choque con Node.
# Luego entramos a la carpeta de Node y ejecutamos el servidor principal en el puerto 8080.
CMD ["sh", "-c", "python3 /app/streaming/main.py --port 8081 & cd /app/backend && npm run start"]
