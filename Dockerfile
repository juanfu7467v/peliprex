
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

# Instalación de dependencias del sistema indispensables para ambos
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

# Instalar PM2 globalmente
RUN npm install -g pm2

# ==========================================
# 2. CONSTRUCCIÓN Y DEPENDENCIAS DE NODE.JS (Backend)
# ==========================================
FROM base AS node_build
WORKDIR /app/backend
COPY backend/package.json ./
RUN npm install --production
COPY backend/ ./

# ==========================================
# 3. ENTORNO FINAL DE EJECUCIÓN (Híbrido)
# ==========================================
FROM base AS final
WORKDIR /app

# Crear directorio de datos para la caché persistente
RUN mkdir -p /data && chmod 777 /data

# Copiamos el backend de Node preparado
COPY --from=node_build /app/backend ./backend

# Configuración e instalación de dependencias de Python (Streaming)
WORKDIR /app/streaming
COPY streaming/requirements.txt ./
# Instalamos dependencias directamente en el sistema de la imagen final para simplicidad con PM2
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
COPY streaming/ ./

# Volvemos a la raíz para la configuración de PM2
WORKDIR /app
COPY ecosystem.config.js ./ecosystem.config.js

# Exponemos el puerto 8080 (Backend) y 8081 (Streaming)
EXPOSE 8080
EXPOSE 8081

# Comando final: Iniciar con PM2 para gestión robusta de procesos
CMD ["pm2-runtime", "start", "ecosystem.config.js"]
