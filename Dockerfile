
# Etapa 1: Construir el backend (Node.js)
FROM node:22.21.1-slim AS backend_builder
WORKDIR /app/backend
COPY backend/package.json backend/package-lock.json ./ # Copiar solo los archivos de dependencias
RUN npm install --production

# Etapa 2: Construir el servicio de streaming (Python)
FROM python:3.11-slim AS streaming_builder
WORKDIR /app/streaming
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# Instalar dependencias del sistema para Pillow y ffmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY streaming/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Etapa 3: Imagen final (unificada)
FROM debian:bookworm-slim

# Instalar Node.js y Python en la imagen final
RUN apt-get update && apt-get install -y --no-install-recommends \
    nodejs \
    npm \
    python3 \
    python3-pip \
    ffmpeg \
    # Dependencias de Pillow para runtime
    libjpeg62-turbo \
    zlib1g \
    # Para el proceso de arranque (supervisord)
    supervisor \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar el backend construido
COPY --from=backend_builder /app/backend /app/backend

# Copiar el streaming construido
COPY --from=streaming_builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY streaming /app/streaming

# Crear el directorio /data para persistencia
RUN mkdir -p /data && chmod 777 /data

# Configurar supervisord para ejecutar ambos servicios
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Exponer los puertos
EXPOSE 8080
EXPOSE 8081

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
