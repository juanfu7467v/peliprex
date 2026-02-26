FROM python:3.11-slim

# Evitar archivos .pyc y forzar logs en tiempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

WORKDIR /app

# 1. Dependencias de sistema necesarias para compilación de librerías
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 3. Crear directorio de datos para la caché persistente
RUN mkdir -p /data && chmod 777 /data

# 4. Copiar el resto del código
COPY . .

# 5. Puerto
EXPOSE 8080

# 6. Ejecución
CMD ["python", "main.py"]
