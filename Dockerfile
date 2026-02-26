FROM python:3.11-slim

# Evitar archivos .pyc y forzar logs en tiempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

WORKDIR /app

# 1. Dependencias de sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Instalar dependencias de Python (Sintaxis robusta)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
# Usamos -r de forma explícita y separada
RUN pip install --no-cache-dir -r /app/requirements.txt

# 3. Directorio de datos
RUN mkdir -p /data && chmod 777 /data

# 4. Copiar código
COPY . .

# 5. Puerto
EXPOSE 8080

# 6. Ejecución
CMD ["python", "main.py"]
