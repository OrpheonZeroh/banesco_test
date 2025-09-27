# Usar imagen oficial de Python 3.11 slim para menor tamaño
FROM python:3.11-slim

# Establecer metadatos
LABEL maintainer="ETL Team"
LABEL description="FastAPI ETL Dashboard para integrar usuarios, pedidos y clima"
LABEL version="1.0"

# Establecer directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements.txt primero para aprovechar cache de Docker
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar archivos de aplicación
COPY app.py main.py orders.csv ./
COPY static/ ./static/

# Crear directorio para salida
RUN mkdir -p /app/output

# Crear usuario no-root para seguridad
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Exponer puerto
EXPOSE $PORT

# Healthcheck básico
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:$PORT/status || exit 1

# Comando por defecto para Railway
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port $PORT"]
