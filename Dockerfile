# Usar imagen oficial de Python 3.11 slim para menor tamaño
FROM python:3.11-slim

# Establecer metadatos
LABEL maintainer="ETL Team"
LABEL description="ETL Script para integrar usuarios, pedidos y clima"
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

# Copiar el script principal
COPY main.py .

# Crear directorio para datos de entrada y salida
RUN mkdir -p /app/data /app/output

# Crear usuario no-root para seguridad
RUN groupadd -r etluser && useradd -r -g etluser etluser
RUN chown -R etluser:etluser /app
USER etluser

# Punto de entrada por defecto
ENTRYPOINT ["python", "main.py"]

# Comando por defecto (puede ser sobrescrito)
CMD ["--orders", "/app/data/orders.csv", "--out", "/app/output/integration_output.json"]

# Exponer volúmenes para datos
VOLUME ["/app/data", "/app/output"]

# Healthcheck básico
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import aiohttp, pandas, json; print('Dependencies OK')" || exit 1
