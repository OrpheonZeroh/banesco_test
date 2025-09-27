# ETL Script: Usuarios + Pedidos + Clima

Script que integra datos de usuarios de JSONPlaceholder con un CSV de pedidos y datos climáticos.

## 🎯 ¿Qué hace?

Toma 3 fuentes de datos y crea un reporte unificado:
1. **Usuarios** (JSONPlaceholder API)
2. **Pedidos** (CSV)  
3. **Clima** (OpenWeatherMap API)

## ⚠️ Problema Principal: No hay conexión entre datos

El CSV **NO tiene campo Rep** para conectar con usuarios:

```csv
# CSV original
,ID,Date,Region,City,Category,Product,Qty,UnitPrice,TotalPrice
,ID07351,1-ene,East,Boston,Bars,Carrot,33,1.77,58.41
,ID07352,4-ene,East,Boston,Crackers,Wheat,87,3.49,303.63
```

```json
// Usuarios JSONPlaceholder
{"id": 1, "username": "Bret", "name": "Leanne Graham"}
{"id": 2, "username": "Antonette", "name": "Ervin Howell"}
```

**❌ No hay forma de saber qué pedidos pertenecen a qué usuario**

## ✅ Solución: Crear la conexión artificialmente

### El script INVENTA el campo Rep

```python
def synthesize_rep_if_missing(df, users_usernames):
    # users_usernames = ["Bret", "Antonette", "Samantha", ...]
    
    # Para cada pedido, asigna un username por ciudad
    city_counters = {}
    for _, row in df.iterrows():
        city = row["City"]  # "Boston"
        counter = city_counters.get(city, 0)
        
        # Asigna username en round-robin
        rep = users_usernames[counter % len(users_usernames)]
        row["Rep"] = rep  # ¡AQUÍ SE CREA EL CAMPO!
        
        city_counters[city] = counter + 1
```

### Resultado: CSV con Rep sintético

```csv
# CSV DESPUÉS del script
,ID,Date,Region,City,Category,Product,Qty,UnitPrice,TotalPrice,Rep
,ID07351,1-ene,East,Boston,Bars,Carrot,33,1.77,58.41,Bret
,ID07352,4-ene,East,Boston,Crackers,Wheat,87,3.49,303.63,Antonette
,ID07353,7-ene,West,Los Angeles,Cookies,Chip,58,1.87,108.46,Samantha
```

### Estrategia de asignación

**Por ciudad (round-robin):**
- Boston pedido 1 → `Bret`
- Boston pedido 2 → `Antonette`  
- Boston pedido 3 → `Samantha`
- Boston pedido 4 → `Bret` (reinicia)

**Los Angeles pedidos empiezan su propio contador:**
- LA pedido 1 → `Bret`
- LA pedido 2 → `Antonette`

## 🔗 Cómo funciona la correlación

### 1. Agregación de ventas por Rep creado

```python
# Suma ventas por el Rep sintético
sales_map = {
    "bret": {"total_units_sold": 120, "total_revenue": 2500.0},
    "antonette": {"total_units_sold": 87, "total_revenue": 1800.0}
}
```

### 2. Enriquecimiento usando username

```python
def enrich_user(user):
    # user = {"username": "Bret", "name": "Leanne Graham", ...}
    
    # Buscar ventas usando username como clave
    username_lower = user["username"].lower()  # "bret"
    sales = sales_map.get(username_lower, {"total_units_sold": 0, "total_revenue": 0})
    
    return {
        "username": user["username"],
        "sales_summary": sales  # Ventas del Rep sintético
    }
```

## 📋 Funciones principales

| Función | Qué hace |
|---------|----------|
| `get_users()` | Obtiene usuarios de JSONPlaceholder |
| `read_orders()` | Lee CSV y normaliza columnas |
| **`synthesize_rep_if_missing()`** | **CREA el campo Rep que no existe** |
| `aggregate_sales()` | Suma ventas por Rep |
| `get_weather_for()` | Obtiene clima por coordenadas |
| `enrich_user()` | Combina usuario + ventas + clima |

## 🚀 Uso

### Opción 1: Ejecución Local

#### Con entorno virtual (Recomendado)

```bash
# Crear y activar entorno virtual
python -m venv .venv

# Activar entorno virtual
# En macOS/Linux:
source .venv/bin/activate
# En Windows:
# .venv\Scripts\activate

# Instalar dependencias
pip install aiohttp pandas python-dotenv

# Ejecutar script
python etl_script.py --orders orders.csv --out integration_output.json

# Con API key de clima (opcional)
export OPENWEATHER_API_KEY="your_key"
python etl_script.py --orders orders.csv

# Desactivar entorno virtual cuando termines
deactivate
```

#### Sin entorno virtual (No recomendado)

```bash
# Instalar dependencias globalmente
pip install aiohttp pandas python-dotenv

# Ejecutar
python etl_script.py --orders orders.csv --out integration_output.json
```

### Opción 2: Docker (Recomendado)

#### Preparación de archivos

```bash
# Estructura de directorios
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── etl_script.py
├── data/
│   └── orders.csv          # Tu archivo CSV aquí
├── output/                 # Resultados aparecerán aquí
└── .env                    # Variables de entorno (opcional)
```

#### Usar Docker Compose (Más fácil)

```bash
# 1. Crear directorios
mkdir -p data output

# 2. Copiar tu CSV
cp orders.csv data/

# 3. Crear .env (opcional)
echo "OPENWEATHER_API_KEY=your_api_key_here" > .env

# 4. Ejecutar con Docker Compose
docker-compose up --build

# 5. Verificar resultado
ls output/integration_output.json
```

#### Usar Docker directamente

```bash
# 1. Construir imagen
docker build -t etl-script .

# 2. Ejecutar con volúmenes
docker run --rm \
  -v $(pwd)/data:/app/data:ro \
  -v $(pwd)/output:/app/output:rw \
  -e OPENWEATHER_API_KEY="your_key" \
  etl-script \
  --orders /app/data/orders.csv \
  --out /app/output/integration_output.json \
  --concurrency 5

# 3. Verificar resultado
ls output/integration_output.json
```

#### Personalizar ejecución Docker

```bash
# Cambiar parámetros
docker run --rm \
  -v $(pwd)/data:/app/data:ro \
  -v $(pwd)/output:/app/output:rw \
  etl-script \
  --orders /app/data/orders.csv \
  --out /app/output/custom_report.json \
  --concurrency 3

# Debug mode (ver logs detallados)
docker run --rm \
  -v $(pwd)/data:/app/data:ro \
  -v $(pwd)/output:/app/output:rw \
  etl-script \
  --orders /app/data/orders.csv 2>&1 | tee etl.log
```

## 📊 Resultado final

```json
[
  {
    "userId": 1,
    "name": "Leanne Graham",
    "username": "Bret",
    "email": "Sincere@april.biz",
    "weather": {
      "main": "Clear",
      "temperature_celsius": 25.5
    },
    "sales_summary": {
      "total_units_sold": 120,
      "total_revenue": 2500.0
    }
  }
]
```

## 🔑 Puntos clave

1. **El CSV original NO tiene Rep** - El script lo inventa
2. **Asignación por ciudad** - Garantiza consistencia 
3. **Username es la clave** - Conecta usuarios con ventas sintéticas
4. **Resultado reproducible** - Siempre misma asignación
5. **Docker incluido** - Ejecución fácil y portable
6. **Entorno virtual recomendado** - Para desarrollo local limpio

## ⚡ Quick Start

### Opción más rápida (Docker):
```bash
mkdir -p data output && cp orders.csv data/ && docker-compose up --build
```

### Opción desarrollo local:
```bash
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
python etl_script.py --orders orders.csv
```

## 📁 Estructura de archivos requerida

```
proyecto/
├── Dockerfile                 # Imagen Docker
├── docker-compose.yml         # Orquestación fácil
├── requirements.txt           # Dependencias Python
├── etl_script.py             # Script principal
├── .venv/                    # Entorno virtual (local)
├── data/
│   └── orders.csv            # Tu archivo CSV
├── output/                   # Resultados generados
└── .env                      # Variables de entorno (opcional)
```

## 🛠️ Dependencias

### Para ejecución local:
- Python 3.8+
- Entorno virtual (recomendado)
- Dependencias: `aiohttp`, `pandas`, `python-dotenv`

### Para ejecución con Docker:
- Docker
- Docker Compose (opcional pero recomendado)

**En resumen: El script resuelve la falta de conexión entre datos creando una correlación artificial pero inteligente.**