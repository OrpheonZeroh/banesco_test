#!/usr/bin/env python3
import os
import json
import asyncio
import argparse
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd
import logging
LOG = logging.getLogger("etl")
if not LOG.handlers:
    logging.basicConfig(
        level=logging.INFO,  # cambia a DEBUG si quieres mayor verbosidad
        format="%(asctime)s %(levelname)s %(message)s"
    )

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

USERS_URL = "https://jsonplaceholder.typicode.com/users"
WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

# ---------- Utilidades ----------
def k_to_c(k: float) -> float:
    return round(k - 273.15, 2)

def to_float_or_none(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

# ---------- HTTP genérico ----------
async def fetch_json(session: aiohttp.ClientSession, url: str, params: Dict[str, Any] = None, retries: int = 3, timeout: int = 20) -> Any:
    params = params or {}
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=timeout) as resp:
                resp.raise_for_status()
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)  # backoff exponencial

# ---------- Fuentes de datos ----------
async def get_users(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    data = await fetch_json(session, USERS_URL)
    users = []
    for u in data:
        geo = (u.get("address") or {}).get("geo") or {}
        lat = to_float_or_none(geo.get("lat"))
        lon = to_float_or_none(geo.get("lng"))  # JSONPlaceholder usa 'lng'
        users.append({
            "userId": u.get("id"),
            "name": u.get("name"),
            "username": u.get("username"),
            "email": u.get("email"),
            "lat": lat,
            "lon": lon
        })
    return users

async def get_weather_for(session: aiohttp.ClientSession, lat: Optional[float], lon: Optional[float]) -> Dict[str, Any]:
    if not OPENWEATHER_API_KEY:
        LOG.warning("OWM: OPENWEATHER_API_KEY no está definida; devolviendo null.")
        return {"main": None, "temperature_celsius": None}
    if lat is None or lon is None:
        LOG.warning("OWM: coords inválidas lat=%s lon=%s; devolviendo null.", lat, lon)
        return {"main": None, "temperature_celsius": None}
    params = {"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY}
    try:
        w = await fetch_json(session, WEATHER_URL, params=params)
        main_desc = None
        if isinstance(w.get("weather"), list) and w["weather"]:
            main_desc = w["weather"][0].get("main")
        temp_k = (w.get("main") or {}).get("temp")
        temp_c = k_to_c(temp_k) if isinstance(temp_k, (int, float)) else None
        return {"main": main_desc, "temperature_celsius": temp_c}
    except Exception:
        return {"main": None, "temperature_celsius": None}

def read_orders(path_csv: str) -> pd.DataFrame:
    LOG.info("Leyendo CSV: %s", path_csv)
    df = pd.read_csv(path_csv)
    LOG.info("Columnas originales: %s", list(df.columns))

    # 1) Eliminar columnas vacías tipo 'Unnamed'
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    LOG.info("Columnas tras remover 'Unnamed': %s", list(df.columns))

    # 2) Normalizar headers (lower + strip)
    df.columns = [str(c).strip().lower() for c in df.columns]
    LOG.info("Columnas normalizadas (lower): %s", list(df.columns))

    # 3) Alias admitidos
    rep_aliases    = {"rep", "representante", "sales_rep", "salesrep", "vendedor", "usuario", "username"}
    units_aliases  = {"units", "qty", "cantidad", "cant", "quantity"}
    total_aliases  = {"total", "totalprice", "importe", "monto", "amount", "revenue"}

    def pick(cols, wanted, label):
        for c in cols:
            if c in wanted:
                LOG.info("Detectado '%s' como: %s", label, c)
                return c
        raise KeyError(f"No encuentro columna de {label}. Admitidos: {sorted(wanted)}. Disponibles: {list(cols)}")

    # 4) Detectar columnas clave
    units_col = pick(df.columns, units_aliases, "unidades")
    total_col = pick(df.columns, total_aliases, "total")

    # 5) Renombrar a canónico
    rename_map = {}
    if units_col != "units": rename_map[units_col] = "units"
    if total_col != "total": rename_map[total_col] = "total"
    if rename_map:
        df = df.rename(columns=rename_map)

    # 6) Tipos
    df["units"] = pd.to_numeric(df["units"], errors="coerce").fillna(0).astype(int)
    df["total"] = pd.to_numeric(df["total"], errors="coerce").fillna(0.0).astype(float)

    LOG.info("Columnas finales tras normalización: %s", list(df.columns))
    return df


def synthesize_rep_if_missing(df: pd.DataFrame, users_usernames: list) -> pd.DataFrame:
    """
    Si no existe 'Rep', la crea distribuyendo usernames de JSONPlaceholder
    de forma determinística por City (round-robin por grupo de ciudad).
    """
    if "Rep" in df.columns:
        return df

    # Si no hay City, asigna en round-robin global
    if "City" not in df.columns:
        assign = []
        k = 0
        for _ in range(len(df)):
            assign.append(users_usernames[k % len(users_usernames)])
            k += 1
        df["Rep"] = assign
        return df

    # Round-robin por City (para que pedidos de una misma ciudad caigan en reps consistentes)
    rep_series = []
    city_counters = {}
    for _, row in df.iterrows():
        city = str(row["City"]).strip().lower()
        idx = city_counters.get(city, 0)
        rep = users_usernames[idx % len(users_usernames)]
        city_counters[city] = idx + 1
        rep_series.append(rep)

    df["Rep"] = rep_series
    return df


def aggregate_sales(df: pd.DataFrame) -> pd.DataFrame:
    # Asegura que la columna exista con el nombre correcto
    if "rep_norm" not in df.columns:
        raise KeyError("Falta la columna 'rep_norm' antes de agregar. Verifica la creación en main().")

    agg = (
        df.groupby("rep_norm", dropna=False)
          .agg(
              total_units_sold=("units", "sum"),
              total_revenue=("total", "sum"),
          )
          .reset_index()
    )
    return agg


# ---------- Orquestación ----------
async def main(orders_csv: str, output_json: str, concurrency: int = 10):
    # Sesión HTTP compartida
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        LOG.info("Descargando usuarios de JSONPlaceholder...")
        users = await get_users(session)
        LOG.info("Usuarios obtenidos: %d", len(users))

        LOG.info("Leyendo y normalizando pedidos desde: %s", orders_csv)
        orders_df = read_orders(orders_csv)

        # usernames para sintetizar 'Rep' si falta
        usernames = [(u.get("username") or "").strip() for u in users if u.get("username")]
        orders_df = synthesize_rep_if_missing(orders_df, usernames)

        # Normalización para el join (case-insensitive)
        # read_orders y synthesize usan headers en minúsculas; si tu versión deja "Rep", este fallback lo cubre
        rep_col = "rep" if "rep" in orders_df.columns else ("Rep" if "Rep" in orders_df.columns else None)
        if rep_col is None:
            raise ValueError("No se encontró columna 'rep' ni 'Rep' después de la síntesis.")
        orders_df["rep_norm"] = orders_df[rep_col].astype(str).str.strip().str.lower()
        LOG.info("Primeros rep_norm: %s", orders_df["rep_norm"].head(5).tolist())

        LOG.info("Agregando ventas por representante...")
        sales_df = aggregate_sales(orders_df)
        LOG.info("Filas en agregación de ventas: %d", len(sales_df))

        # Índice de ventas por username (case-insensitive)
        sales_map = {
            row["rep_norm"]: {
                "total_units_sold": int(row["total_units_sold"]),
                "total_revenue": float(row["total_revenue"])
            }
            for _, row in sales_df.iterrows()
        }
        LOG.info("Entradas en sales_map: %d", len(sales_map))

        sem = asyncio.Semaphore(concurrency)

        async def enrich_user(u: Dict[str, Any]) -> Dict[str, Any]:
            async with sem:
                weather = await get_weather_for(session, u["lat"], u["lon"])
            rep_key = (u.get("username") or "").strip().lower()
            sales_summary = sales_map.get(rep_key, {"total_units_sold": 0, "total_revenue": 0.0})
            return {
                "userId": u["userId"],
                "name": u["name"],
                "username": u["username"],
                "email": u["email"],
                "weather": weather,
                "sales_summary": sales_summary
            }

        LOG.info("Enriqueciendo usuarios con clima y ventas (concurrency=%d)...", concurrency)
        results = await asyncio.gather(*(enrich_user(u) for u in users))

    LOG.info("Escribiendo JSON final en: %s", output_json)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    LOG.info("Listo ✅")

# ---------- CLI ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Usuarios + Pedidos + Clima")
    parser.add_argument("--orders", required=True, help="Ruta a orders.csv")
    parser.add_argument("--out", default="integration_output.json", help="Ruta del JSON de salida")
    parser.add_argument("--concurrency", type=int, default=10, help="Máximo de requests simultáneas")
    args = parser.parse_args()

    if not OPENWEATHER_API_KEY:
        print("ADVERTENCIA: OPENWEATHER_API_KEY no está definida. El clima saldrá como null.")

    asyncio.run(main(args.orders, args.out, args.concurrency))