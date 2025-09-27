#!/usr/bin/env python3
"""
FastAPI Web Application para ETL Processor
Ejecuta el ETL existente (main.py) de manera transparente y muestra logs en tiempo real
"""

import asyncio
import json
import os
import io
from pathlib import Path
from typing import AsyncGenerator, Dict, Any
import subprocess
import queue
import threading
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

# Estado global de la aplicación
class AppState:
    def __init__(self):
        self.is_running = False
        self.last_execution = None
        self.log_queue = asyncio.Queue()
        self.clients = set()
        
app_state = AppState()

# Crear aplicación FastAPI
app = FastAPI(
    title="ETL Dashboard",
    description="Dashboard para ejecutar y monitorear el proceso ETL",
    version="1.0.0"
)

# Montar archivos estáticos
static_path = Path(__file__).parent / "static"
static_path.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Servir la página principal"""
    html_file = static_path / "index.html"
    if html_file.exists():
        return HTMLResponse(content=html_file.read_text(encoding='utf-8'))
    else:
        # HTML básico si no existe el archivo
        return HTMLResponse(content="""
        <!DOCTYPE html>
        <html>
        <head><title>ETL Dashboard</title></head>
        <body>
            <h1>ETL Dashboard</h1>
            <p>Error: static/index.html no encontrado</p>
        </body>
        </html>
        """)

@app.get("/status")
async def get_status():
    """Obtener estado actual del proceso ETL"""
    output_file = Path("output/integration_output.json")
    return {
        "is_running": app_state.is_running,
        "last_execution": app_state.last_execution,
        "output_exists": output_file.exists(),
        "output_size": output_file.stat().st_size if output_file.exists() else 0,
        "connected_clients": len(app_state.clients)
    }

async def log_reader(process: asyncio.subprocess.Process):
    """Lee logs del proceso ETL y los envía a la cola"""
    try:
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            
            log_text = line.decode('utf-8').strip()
            if log_text:
                timestamp = datetime.now().strftime("%H:%M:%S")
                log_entry = {
                    "timestamp": timestamp,
                    "message": log_text,
                    "type": "info"
                }
                
                # Detectar nivel de log por contenido
                if "ERROR" in log_text.upper() or "FAILED" in log_text.upper():
                    log_entry["type"] = "error"
                elif "WARNING" in log_text.upper() or "WARN" in log_text.upper():
                    log_entry["type"] = "warning"
                elif "✅" in log_text or "SUCCESS" in log_text.upper():
                    log_entry["type"] = "success"
                
                await app_state.log_queue.put(log_entry)
                
    except Exception as e:
        error_log = {
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "message": f"Error reading logs: {str(e)}",
            "type": "error"
        }
        await app_state.log_queue.put(error_log)

@app.post("/process-etl")
async def process_etl():
    """Ejecutar el proceso ETL"""
    if app_state.is_running:
        raise HTTPException(status_code=409, detail="ETL process is already running")
    
    # Verificar que existe el archivo orders.csv
    if not Path("orders.csv").exists():
        raise HTTPException(status_code=400, detail="orders.csv not found")
    
    app_state.is_running = True
    app_state.last_execution = datetime.now().isoformat()
    
    try:
        # Limpiar cola de logs
        while not app_state.log_queue.empty():
            try:
                app_state.log_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        
        # Log inicial
        start_log = {
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "message": "🚀 Iniciando proceso ETL...",
            "type": "info"
        }
        await app_state.log_queue.put(start_log)
        
        # Crear directorio de output si no existe
        Path("output").mkdir(exist_ok=True)
        
        # Ejecutar main.py como subproceso
        process = await asyncio.create_subprocess_exec(
            "python", "main.py", 
            "--orders", "orders.csv",
            "--out", "output/integration_output.json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=Path(__file__).parent
        )
        
        # Iniciar lector de logs
        log_task = asyncio.create_task(log_reader(process))
        
        # Esperar que termine el proceso
        return_code = await process.wait()
        
        # Cancelar tarea de lectura si sigue activa
        if not log_task.done():
            log_task.cancel()
        
        # Log final
        if return_code == 0:
            final_log = {
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "message": "✅ Proceso ETL completado exitosamente",
                "type": "success"
            }
        else:
            final_log = {
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "message": f"❌ Proceso ETL terminó con error (código: {return_code})",
                "type": "error"
            }
        
        await app_state.log_queue.put(final_log)
        
        return {
            "success": return_code == 0,
            "return_code": return_code,
            "message": "Process completed" if return_code == 0 else f"Process failed with code {return_code}"
        }
        
    except Exception as e:
        error_log = {
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "message": f"❌ Error ejecutando ETL: {str(e)}",
            "type": "error"
        }
        await app_state.log_queue.put(error_log)
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        app_state.is_running = False

async def event_generator() -> AsyncGenerator[str, None]:
    """Generador de eventos para Server-Sent Events"""
    try:
        while True:
            try:
                # Obtener log de la cola (con timeout)
                log_entry = await asyncio.wait_for(app_state.log_queue.get(), timeout=30.0)
                
                # Formatear como Server-Sent Event
                data = json.dumps(log_entry)
                yield f"data: {data}\n\n"
                
            except asyncio.TimeoutError:
                # Enviar ping cada 30 segundos para mantener conexión
                yield "data: {\"type\":\"ping\"}\n\n"
                
    except asyncio.CancelledError:
        pass

@app.get("/logs/stream")
async def stream_logs(request: Request):
    """Endpoint para Server-Sent Events de logs"""
    
    async def event_stream():
        try:
            # Agregar cliente a la lista
            client_id = id(request)
            app_state.clients.add(client_id)
            
            # Enviar evento de conexión
            connection_log = {
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "message": "📡 Conectado al stream de logs",
                "type": "info"
            }
            data = json.dumps(connection_log)
            yield f"data: {data}\n\n"
            
            # Stream de eventos
            async for event in event_generator():
                # Verificar si el cliente sigue conectado
                if await request.is_disconnected():
                    break
                yield event
                
        except asyncio.CancelledError:
            pass
        finally:
            # Remover cliente de la lista
            app_state.clients.discard(client_id)
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
        }
    )

@app.get("/download")
async def download_result():
    """Descargar el archivo JSON resultado"""
    output_file = Path("output/integration_output.json")
    
    if not output_file.exists():
        raise HTTPException(status_code=404, detail="Output file not found. Run ETL process first.")
    
    return FileResponse(
        path=str(output_file),
        filename="integration_output.json",
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": "attachment; filename=\"integration_output.json\"",
            "Content-Type": "application/octet-stream"
        }
    )

@app.get("/download-direct")
async def download_direct():
    """Método alternativo de descarga - devuelve el JSON como texto"""
    output_file = Path("output/integration_output.json")
    
    if not output_file.exists():
        raise HTTPException(status_code=404, detail="Output file not found. Run ETL process first.")
    
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return StreamingResponse(
            io.BytesIO(content.encode('utf-8')),
            media_type="application/json",
            headers={
                "Content-Disposition": "attachment; filename=\"integration_output.json\"",
                "Content-Length": str(len(content.encode('utf-8')))
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")

@app.get("/result")
async def get_result():
    """Obtener el contenido del JSON resultado para mostrar en la web"""
    output_file = Path("output/integration_output.json")
    
    if not output_file.exists():
        raise HTTPException(status_code=404, detail="Output file not found. Run ETL process first.")
    
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return {
            "success": True,
            "data": data,
            "count": len(data) if isinstance(data, list) else 1,
            "file_size": output_file.stat().st_size,
            "last_modified": output_file.stat().st_mtime
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")

# Configuración para desarrollo local
if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
