class ETLDashboard {
    constructor() {
        this.eventSource = null;
        this.autoScroll = true;
        this.isProcessing = false;
        
        this.initElements();
        this.bindEvents();
        this.checkStatus();
        this.connectToLogs();
    }

    initElements() {
        // Botones y controles
        this.processBtn = document.getElementById('processBtn');
        this.btnText = document.getElementById('btnText');
        this.spinner = document.getElementById('spinner');
        this.downloadBtn = document.getElementById('downloadBtn');
        this.viewResultBtn = document.getElementById('viewResultBtn');
        this.downloadButtons = document.getElementById('downloadButtons');
        this.directDownloadLink = document.getElementById('directDownloadLink');
        this.clearLogsBtn = document.getElementById('clearLogsBtn');
        this.toggleAutoScrollBtn = document.getElementById('toggleAutoScrollBtn');
        this.closeResultBtn = document.getElementById('closeResultBtn');
        
        // Indicadores de estado
        this.statusText = document.getElementById('statusText');
        this.statusDot = document.getElementById('statusDot');
        this.connectionStatus = document.getElementById('connectionStatus');
        this.clientCount = document.getElementById('clientCount');
        
        // Logs
        this.logsContent = document.getElementById('logsContent');
        
        // Result section
        this.resultSection = document.getElementById('resultSection');
        this.resultContent = document.getElementById('resultContent');
        this.resultStats = document.getElementById('resultStats');
    }

    bindEvents() {
        this.processBtn.addEventListener('click', () => this.processETL());
        this.downloadBtn.addEventListener('click', () => this.downloadResult());
        this.viewResultBtn.addEventListener('click', () => this.viewResult());
        this.closeResultBtn.addEventListener('click', () => this.closeResult());
        this.clearLogsBtn.addEventListener('click', () => this.clearLogs());
        this.toggleAutoScrollBtn.addEventListener('click', () => this.toggleAutoScroll());
    }

    async checkStatus() {
        try {
            const response = await fetch('/status');
            const status = await response.json();
            
            this.updateStatus(status.is_running ? 'running' : 'ready');
            this.updateClientCount(status.connected_clients);
            
            // Mostrar botones si existe output
            if (status.output_exists && status.output_size > 0) {
                this.downloadButtons.classList.remove('hidden');
            } else {
                this.downloadButtons.classList.add('hidden');
            }
            
        } catch (error) {
            console.error('Error checking status:', error);
        }
    }

    connectToLogs() {
        if (this.eventSource) {
            this.eventSource.close();
        }

        this.eventSource = new EventSource('/logs/stream');
        
        this.eventSource.onopen = () => {
            this.updateConnectionStatus(true);
        };
        
        this.eventSource.onmessage = (event) => {
            try {
                const logData = JSON.parse(event.data);
                
                if (logData.type === 'ping') {
                    return; // Ignorar pings
                }
                
                this.addLogEntry(logData);
            } catch (error) {
                console.error('Error parsing log data:', error);
            }
        };
        
        this.eventSource.onerror = () => {
            this.updateConnectionStatus(false);
            
            // Reconectar después de 3 segundos
            setTimeout(() => {
                if (this.eventSource.readyState === EventSource.CLOSED) {
                    this.connectToLogs();
                }
            }, 3000);
        };
    }

    async processETL() {
        if (this.isProcessing) return;
        
        this.isProcessing = true;
        this.updateStatus('running');
        this.downloadButtons.classList.add('hidden');
        this.closeResult();
        
        // Limpiar logs anteriores
        this.clearLogs();
        
        try {
            const response = await fetch('/process-etl', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            const result = await response.json();
            
            if (response.ok) {
                if (result.success) {
                    this.updateStatus('success');
                    this.downloadBtn.classList.remove('hidden');
                    this.viewResultBtn.classList.remove('hidden');
                } else {
                    this.updateStatus('error');
                }
            } else {
                throw new Error(result.detail || 'Error processing ETL');
            }
            
        } catch (error) {
            console.error('Error processing ETL:', error);
            this.updateStatus('error');
            this.addLogEntry({
                timestamp: new Date().toLocaleTimeString(),
                message: `❌ Error: ${error.message}`,
                type: 'error'
            });
        } finally {
            this.isProcessing = false;
        }
    }

    async downloadResult() {
        try {
            this.addLogEntry({
                timestamp: new Date().toLocaleTimeString(),
                message: '📥 Iniciando descarga...',
                type: 'info'
            });

            const response = await fetch('/download', {
                method: 'GET',
                headers: {
                    'Accept': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            // Obtener el contenido como blob
            const blob = await response.blob();
            
            // Crear URL temporal
            const url = window.URL.createObjectURL(blob);
            
            // Crear elemento de descarga
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = 'integration_output.json';
            a.setAttribute('download', 'integration_output.json');
            
            // Agregar al DOM, hacer click y limpiar
            document.body.appendChild(a);
            
            // Pequeño delay para asegurar que el elemento esté en el DOM
            setTimeout(() => {
                a.click();
                
                // Limpiar después de un momento
                setTimeout(() => {
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                }, 100);
            }, 10);
            
            this.addLogEntry({
                timestamp: new Date().toLocaleTimeString(),
                message: '✅ Descarga iniciada - revisa tu carpeta de Descargas',
                type: 'success'
            });
            
        } catch (error) {
            console.error('Error downloading file:', error);
            this.addLogEntry({
                timestamp: new Date().toLocaleTimeString(),
                message: `❌ Error al descargar: ${error.message}`,
                type: 'error'
            });
            
            // Método alternativo: abrir en nueva ventana
            try {
                window.open('/download', '_blank');
                this.addLogEntry({
                    timestamp: new Date().toLocaleTimeString(),
                    message: '🔗 Abriendo descarga en nueva ventana',
                    type: 'info'
                });
            } catch (fallbackError) {
                console.error('Fallback download failed:', fallbackError);
            }
        }
    }

    async viewResult() {
        try {
            const response = await fetch('/result');
            
            if (!response.ok) {
                throw new Error('Result not available');
            }
            
            const result = await response.json();
            
            // Mostrar estadísticas
            this.resultStats.textContent = `${result.count} registros • ${Math.round(result.file_size / 1024)} KB`;
            
            // Mostrar JSON formateado
            this.resultContent.textContent = JSON.stringify(result.data, null, 2);
            
            // Mostrar sección
            this.resultSection.classList.remove('hidden');
            
            this.addLogEntry({
                timestamp: new Date().toLocaleTimeString(),
                message: '👁️ Mostrando resultado JSON en pantalla',
                type: 'success'
            });
            
        } catch (error) {
            console.error('Error viewing result:', error);
            this.addLogEntry({
                timestamp: new Date().toLocaleTimeString(),
                message: `❌ Error al mostrar resultado: ${error.message}`,
                type: 'error'
            });
        }
    }

    closeResult() {
        this.resultSection.classList.add('hidden');
    }

    addLogEntry(logData) {
        const logEntry = document.createElement('div');
        logEntry.className = `log-entry ${logData.type}`;
        
        const timeSpan = document.createElement('span');
        timeSpan.className = 'log-time';
        timeSpan.textContent = logData.timestamp;
        
        const messageSpan = document.createElement('span');
        messageSpan.className = 'log-message';
        messageSpan.textContent = logData.message;
        
        logEntry.appendChild(timeSpan);
        logEntry.appendChild(messageSpan);
        
        this.logsContent.appendChild(logEntry);
        
        // Auto-scroll si está habilitado
        if (this.autoScroll) {
            logEntry.scrollIntoView({ behavior: 'smooth', block: 'end' });
        }
        
        // Limitar número de logs (mantener últimos 500)
        const logEntries = this.logsContent.querySelectorAll('.log-entry');
        if (logEntries.length > 500) {
            logEntries[0].remove();
        }
    }

    clearLogs() {
        // Mantener solo el mensaje inicial
        this.logsContent.innerHTML = `
            <div class="log-entry info">
                <span class="log-time">--:--:--</span>
                <span class="log-message">💡 Logs limpios - Listo para nuevo proceso</span>
            </div>
        `;
    }

    toggleAutoScroll() {
        this.autoScroll = !this.autoScroll;
        
        if (this.autoScroll) {
            this.toggleAutoScrollBtn.classList.add('active');
            this.toggleAutoScrollBtn.textContent = '📜 Auto-scroll';
            
            // Scroll al final
            const lastEntry = this.logsContent.lastElementChild;
            if (lastEntry) {
                lastEntry.scrollIntoView({ behavior: 'smooth', block: 'end' });
            }
        } else {
            this.toggleAutoScrollBtn.classList.remove('active');
            this.toggleAutoScrollBtn.textContent = '⏸️ Manual';
        }
    }

    updateStatus(status) {
        const statusTexts = {
            ready: 'Listo para procesar',
            running: 'Procesando datos...',
            success: 'Proceso completado ✅',
            error: 'Error en proceso ❌'
        };
        
        this.statusText.textContent = statusTexts[status] || statusTexts.ready;
        
        // Actualizar dot
        this.statusDot.className = `status-dot ${status}`;
        
        // Actualizar botón
        if (status === 'running') {
            this.processBtn.disabled = true;
            this.btnText.textContent = 'PROCESANDO...';
            this.spinner.classList.remove('hidden');
        } else {
            this.processBtn.disabled = false;
            this.btnText.textContent = 'OBTENER INTEGRATION_OUTPUT.JSON';
            this.spinner.classList.add('hidden');
        }
    }

    updateConnectionStatus(connected) {
        if (connected) {
            this.connectionStatus.textContent = '🔌 Conectado';
            this.connectionStatus.style.color = '#28a745';
        } else {
            this.connectionStatus.textContent = '🔌 Desconectado';
            this.connectionStatus.style.color = '#dc3545';
        }
    }

    updateClientCount(count) {
        if (count > 0) {
            this.clientCount.textContent = `👥 ${count} cliente${count > 1 ? 's' : ''} conectado${count > 1 ? 's' : ''}`;
        } else {
            this.clientCount.textContent = '';
        }
    }

    // Cleanup al cerrar la página
    destroy() {
        if (this.eventSource) {
            this.eventSource.close();
        }
    }
}

// Inicializar dashboard cuando se carga la página
document.addEventListener('DOMContentLoaded', () => {
    window.etlDashboard = new ETLDashboard();
});

// Cleanup al cerrar la página
window.addEventListener('beforeunload', () => {
    if (window.etlDashboard) {
        window.etlDashboard.destroy();
    }
});

// Actualizar estado cada 30 segundos
setInterval(() => {
    if (window.etlDashboard) {
        window.etlDashboard.checkStatus();
    }
}, 30000);
