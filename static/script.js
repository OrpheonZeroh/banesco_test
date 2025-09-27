class ETLDashboard {
    constructor() {
        this.processButton = null;
        this.buttonText = null;
        this.buttonSpinner = null;
        this.loadingOverlay = null;
        this.progressFill = null;
        this.progressText = null;
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
        const processButton = document.getElementById('processBtn');
        const buttonText = document.getElementById('btnText');
        const buttonSpinner = document.getElementById('spinner');
        const progressSection = document.getElementById('progressSection');
        const progressFill = document.getElementById('progressFill');
        const progressText = document.getElementById('progressText');
        const progressPercent = document.getElementById('progressPercent');
        this.downloadBtn = document.getElementById('downloadBtn');
        this.viewResultBtn = document.getElementById('viewResultBtn');
        this.deleteBtn = document.getElementById('deleteBtn');
        this.downloadButtons = document.getElementById('downloadButtons');
        this.directDownloadLink = document.getElementById('directDownloadLink');
        this.clearLogsBtn = document.getElementById('clearLogsBtn');
        this.toggleAutoScrollBtn = document.getElementById('toggleAutoScrollBtn');
        this.closeResultBtn = document.getElementById('closeResultBtn');
        
        // Indicadores de estado
        this.statusText = document.getElementById('statusText');
        this.statusDot = document.getElementById('statusDot');
        this.connectionStatus = document.getElementById('connection-status');
        this.clientCount = document.getElementById('process-status');
        
        // Logs
        this.logsContent = document.getElementById('logsContent');
        
        // Result section
        this.resultSection = document.getElementById('resultSection');
        this.resultContent = document.getElementById('resultContent');
        this.resultStats = document.getElementById('resultStats');
        
        this.processButton = processButton;
        this.buttonText = buttonText;
        this.buttonSpinner = buttonSpinner;
        this.progressSection = progressSection;
        this.progressFill = progressFill;
        this.progressText = progressText;
        this.progressPercent = progressPercent;
    }

    bindEvents() {
        if (this.processButton) {
            this.processButton.addEventListener('click', () => this.processETL());
        }
        if (this.downloadBtn) {
            this.downloadBtn.addEventListener('click', () => this.downloadResult());
        }
        if (this.viewResultBtn) {
            this.viewResultBtn.addEventListener('click', () => this.viewResult());
        }
        if (this.deleteBtn) {
            this.deleteBtn.addEventListener('click', () => this.deleteFile());
        }
        if (this.closeResultBtn) {
            this.closeResultBtn.addEventListener('click', () => this.closeResult());
        }
        if (this.clearLogsBtn) {
            this.clearLogsBtn.addEventListener('click', () => this.clearLogs());
        }
        if (this.toggleAutoScrollBtn) {
            this.toggleAutoScrollBtn.addEventListener('click', () => this.toggleAutoScroll());
        }
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
                const data = JSON.parse(event.data);
                if (data.type === 'log') {
                    this.addLog(data.level, data.message);
                    this.updateProgressFromLog(data.message);
                } else if (data.type === 'complete') {
                    this.downloadButtons.classList.remove('hidden');
                    this.updateStatus('ready');
                    this.updateProgress(100, 'Completado');
                }
            } catch (error) {
                console.error('Error parsing SSE data:', error);
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
        this.updateProcessButton(true);
        this.showProgressBar();
        this.clearLogs();
        this.updateStatus('running');
        
        try {
            const response = await fetch('/process-etl', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const result = await response.json();
            
            if (response.ok) {
                if (result.success) {
                    this.addLog('success', '✅ Proceso ETL completado exitosamente');
                    this.downloadButtons.classList.remove('hidden');
                    this.updateProgress(100, 'Proceso completado');
                } else {
                    this.addLog('error', `❌ Error: ${result.error}`);
                }
            } else {
                throw new Error(result.detail || 'Error processing ETL');
            }
            
        } catch (error) {
            console.error('Error processing ETL:', error);
            this.addLog('error', `❌ Error de conexión: ${error.message}`);
        } finally {
            setTimeout(() => {
                this.hideProgressBar();
                this.isProcessing = false;
                this.updateProcessButton(false);
                this.updateStatus('ready');
            }, 1000);
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

    async deleteFile() {
        if (!confirm('¿Estás seguro de que quieres eliminar el archivo integration_output.json?')) {
            return;
        }
        
        try {
            this.addLog('info', '🗑️ Eliminando archivo...');
            
            const response = await fetch('/delete-output', {
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json',
                },
            });
            
            if (response.ok) {
                const result = await response.json();
                this.addLog('success', `✅ ${result.message}`);
                this.downloadButtons.classList.add('hidden');
                this.resultSection.classList.add('hidden');
            } else {
                const error = await response.json();
                this.addLog('error', `❌ Error: ${error.detail}`);
            }
            
        } catch (error) {
            console.error('Error deleting file:', error);
            this.addLog('error', `❌ Error de conexión: ${error.message}`);
        }
    }

    addLog(level, message) {
        const timestamp = new Date().toLocaleTimeString();
        const logEntry = document.createElement('div');
        logEntry.className = `log-entry ${level}`;
        
        logEntry.innerHTML = `
            <span class="log-time">${timestamp}</span>
            <span class="log-message">${message}</span>
        `;
        
        if (this.logsContent) {
            this.logsContent.appendChild(logEntry);
            
            if (this.autoScroll) {
                logEntry.scrollIntoView({ behavior: 'smooth' });
            }
        } else {
            console.log(`[${level.toUpperCase()}] ${timestamp}: ${message}`);
        }
    }

    clearLogs() {
        // Mantener solo el mensaje inicial
        if (this.logsContent) {
            this.logsContent.innerHTML = `
                <div class="log-entry info">
                    <span class="log-time">--:--:--</span>
                    <span class="log-message">💡 Logs limpios - Listo para nuevo proceso</span>
                </div>
            `;
        }
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
    }

    updateProcessButton(isLoading) {
        if (isLoading) {
            this.buttonText.textContent = 'Procesando...';
            this.buttonSpinner.classList.remove('hidden');
            this.processButton.disabled = true;
        } else {
            this.buttonText.textContent = 'OBTENER INTEGRATION_OUTPUT.JSON';
            this.buttonSpinner.classList.add('hidden');
            this.processButton.disabled = false;
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

    showProgressBar() {
        this.progressSection.classList.remove('hidden');
        this.updateProgress(0, 'Iniciando proceso ETL...');
    }

    hideProgressBar() {
        this.progressSection.classList.add('hidden');
    }

    updateProgress(percentage, text) {
        this.progressFill.style.width = `${percentage}%`;
        this.progressText.textContent = text;
        this.progressPercent.textContent = `${Math.round(percentage)}%`;
    }

    updateProgressFromLog(message) {
        console.log('Checking progress for message:', message); // Debug
        
        if (message.includes('Descargando usuarios') || message.includes('usuarios')) {
            this.updateProgress(25, 'Descargando usuarios...');
        } else if (message.includes('pedidos') || message.includes('CSV')) {
            this.updateProgress(50, 'Procesando pedidos...');
        } else if (message.includes('clima') || message.includes('weather') || message.includes('Enriqueciendo')) {
            this.updateProgress(75, 'Obteniendo datos del clima...');
        } else if (message.includes('Escribiendo JSON') || message.includes('Listo') || message.includes('✅')) {
            this.updateProgress(95, 'Generando reporte final...');
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
