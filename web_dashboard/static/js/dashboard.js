/**
 * Smart HVAC Dashboard JavaScript
 * Handles real-time data updates, charts, and user interactions
 */

class SmartHVACDashboard {
    constructor() {
        this.socket = null;
        this.charts = {};
        this.sensorsData = {};
        this.isConnected = false;
        this.refreshInterval = null;
        
        // Color scheme
        this.colors = {
            primary: '#0a0e27',
            accent: '#00d4ff',
            background: '#1a1d3a',
            text: '#ffffff'
        };
        
        // Sensor colors
        this.sensorColors = [
            '#00d4ff', '#0099cc', '#66e0ff', '#33ccff',
            '#009bff', '#4dd8ff', '#1ac6ff', '#80e8ff'
        ];
        
        this.init();
    }
    
    /**
     * Initialize the dashboard
     */
    init() {
        this.setupEventListeners();
        this.initializeSocket();
        this.initializeCharts();
        this.loadInitialData();
        this.startHealthCheck();
        
        console.log('🚀 Smart HVAC Dashboard initialized');
    }
    
    /**
     * Setup DOM event listeners
     */
    setupEventListeners() {
        // Window events
        window.addEventListener('load', () => this.onPageLoad());
        window.addEventListener('beforeunload', () => this.cleanup());
        window.addEventListener('resize', () => this.handleResize());
        
        // Navigation events
        document.addEventListener('DOMContentLoaded', () => {
            this.addNavigationEffects();
            this.addButtonEffects();
            this.addCardEffects();
        });
        
        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => this.handleKeyboardShortcuts(e));
    }
    
    /**
     * Initialize Socket.IO connection
     */
    initializeSocket() {
        this.socket = io();
        
        this.socket.on('connect', () => {
            this.isConnected = true;
            this.updateConnectionStatus(true);
            console.log('✅ Connected to server');
        });
        
        this.socket.on('disconnect', () => {
            this.isConnected = false;
            this.updateConnectionStatus(false);
            console.log('❌ Disconnected from server');
        });
        
        this.socket.on('initial_data', (data) => {
            console.log('📊 Initial data received', data);
            this.handleInitialData(data);
        });
        
        this.socket.on('realtime_data', (data) => {
            console.log('🔄 Real-time data received', data);
            this.handleRealtimeData(data);
        });
        
        this.socket.on('new_alert', (alert) => {
            console.log('🚨 New alert received', alert);
            this.handleNewAlert(alert);
        });
        
        this.socket.on('system_status_update', (status) => {
            this.handleSystemStatusUpdate(status);
        });
        
        this.socket.on('error', (error) => {
            console.error('❌ Socket error:', error);
            this.showNotification('Connection error occurred', 'error');
        });
    }
    
    /**
     * Initialize charts
     */
    initializeCharts() {
        const chartOptions = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: this.colors.text,
                        font: { family: 'Segoe UI' }
                    }
                }
            },
            scales: {
                x: {
                    ticks: { color: this.colors.text },
                    grid: { color: 'rgba(0,212,255,0.1)' }
                },
                y: {
                    ticks: { color: this.colors.text },
                    grid: { color: 'rgba(0,212,255,0.1)' }
                }
            },
            animation: {
                duration: 750,
                easing: 'easeInOutQuart'
            }
        };
        
        // Temperature Chart
        const tempCtx = document.getElementById('temperatureChart');
        if (tempCtx) {
            this.charts.temperature = new Chart(tempCtx, {
                type: 'line',
                data: { labels: [], datasets: [] },
                options: {
                    ...chartOptions,
                    scales: {
                        ...chartOptions.scales,
                        y: {
                            ...chartOptions.scales.y,
                            title: { 
                                display: true, 
                                text: 'Temperature (°C)', 
                                color: this.colors.text 
                            }
                        }
                    }
                }
            });
        }
        
        // Humidity & CO2 Chart
        const humidityCtx = document.getElementById('humidityCoChart');
        if (humidityCtx) {
            this.charts.humidity = new Chart(humidityCtx, {
                type: 'line',
                data: { labels: [], datasets: [] },
                options: {
                    ...chartOptions,
                    scales: {
                        x: chartOptions.scales.x,
                        y: {
                            ...chartOptions.scales.y,
                            position: 'left',
                            title: { 
                                display: true, 
                                text: 'Humidity (%)', 
                                color: this.colors.text 
                            }
                        },
                        y1: {
                            type: 'linear',
                            display: true,
                            position: 'right',
                            title: { 
                                display: true, 
                                text: 'CO₂ (ppm)', 
                                color: this.colors.text 
                            },
                            ticks: { color: this.colors.text },
                            grid: { drawOnChartArea: false }
                        }
                    }
                }
            });
        }
    }
    
    /**
     * Load initial data from API
     */
    async loadInitialData() {
        try {
            const [sensorsResponse, alertsResponse] = await Promise.all([
                fetch('/api/sensors'),
                fetch('/api/alerts')
            ]);
            
            const sensorsData = await sensorsResponse.json();
            const alertsData = await alertsResponse.json();
            
            if (sensorsData.success) {
                this.renderSensors(sensorsData.data);
                this.renderBuildingMap(sensorsData.data);
            }
            
            if (alertsData.success) {
                this.updateAlerts(alertsData.data);
            }
            
        } catch (error) {
            console.error('❌ Error loading initial data:', error);
            this.showNotification('Failed to load initial data', 'error');
        }
    }
    
    /**
     * Handle initial data from socket
     */
    handleInitialData(data) {
        if (data.sensors) {
            this.renderSensors(data.sensors);
            this.renderBuildingMap(data.sensors);
        }
        
        if (data.system_status) {
            this.updateSystemStatus(data.system_status);
        }
        
        if (data.alerts) {
            this.updateAlerts(data.alerts);
        }
    }
    
    /**
     * Handle real-time data updates
     */
    handleRealtimeData(data) {
        const sensorId = data.sensor_id;
        const sensorData = data.data;
        
        // Store data for charts
        if (!this.sensorsData[sensorId]) {
            this.sensorsData[sensorId] = [];
        }
        
        this.sensorsData[sensorId].push({
            timestamp: new Date(data.timestamp),
            temperature: sensorData.data.temperature,
            humidity: sensorData.data.humidity,
            co2: sensorData.data.co2
        });
        
        // Keep only last 50 points
        if (this.sensorsData[sensorId].length > 50) {
            this.sensorsData[sensorId] = this.sensorsData[sensorId].slice(-50);
        }
        
        // Update sensor card
        this.updateSensorCard(sensorId, sensorData);
        
        // Update charts
        this.updateCharts();
    }
    
    /**
     * Handle new alerts
     */
    handleNewAlert(alert) {
        this.addNewAlert(alert);
        this.showNotification(`New ${alert.severity.toLowerCase()} alert: ${alert.message}`, 'warning');
        
        // Add pulse effect to alerts counter
        const alertsCount = document.getElementById('alertsCount');
        if (alertsCount) {
            alertsCount.style.animation = 'pulse 1s ease-in-out';
            setTimeout(() => {
                alertsCount.style.animation = '';
            }, 1000);
        }
    }
    
    /**
     * Handle system status updates
     */
    handleSystemStatusUpdate(status) {
        this.updateSystemStatus(status.system_status.status);
    }
    
    /**
     * Render sensors grid
     */
    renderSensors(sensors) {
        const container = document.getElementById('sensorsContainer');
        if (!container) return;
        
        container.innerHTML = '';
        
        sensors.forEach((sensor, index) => {
            const sensorCard = this.createSensorCard(sensor, index);
            container.appendChild(sensorCard);
        });
        
        // Add entrance animations
        this.addEntranceAnimations(container.children);
    }
    
    /**
     * Create sensor card element
     */
    createSensorCard(sensor, index) {
        const col = document.createElement('div');
        col.className = 'col-lg-3 col-md-6 mb-4';
        
        const latestData = sensor.latest_data;
        const hasData = latestData && latestData.data;
        
        const cardHtml = hasData ? this.createSensorCardWithData(sensor, latestData) : 
                                  this.createSensorCardNoData(sensor);
        
        col.innerHTML = cardHtml;
        return col;
    }
    
    /**
     * Create sensor card with data
     */
    createSensorCardWithData(sensor, latestData) {
        return `
            <div class="sensor-card" data-sensor-id="${sensor.sensor_id}">
                <div class="sensor-header">
                    <i class="fas fa-microchip me-2"></i>
                    ${sensor.name}
                </div>
                <div class="card-body p-0">
                    <div class="metric-item">
                        <div class="metric-icon">
                            <i class="fas fa-thermometer-half"></i>
                        </div>
                        <div class="metric-info">
                            <div class="metric-label">Temperature</div>
                            <div class="metric-value" id="temp-${sensor.sensor_id}">
                                ${latestData.data.temperature}°C
                            </div>
                        </div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-icon">
                            <i class="fas fa-tint"></i>
                        </div>
                        <div class="metric-info">
                            <div class="metric-label">Humidity</div>
                            <div class="metric-value" id="humidity-${sensor.sensor_id}">
                                ${latestData.data.humidity}%
                            </div>
                        </div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-icon">
                            <i class="fas fa-lungs"></i>
                        </div>
                        <div class="metric-info">
                            <div class="metric-label">CO₂</div>
                            <div class="metric-value" id="co2-${sensor.sensor_id}">
                                ${latestData.data.co2} ppm
                            </div>
                        </div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-icon">
                            <i class="fas fa-users"></i>
                        </div>
                        <div class="metric-info">
                            <div class="metric-label">Occupancy</div>
                            <div class="metric-value" id="occupancy-${sensor.sensor_id}">
                                ${latestData.data.occupancy} people
                            </div>
                        </div>
                    </div>
                    <div class="metric-item">
                        <div class="d-flex justify-content-between align-items-center w-100">
                            <span class="metric-label">System Status</span>
                            <span class="status-badge status-${latestData.system_status.toLowerCase()}" 
                                  id="status-${sensor.sensor_id}">
                                ${this.getStatusText(latestData.system_status)}
                            </span>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }
    
    /**
     * Create sensor card without data
     */
    createSensorCardNoData(sensor) {
        return `
            <div class="sensor-card" data-sensor-id="${sensor.sensor_id}">
                <div class="sensor-header">
                    <i class="fas fa-microchip me-2"></i>
                    ${sensor.name}
                </div>
                <div class="card-body">
                    <div class="text-center p-4">
                        <i class="fas fa-exclamation-circle fa-2x mb-2" style="color: var(--accent-color);"></i>
                        <div style="color: var(--text-muted);">No data available</div>
                    </div>
                </div>
            </div>
        `;
    }
    
    /**
     * Render building map
     */
    renderBuildingMap(sensors) {
        const mapContainer = document.getElementById('buildingMap');
        if (!mapContainer) return;
        
        mapContainer.innerHTML = '';
        
        sensors.forEach((sensor, index) => {
            const marker = this.createSensorMarker(sensor, index);
            mapContainer.appendChild(marker);
        });
    }
    
    /**
     * Create sensor marker for building map
     */
    createSensorMarker(sensor, index) {
        const marker = document.createElement('div');
        marker.className = 'sensor-marker';
        marker.style.left = `${(sensor.coordinates.x / 40) * 100}%`;
        marker.style.top = `${(sensor.coordinates.y / 25) * 100}%`;
        marker.title = sensor.name;
        marker.innerHTML = index + 1;
        
        marker.addEventListener('click', () => {
            this.scrollToSensor(sensor.sensor_id);
        });
        
        return marker;
    }
    
    /**
     * Update sensor card values
     */
    updateSensorCard(sensorId, sensorData) {
        const elements = {
            temp: document.getElementById(`temp-${sensorId}`),
            humidity: document.getElementById(`humidity-${sensorId}`),
            co2: document.getElementById(`co2-${sensorId}`),
            occupancy: document.getElementById(`occupancy-${sensorId}`),
            status: document.getElementById(`status-${sensorId}`)
        };
        
        if (elements.temp) {
            this.animateValueChange(elements.temp, `${sensorData.data.temperature}°C`);
        }
        if (elements.humidity) {
            this.animateValueChange(elements.humidity, `${sensorData.data.humidity}%`);
        }
        if (elements.co2) {
            this.animateValueChange(elements.co2, `${sensorData.data.co2} ppm`);
        }
        if (elements.occupancy) {
            this.animateValueChange(elements.occupancy, `${sensorData.data.occupancy} people`);
        }
        
        if (elements.status) {
            const status = sensorData.system_status;
            elements.status.textContent = this.getStatusText(status);
            elements.status.className = `status-badge status-${status.toLowerCase()}`;
        }
    }
    
    /**
     * Update charts with latest data
     */
    updateCharts() {
        if (Object.keys(this.sensorsData).length === 0) return;
        
        // Get timestamps
        const allTimestamps = Object.values(this.sensorsData)
            .flat()
            .map(d => d.timestamp)
            .sort((a, b) => a - b);
        
        const uniqueTimestamps = [...new Set(allTimestamps.map(t => t.getTime()))]
            .map(t => new Date(t))
            .slice(-20);
        
        const labels = uniqueTimestamps.map(t => 
            t.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
        );
        
        this.updateTemperatureChart(labels, uniqueTimestamps);
        this.updateHumidityChart(labels, uniqueTimestamps);
    }
    
    /**
     * Update temperature chart
     */
    updateTemperatureChart(labels, timestamps) {
        if (!this.charts.temperature) return;
        
        const datasets = Object.keys(this.sensorsData).map((sensorId, index) => {
            const data = this.sensorsData[sensorId]
                .filter(d => timestamps.some(t => t.getTime() === d.timestamp.getTime()))
                .map(d => d.temperature);
            
            return {
                label: this.getSensorName(sensorId),
                data: data,
                borderColor: this.sensorColors[index % this.sensorColors.length],
                backgroundColor: this.sensorColors[index % this.sensorColors.length] + '20',
                tension: 0.4,
                fill: false,
                pointRadius: 3,
                pointHoverRadius: 6
            };
        });
        
        this.charts.temperature.data.labels = labels;
        this.charts.temperature.data.datasets = datasets;
        this.charts.temperature.update('none');
    }
    
    /**
     * Update humidity chart
     */
    updateHumidityChart(labels, timestamps) {
        if (!this.charts.humidity) return;
        
        const humidityDatasets = [];
        const co2Datasets = [];
        
        Object.keys(this.sensorsData).forEach((sensorId, index) => {
            const sensorDataPoints = this.sensorsData[sensorId]
                .filter(d => timestamps.some(t => t.getTime() === d.timestamp.getTime()));
            
            const humidityData = sensorDataPoints.map(d => d.humidity);
            const co2Data = sensorDataPoints.map(d => d.co2);
            
            humidityDatasets.push({
                label: `${this.getSensorName(sensorId)} - Humidity`,
                data: humidityData,
                borderColor: this.sensorColors[index % this.sensorColors.length],
                backgroundColor: this.sensorColors[index % this.sensorColors.length] + '20',
                yAxisID: 'y',
                tension: 0.4,
                fill: false,
                pointRadius: 3
            });
            
            co2Datasets.push({
                label: `${this.getSensorName(sensorId)} - CO₂`,
                data: co2Data,
                borderColor: this.sensorColors[index % this.sensorColors.length],
                backgroundColor: this.sensorColors[index % this.sensorColors.length] + '40',
                yAxisID: 'y1',
                tension: 0.4,
                fill: false,
                borderDash: [5, 5],
                pointRadius: 3
            });
        });
        
        this.charts.humidity.data.labels = labels;
        this.charts.humidity.data.datasets = [...humidityDatasets, ...co2Datasets];
        this.charts.humidity.update('none');
    }
    
    /**
     * Update connection status indicator
     */
    updateConnectionStatus(connected) {
        const indicator = document.getElementById('realtimeIndicator');
        if (!indicator) return;
        
        if (connected) {
            indicator.className = 'realtime-indicator';
            indicator.innerHTML = '<i class="fas fa-circle me-2"></i>Live Connection';
        } else {
            indicator.className = 'realtime-indicator disconnected';
            indicator.innerHTML = '<i class="fas fa-circle me-2"></i>Disconnected';
        }
    }
    
    /**
     * Update system status
     */
    updateSystemStatus(status) {
        const statusElement = document.getElementById('systemStatus');
        if (!statusElement) return;
        
        const statusConfigs = {
            'NORMAL': { 
                text: 'All Systems Operating Normally', 
                class: 'system-status status-normal', 
                icon: 'fa-check-circle' 
            },
            'WARNING': { 
                text: 'Minor Issues Detected', 
                class: 'system-status status-warning', 
                icon: 'fa-exclamation-triangle' 
            },
            'CRITICAL': { 
                text: 'Critical Issues Require Attention', 
                class: 'system-status status-critical', 
                icon: 'fa-exclamation-circle' 
            }
        };
        
        const config = statusConfigs[status] || statusConfigs['NORMAL'];
        statusElement.className = config.class;
        statusElement.innerHTML = `<i class="fas ${config.icon} me-2"></i>${config.text}`;
    }
    
    /**
     * Utility functions
     */
    getSensorName(sensorId) {
        const sensorNames = {
            'hvac_office_a1': 'Office A1',
            'hvac_office_a2': 'Office A2',
            'hvac_meeting_room': 'Meeting Room',
            'hvac_main_corridor': 'Corridor',
            'hvac_kitchen': 'Kitchen',
            'hvac_reception': 'Reception',
            'hvac_restroom': 'Restroom'
        };
        return sensorNames[sensorId] || sensorId;
    }
    
    getStatusText(status) {
        const statusTexts = {
            'NORMAL': 'Normal',
            'WARNING': 'Warning',
            'CRITICAL': 'Critical',
            'MALFUNCTION': 'Error'
        };
        return statusTexts[status] || status;
    }
    
    /**
     * Animation and interaction functions
     */
    scrollToSensor(sensorId) {
        const sensorCard = document.querySelector(`[data-sensor-id="${sensorId}"]`);
        if (sensorCard) {
            sensorCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
            sensorCard.style.animation = 'pulse 2s ease-in-out';
            setTimeout(() => sensorCard.style.animation = '', 2000);
        }
    }
    
    animateValueChange(element, newValue) {
        element.style.transform = 'scale(1.1)';
        element.style.transition = 'transform 0.2s ease';
        
        setTimeout(() => {
            element.textContent = newValue;
            element.style.transform = 'scale(1)';
        }, 100);
    }
    
    addEntranceAnimations(elements) {
        Array.from(elements).forEach((element, index) => {
            element.style.opacity = '0';
            element.style.transform = 'translateY(50px)';
            
            setTimeout(() => {
                element.style.transition = 'all 0.6s ease';
                element.style.opacity = '1';
                element.style.transform = 'translateY(0)';
            }, index * 100);
        });
    }
    
    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = 'notification';
        notification.style.cssText = `
            position: fixed;
            top: 80px;
            right: 20px;
            z-index: 9999;
            min-width: 300px;
            background: var(--gradient-card);
            border: 1px solid var(--accent-color);
            border-radius: var(--border-radius);
            padding: 1rem;
            color: var(--text-primary);
            backdrop-filter: blur(10px);
            box-shadow: var(--shadow-md);
            animation: slideInRight 0.3s ease-out;
        `;
        
        const iconClass = type === 'success' ? 'fa-check' : 
                         type === 'warning' ? 'fa-exclamation-triangle' : 
                         type === 'error' ? 'fa-times' : 'fa-info';
        
        notification.innerHTML = `
            <div class="d-flex align-items-center">
                <i class="fas ${iconClass} me-2" style="color: var(--accent-color);"></i>
                <span>${message}</span>
                <button type="button" class="btn-close ms-auto" style="color: var(--accent-color);" 
                        onclick="this.parentElement.parentElement.remove()"></button>
            </div>
        `;
        
        document.body.appendChild(notification);
        setTimeout(() => notification.remove(), 5000);
    }
    
    /**
     * Event handlers
     */
    onPageLoad() {
        console.log('📱 Dashboard page loaded');
        this.addPageLoadAnimations();
    }
    
    addPageLoadAnimations() {
        const elements = document.querySelectorAll('.fade-in, .slide-in, .bounce-in');
        elements.forEach((element, index) => {
            element.style.opacity = '0';
            setTimeout(() => {
                element.style.opacity = '1';
                element.classList.add('animate');
            }, index * 100);
        });
    }
    
    handleResize() {
        // Resize charts
        Object.values(this.charts).forEach(chart => {
            if (chart) {
                chart.resize();
            }
        });
    }
    
    handleKeyboardShortcuts(e) {
        // Ctrl+R or F5 - Refresh data
        if ((e.ctrlKey && e.key === 'r') || e.key === 'F5') {
            e.preventDefault();
            this.loadInitialData();
            this.showNotification('Data refreshed', 'success');
        }
        
        // Escape - Close notifications
        if (e.key === 'Escape') {
            document.querySelectorAll('.notification').forEach(n => n.remove());
        }
    }
    
    /**
     * Health check and monitoring
     */
    startHealthCheck() {
        this.healthCheckInterval = setInterval(() => {
            if (!this.isConnected) {
                console.log('🔄 Attempting to reconnect...');
                this.loadInitialData();
            }
        }, 30000);
    }
    
    cleanup() {
        if (this.healthCheckInterval) {
            clearInterval(this.healthCheckInterval);
        }
        
        if (this.socket) {
            this.socket.disconnect();
        }
        
        console.log('🧹 Dashboard cleaned up');
    }
    
    /**
     * Add interactive effects
     */
    addNavigationEffects() {
        const navLinks = document.querySelectorAll('.nav-link');
        navLinks.forEach(link => {
            link.addEventListener('mouseenter', function() {
                this.style.transform = 'translateY(-2px)';
            });
            
            link.addEventListener('mouseleave', function() {
                this.style.transform = 'translateY(0)';
            });
        });
    }
    
    addButtonEffects() {
        const buttons = document.querySelectorAll('.btn-smart, .btn-outline-smart');
        buttons.forEach(button => {
            button.addEventListener('mouseenter', function() {
                this.style.transform = 'translateY(-2px) scale(1.02)';
            });
            
            button.addEventListener('mouseleave', function() {
                this.style.transform = 'translateY(0) scale(1)';
            });
        });
    }
    
    addCardEffects() {
        const cards = document.querySelectorAll('.sensor-card, .chart-container, .card-smart');
        cards.forEach(card => {
            card.addEventListener('mouseenter', function() {
                this.style.transform = 'translateY(-5px)';
            });
            
            card.addEventListener('mouseleave', function() {
                this.style.transform = 'translateY(0)';
            });
        });
    }
}

// Global functions for backward compatibility
window.exportData = function() {
    const hours = prompt('Export data for how many hours? (default: 24)', '24');
    if (hours) {
        window.open(`/api/export/csv?hours=${hours}`, '_blank');
        dashboard.showNotification('Data is being exported...', 'success');
    }
};

window.acknowledgeAlert = function(alertId) {
    fetch(`/api/alerts/${alertId}/acknowledge`, { method: 'POST' })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                dashboard.showNotification('Alert acknowledged successfully', 'success');
            }
        })
        .catch(error => {
            console.error('❌ Error acknowledging alert:', error);
            dashboard.showNotification('Error acknowledging alert', 'error');
        });
};

window.resolveAlert = function(alertId) {
    fetch(`/api/alerts/${alertId}/resolve`, { method: 'POST' })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                dashboard.showNotification('Alert resolved successfully', 'success');
            }
        })
        .catch(error => {
            console.error('❌ Error resolving alert:', error);
            dashboard.showNotification('Error resolving alert', 'error');
        });
};

// Initialize dashboard when DOM is ready
let dashboard;
document.addEventListener('DOMContentLoaded', function() {
    dashboard = new SmartHVACDashboard();
    window.dashboard = dashboard; // Make it globally accessible
});

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SmartHVACDashboard;
}