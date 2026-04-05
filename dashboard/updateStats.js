const VM_IP = "172.169.248.121"

const PROCESSING_STATS_API_URL = `http://${VM_IP}:8100/stats`
const ANALYZER_STATS_API_URL = `http://${VM_IP}:5005/analyzer/stats`
const ANALYZER_PERFORMANCE_API_BASE = `http://${VM_IP}:5005/analyzer/performance`
const ANALYZER_ERROR_API_BASE = `http://${VM_IP}:5005/analyzer/error`
const HEALTH_CHECK_API_URL = `http://${VM_IP}:8120/healthcheck/health-status`

/**
 * Generic fetch function to retrieve data from API endpoints
 */
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => {
            if (!res.ok) {
                throw new Error(`HTTP error! status: ${res.status}`);
            }
            return res.json();
        })
        .then((result) => {
            console.log("✓ Received from " + url);
            cb(result);
        })
        .catch((error) => {
            console.error("✗ Error fetching from " + url + ":", error);
            updateErrorMessages(error.message);
        });
};

/**
 * Update a code div with formatted JSON
 */
const updateCodeDiv = (result, elemId) => {
    document.getElementById(elemId).innerText = JSON.stringify(result, null, 2);
};

/**
 * Get current date and time as a formatted string
 */
const getLocaleDateStr = () => (new Date()).toLocaleString();

/**
 * Convert ISO timestamp to "X seconds ago" format
 * Makes it easier for analysts to see freshness at a glance
 */
const getTimeAgo = (isoTimestamp) => {
    try {
        const lastCheck = new Date(isoTimestamp);
        const now = new Date();
        const secondsAgo = Math.floor((now - lastCheck) / 1000);

        if (secondsAgo < 0) {
            return "just now";
        } else if (secondsAgo < 60) {
            return `${secondsAgo}s ago`;
        } else if (secondsAgo < 3600) {
            const minutes = Math.floor(secondsAgo / 60);
            return `${minutes}m ago`;
        } else {
            const hours = Math.floor(secondsAgo / 3600);
            return `${hours}h ago`;
        }
    } catch (e) {
        return "unknown";
    }
};

/**
 * Update individual metrics from processing stats
 */
const updateProcessingMetrics = (stats) => {
    document.getElementById("proc-perf").innerText = stats.num_performance_readings || 0;
    document.getElementById("proc-error").innerText = stats.num_error_readings || 0;
    document.getElementById("proc-cpu").innerText = (stats.max_cpu_reading || 0).toFixed(1) + "%";
    document.getElementById("proc-severity").innerText = stats.max_severity_level || 0;
};

/**
 * Update individual metrics from analyzer stats
 */
const updateAnalyzerMetrics = (stats) => {
    document.getElementById("ana-perf").innerText = stats.num_performance_events || 0;
    document.getElementById("ana-error").innerText = stats.num_error_events || 0;
};

/**
 * Format and display health status with color-coded indicators
 * Now shows "X seconds ago" instead of timestamp
 */
const updateHealthStatus = (health_data) => {
    const services_grid = document.getElementById("health-services");
    services_grid.innerHTML = '';

    const service_colors = {
        'receiver': 'Receiver',
        'storage': 'Storage',
        'processing': 'Processing',
        'analyzer': 'Analyzer'
    };

    for (const [service_key, service_name] of Object.entries(service_colors)) {
        const status = health_data[service_key] || 'Unknown';
        const status_class = status === 'Up' ? 'up' : (status === 'Down' ? 'down' : 'unknown');
        const status_display = status === 'Up' ? '✓ Up' : (status === 'Down' ? '✗ Down' : '? Unknown');

        const html = `
            <div class="service-item ${status_class}">
                <div class="service-indicator ${status_class}"></div>
                <div class="service-name">${service_name}</div>
                <div class="service-status ${status_class}">${status_display}</div>
            </div>
        `;
        services_grid.innerHTML += html;
    }

   
    const timeAgo = getTimeAgo(health_data.last_update);
    document.getElementById("health-last-update").innerText = timeAgo;
};

/**
 * Main function to fetch all statistics and update the dashboard
 */
const getStats = () => {
    console.log("🔄 Updating all statistics...");
    document.getElementById("last-updated-value").innerText = getLocaleDateStr();
    
    // ========================================================================
    // PROCESSING SERVICE STATS
    // ========================================================================
    makeReq(PROCESSING_STATS_API_URL, (result) => {
        updateCodeDiv(result, "processing-stats");
        updateProcessingMetrics(result);
    });
    
    // ========================================================================
    // ANALYZER SERVICE STATS + DYNAMIC EVENTS
    // ========================================================================
    makeReq(ANALYZER_STATS_API_URL, (stats) => {
        updateCodeDiv(stats, "analyzer-stats");
        updateAnalyzerMetrics(stats);
        
        // Pick random performance event index
        if (stats.num_performance_events > 0) {
            const randomPerfIndex = Math.floor(
                Math.random() * stats.num_performance_events
            );
            console.log(`📉 Fetching performance event at index ${randomPerfIndex}`);
            
            document.getElementById("performance-event-heading").innerText = 
                `📉 Performance Event (Index ${randomPerfIndex})`;
            
            const perfUrl = `${ANALYZER_PERFORMANCE_API_BASE}?index=${randomPerfIndex}`;
            makeReq(perfUrl, (result) => {
                updateCodeDiv(result, "event-performance");
            });
        } else {
            document.getElementById("event-performance").innerText = 
                "No performance events available";
            document.getElementById("performance-event-heading").innerText = 
                "📉 Performance Event (No data)";
        }
        
        // Pick random error event index
        if (stats.num_error_events > 0) {
            const randomErrorIndex = Math.floor(
                Math.random() * stats.num_error_events
            );
            console.log(`⚠️ Fetching error event at index ${randomErrorIndex}`);
            
            document.getElementById("error-event-heading").innerText = 
                `⚠️ Error Event (Index ${randomErrorIndex})`;
            
            const errorUrl = `${ANALYZER_ERROR_API_BASE}?index=${randomErrorIndex}`;
            makeReq(errorUrl, (result) => {
                updateCodeDiv(result, "event-error");
            });
        } else {
            document.getElementById("event-error").innerText = 
                "No error events available";
            document.getElementById("error-event-heading").innerText = 
                "⚠️ Error Event (No data)";
        }
    });
    
    // ========================================================================
    // HEALTH CHECK SERVICE
    // ========================================================================
    makeReq(HEALTH_CHECK_API_URL, (result) => {
        console.log("💚 Health status received");
        updateHealthStatus(result);
    });
};

/**
 * Display error messages to the user
 */
const updateErrorMessages = (message) => {
    const id = Date.now();
    
    const msg = document.createElement("div");
    msg.id = `error-${id}`;
    msg.innerHTML = `
        <p>⚠️ Error at ${getLocaleDateStr()}</p>
        <code>${message}</code>
    `;
    
    const messagesDiv = document.getElementById("messages");
    messagesDiv.style.display = "block";
    messagesDiv.prepend(msg);
    
    // Auto-remove after 7 seconds
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`);
        if (elem) {
            elem.remove();
        }
        if (messagesDiv.children.length === 0) {
            messagesDiv.style.display = "none";
        }
    }, 7000);
};

/**
 * Initialize the dashboard
 */
const setup = () => {
    console.log("🚀 Dashboard initialized");
    getStats();
    // Update every 3 seconds
    // This also updates the "seconds ago" display in real-time
    setInterval(() => getStats(), 3000);
};

// Wait for DOM to be fully loaded
document.addEventListener('DOMContentLoaded', setup);