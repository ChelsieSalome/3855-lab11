const VM_IP = "172.169.248.121"

const HEALTH_CHECK_API_URL = `http://${VM_IP}:8120/health`
const PROCESSING_STATS_API_URL = `http://${VM_IP}:8100/stats`
const ANALYZER_API_URL = {
    stats: `http://${VM_IP}:5005/analyzer/stats`,
    performance: `http://${VM_IP}:5005/analyzer/performance?index=0`,
    error: `http://${VM_IP}:5005/analyzer/error?index=0`
}

/**
 * Generic fetch function to retrieve data from API endpoints
 * @param {string} url - The API endpoint URL
 * @param {function} callback - Function to call with the result
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
            console.log("Received data from " + url + ": ", result);
            cb(result);
        })
        .catch((error) => {
            console.error("Error fetching from " + url + ":", error);
            updateErrorMessages(error.message);
        });
};

/**
 * Update a code div with formatted JSON
 * @param {object} result - The data to display
 * @param {string} elemId - The element ID to update
 */
const updateCodeDiv = (result, elemId) => {
    document.getElementById(elemId).innerText = JSON.stringify(result, null, 2);
};

/**
 * Get current date and time as a formatted string
 * @returns {string} - Formatted date/time string
 */
const getLocaleDateStr = () => (new Date()).toLocaleString();

/**
 * Main function to fetch all statistics and update the dashboard
 */
const getStats = () => {
    console.log("🔄 Updating all statistics...");
    document.getElementById("last-updated-value").innerText = getLocaleDateStr();
    
    // Fetch Health Check Status (all services + last update time)
    makeReq(HEALTH_CHECK_API_URL, (result) => {
        updateCodeDiv(result, "service-health-stats");
    });
    
    // Fetch Processing Service Stats
    makeReq(PROCESSING_STATS_API_URL, (result) => {
        updateCodeDiv(result, "processing-stats");
    });
    
    // Fetch Analyzer Service Stats
    makeReq(ANALYZER_API_URL.stats, (result) => {
        updateCodeDiv(result, "analyzer-stats");
    });
    
    // Fetch a Performance Event (Index 0) - with label
    makeReq(ANALYZER_API_URL.performance, (result) => {
        const display = {
            "index": 0,
            "data": result
        };
        updateCodeDiv(display, "event-performance");
    });
    
    // Fetch an Error Event (Index 0) - with label
    makeReq(ANALYZER_API_URL.error, (result) => {
        const display = {
            "index": 0,
            "data": result
        };
        updateCodeDiv(display, "event-error");
    });
};

/**
 * Display error messages to the user
 * @param {string} message - The error message to display
 */
const updateErrorMessages = (message) => {
    const id = Date.now();
    console.log("Creating error message:", id);
    
    const msg = document.createElement("div");
    msg.id = `error-${id}`;
    msg.innerHTML = `<p>⚠️ Error occurred at ${getLocaleDateStr()}</p><code>${message}</code>`;
    
    const messagesDiv = document.getElementById("messages");
    messagesDiv.style.display = "block";
    messagesDiv.prepend(msg);
    
    // Auto-remove error message after 7 seconds
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`);
        if (elem) {
            elem.remove();
        }
        // Hide messages container if empty
        if (messagesDiv.children.length === 0) {
            messagesDiv.style.display = "none";
        }
    }, 7000);
};

/**
 * Initialize the dashboard - called when DOM is fully loaded
 */
const setup = () => {
    console.log("Dashboard initialized");
    // Fetch stats immediately
    getStats();
    // Update every 3 seconds
    setInterval(() => getStats(), 3000);
};

// Wait for DOM to be fully loaded before setting up
document.addEventListener('DOMContentLoaded', setup);