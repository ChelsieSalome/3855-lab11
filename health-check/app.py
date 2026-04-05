import connexion
from flask_cors import CORS
import json
import logging
import logging.config
import yaml
from datetime import datetime, timezone
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import os

# ============================================================================
# CONFIGURATION & LOGGING SETUP
# ============================================================================

with open('/config/healthcheck_config.yml', 'r') as f:
    CONFIG = yaml.safe_load(f)

with open('/config/healthcheck_log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# Extract configuration values
DATASTORE_LOCATION = CONFIG['datastore']['location']
HEALTH_CHECK_INTERVAL = CONFIG['health_check']['interval_seconds']
TIMEOUT = CONFIG['health_check']['timeout_seconds']
SERVICES = CONFIG['services']

logger.info("Health Check Service configuration loaded")

# ============================================================================
# CORE FUNCTIONS - SERVICE HEALTH CHECKING
# ============================================================================

def check_service_health(service_name, service_config):
    """
    Check if a single service is healthy by calling its /health endpoint.
    
    Args:
        service_name (str): Name of service (e.g., 'receiver')
        service_config (dict): Service config with 'url' key
    
    Returns:
        str: Either "Up" or "Down"
    """
    try:
        response = requests.get(
            service_config['url'],
            timeout=TIMEOUT
        )
        
        if response.status_code == 200:
            logger.info(f"✓ {service_name.upper()} service is UP")
            return "Up"
        else:
            logger.warning(
                f"✗ {service_name.upper()} service returned HTTP {response.status_code}"
            )
            return "Down"
    
    except requests.exceptions.Timeout:
        logger.warning(
            f"✗ {service_name.upper()} service TIMEOUT (>{TIMEOUT}s)"
        )
        return "Down"
    
    except requests.exceptions.ConnectionError as e:
        logger.warning(
            f"✗ {service_name.upper()} service CONNECTION ERROR: {e}"
        )
        return "Down"
    
    except Exception as e:
        logger.error(
            f"✗ {service_name.upper()} service ERROR: {str(e)}"
        )
        return "Down"


# ============================================================================
# CORE FUNCTIONS - STATUS UPDATE & PERSISTENCE
# ============================================================================

def update_health_status():
    """
    Poll all services and update the datastore with their current status.
    
    This function is:
    - Called periodically by APScheduler (every 5 seconds)
    - Responsible for checking each service's /health endpoint
    - Updates the JSON datastore with results
    - Logs each status change (requirement)
    """
    logger.info("=" * 50)
    logger.info("STARTING HEALTH CHECK CYCLE")
    logger.info("=" * 50)
    
    # Load existing datastore or create default
    try:
        with open(DATASTORE_LOCATION, 'r') as f:
            health_status = json.load(f)
            logger.debug(f"Loaded existing datastore from {DATASTORE_LOCATION}")
    
    except FileNotFoundError:
        logger.warning(
            f"Datastore not found at {DATASTORE_LOCATION}, creating new"
        )
        health_status = {
            service: {
                "status": "Unknown",
                "last_check": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            for service in SERVICES.keys()
        }
    
    except Exception as e:
        logger.error(f"Error loading datastore: {e}, using defaults")
        health_status = {
            service: {
                "status": "Unknown",
                "last_check": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            for service in SERVICES.keys()
        }
    
    # Current timestamp for all checks in this cycle
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Check each service
    for service_name, service_config in SERVICES.items():
        status = check_service_health(service_name, service_config)
        
        # Update the status dict
        health_status[service_name] = {
            "status": status,
            "last_check": current_time
        }
        
        # REQUIREMENT: Log each time you record the status of a service
        logger.info(
            f"RECORDED: {service_name.upper():12} = {status:4} at {current_time}"
        )
    
    # Save back to datastore
    try:
        # Ensure directory exists (idempotent)
        os.makedirs(os.path.dirname(DATASTORE_LOCATION), exist_ok=True)
        
        with open(DATASTORE_LOCATION, 'w') as f:
            json.dump(health_status, f, indent=2)
        
        logger.debug("Datastore saved successfully")
    
    except Exception as e:
        logger.error(f"Failed to write datastore: {e}")
    
    logger.info("=" * 50)
    logger.info("HEALTH CHECK CYCLE COMPLETE")
    logger.info("=" * 50)


# ============================================================================
# API ENDPOINT - GET HEALTH STATUS
# ============================================================================

def get_health_status():
    """
    API Endpoint: GET /healthcheck/health-status
    
    Returns the current health status of all services.
    
    Response Format (as required by assignment):
    {
        "receiver": "Up",
        "storage": "Down",
        "processing": "Up",
        "analyzer": "Up",
        "last_update": "2026-03-22T11:12:23Z"
    }
    
    REQUIREMENT: Log each time the health status is retrieved through the API
    """
    logger.info("API REQUEST: GET /health-status")
    
    try:
        with open(DATASTORE_LOCATION, 'r') as f:
            health_status = json.load(f)
        
        # Transform from internal format to API format
        response = {}
        latest_timestamp = None
        
        # Add each service's status
        for service_name, service_data in health_status.items():
            response[service_name] = service_data['status']
            
            # Track the most recent check time
            if latest_timestamp is None or service_data['last_check'] > latest_timestamp:
                latest_timestamp = service_data['last_check']
        
        # Add last update timestamp
        response['last_update'] = latest_timestamp if latest_timestamp else "Unknown"
        
        # REQUIREMENT: Log that API was called
        logger.info(f"API RESPONSE: {response}")
        
        return response, 200
    
    except FileNotFoundError:
        logger.error("Datastore file not found - health check may not have run yet")
        return {"message": "Health status not available yet"}, 503
    
    except Exception as e:
        logger.error(f"Error retrieving health status: {e}")
        return {"message": "Error retrieving health status"}, 500


# ============================================================================
# SCHEDULER SETUP
# ============================================================================

def init_scheduler():
    """
    Initialize and start the background scheduler.
    
    The scheduler runs update_health_status() periodically
    (every HEALTH_CHECK_INTERVAL seconds, which is 5 seconds from config).
    """
    logger.info(f"Initializing scheduler with {HEALTH_CHECK_INTERVAL}s interval")
    
    try:
        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(
            update_health_status,
            'interval',
            seconds=HEALTH_CHECK_INTERVAL,
            id='health_check_job',
            name='Periodic health check of all services'
        )
        scheduler.start()
        logger.info("✓ Scheduler started successfully")
    
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        raise


# ============================================================================
# FLASK/CONNEXION APP SETUP
# ============================================================================

# Create Connexion app (wraps Flask)
app = connexion.FlaskApp(__name__, specification_dir='')

# Enable CORS so dashboard can call this API from different origin
CORS(app.app)

# Add the OpenAPI specification
app.add_api(
    'openapi.yaml',
    base_path='/healthcheck',
    strict_validation=True,
    validate_responses=True
)

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("STARTING HEALTH CHECK SERVICE")
    logger.info(f"Listening on 0.0.0.0:{CONFIG['app']['port']}")
    logger.info("=" * 60)
    
    # Start the background scheduler BEFORE the web server
    init_scheduler()
    
    # Start the Flask/Connexion API server

    app.run(
        host='0.0.0.0',
        port=CONFIG['app']['port']
    )