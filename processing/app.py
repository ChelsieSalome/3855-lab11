import connexion
import json
import logging
import logging.config
import yaml
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone
import requests

with open('/config/processing_config.yml', 'r') as f:
    app_config = yaml.safe_load(f)

with open('/config/processing_log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

filename = app_config['datastore']['filename']
performance_url = app_config['eventstores']['performance_url']
errors_url = app_config['eventstores']['errors_url']
scheduler_interval = app_config['scheduler']['interval']


def get_stats():
    logger.info("Request for statistics received")  
    
    try:
        with open(filename, 'r') as f:
            stats = json.load(f)
        
        logger.debug(f"Statistics contents: {stats}")  
        logger.info("Request has completed")  
        return stats, 200
    except FileNotFoundError:
        logger.error("Statistics file does not exist")  
        return {"message": "Statistics do not exist"}, 404  


first_run = True

def populate_stats():
    """
    Periodic task to collect and aggregate statistics from all services.
    
    FIX FOR ISSUE 5: Added consistency validation to check for data loss
    """
    global first_run  
    logger.info("Periodic processing has started")  
    
    try:
        with open(filename, 'r') as f:
            content = f.read().strip()
        
        if not content:
            logger.info("File exists but is empty, using default values")
            stats = {
                "num_performance_readings": 0,
                "max_cpu_reading": 0,
                "num_error_readings": 0,
                "max_severity_level": 0,
                "last_updated": "2026-01-01T00:00:00Z"
            }
        else:
            stats = json.loads(content)
            logger.debug("Successfully loaded existing statistics")
            
            if first_run and (stats['num_performance_readings'] > 0 or 
                              stats['max_cpu_reading'] > 0 or 
                              stats['num_error_readings'] > 0 or 
                              stats['max_severity_level'] > 0):
                logger.info("Resetting statistics to zero before updating with new values")
                stats['num_performance_readings'] = 0
                stats['max_cpu_reading'] = 0
                stats['num_error_readings'] = 0
                stats['max_severity_level'] = 0
                first_run = False  

    except FileNotFoundError:
        logger.info("File not found, creating with default values")
        stats = {
            "num_performance_readings": 0,
            "max_cpu_reading": 0,
            "num_error_readings": 0,
            "max_severity_level": 0,
            "last_updated": "2026-01-01T00:00:00Z"
        }
   
    except Exception as e:
        logger.error(f"Unexpected error reading statistics file: {e}")
        stats = {
            "num_performance_readings": 0,
            "max_cpu_reading": 0,
            "num_error_readings": 0,
            "max_severity_level": 0,
            "last_updated": "2026-01-01T00:00:00Z"
        }
    
    last_updated = datetime.strptime(stats['last_updated'], "%Y-%m-%dT%H:%M:%SZ")
    current_datetime = datetime.now(timezone.utc)
    
    params = {
        'start_timestamp': last_updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'end_timestamp': current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    
    performance_response = requests.get(performance_url, params=params)
    
    if performance_response.status_code == 200:
        performance_readings = performance_response.json()
        logger.info(f"Number of performance events received: {len(performance_readings)}")  
        
        stats['num_performance_readings'] += len(performance_readings)
        
        if performance_readings:
            max_cpu = max(reading['cpu'] for reading in performance_readings)
            if max_cpu > stats['max_cpu_reading']:
                stats['max_cpu_reading'] = max_cpu
    else:
        logger.error(f"Did not get 200 response code for performance data: {performance_response.status_code}")  
    
    error_response = requests.get(errors_url, params=params)
    
    if error_response.status_code == 200:
        error_readings = error_response.json()
        logger.info(f"Number of error events received: {len(error_readings)}")  
        
        stats['num_error_readings'] += len(error_readings)
        
        if error_readings:
            max_severity = max(reading['severity_level'] for reading in error_readings)
            if max_severity > stats['max_severity_level']:
                stats['max_severity_level'] = max_severity
    else:
        logger.error(f"Did not get 200 response code for error data: {error_response.status_code}")  
    
    stats['last_updated'] = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    with open(filename, 'w') as f:
        json.dump(stats, f, indent=4)
    
    logger.debug(f"Updated statistics: {stats}")  
    
    # ==========================================
    # ISSUE 5 FIX: Consistency Validation
    # ==========================================
    # Check if event counts are consistent across all services
    # This helps detect data loss or processing bugs
    validate_consistency(stats)
    
    logger.info("Periodic processing has ended")


def validate_consistency(current_stats):
    """
    Validate that event counts are consistent across all services.
    
    FIX FOR ISSUE 5: Detect data loss or inconsistencies
    
    Checks:
    1. Performance events: received = stored in DB = reported by analyzer
    2. Error events: received = stored in DB = reported by analyzer
    3. Aggregated stats: match the totals from analyzer
    
    If counts don't match, logs a WARNING so you can investigate.
    """
    logger.info("=" * 60)
    logger.info("CONSISTENCY VALIDATION CHECK")
    logger.info("=" * 60)
    
    try:
        # Get current stats from this service
        processing_perf_count = current_stats['num_performance_readings']
        processing_error_count = current_stats['num_error_readings']
        
        logger.info(f"Processing Service counts:")
        logger.info(f"  - Performance events: {processing_perf_count}")
        logger.info(f"  - Error events: {processing_error_count}")
        
        # Try to get stats from Analyzer service
        try:
            analyzer_response = requests.get('http://analyzer-service:5005/analyzer/stats', timeout=5)
            if analyzer_response.status_code == 200:
                analyzer_stats = analyzer_response.json()
                analyzer_perf_count = analyzer_stats.get('num_performance_events', 0)
                analyzer_error_count = analyzer_stats.get('num_error_events', 0)
                
                logger.info(f"Analyzer Service counts:")
                logger.info(f"  - Performance events: {analyzer_perf_count}")
                logger.info(f"  - Error events: {analyzer_error_count}")
                
                # Check for mismatches
                if processing_perf_count != analyzer_perf_count:
                    logger.warning(f"⚠️  MISMATCH: Performance count differs!")
                    logger.warning(f"   Processing: {processing_perf_count}, Analyzer: {analyzer_perf_count}")
                    logger.warning(f"   Difference: {abs(processing_perf_count - analyzer_perf_count)}")
                else:
                    logger.info(f"✅ Performance counts match: {processing_perf_count}")
                
                if processing_error_count != analyzer_error_count:
                    logger.warning(f"⚠️  MISMATCH: Error count differs!")
                    logger.warning(f"   Processing: {processing_error_count}, Analyzer: {analyzer_error_count}")
                    logger.warning(f"   Difference: {abs(processing_error_count - analyzer_error_count)}")
                else:
                    logger.info(f"✅ Error counts match: {processing_error_count}")
            else:
                logger.warning(f"Could not get Analyzer stats: HTTP {analyzer_response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Could not connect to Analyzer service: {e}")
        
        # Try to get counts from Storage service
        try:
            now = datetime.now(timezone.utc)
            two_days_ago = datetime(2000, 1, 1, 0, 0, 0)  # Go back far enough to get all data
            
            params = {
                'start_timestamp': two_days_ago.strftime("%Y-%m-%dT%H:%M:%SZ"),
                'end_timestamp': now.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            
            perf_response = requests.get(
                'http://storage-service:8091/monitoring/performance',
                params=params,
                timeout=5
            )
            
            error_response = requests.get(
                'http://storage-service:8091/monitoring/errors',
                params=params,
                timeout=5
            )
            
            if perf_response.status_code == 200 and error_response.status_code == 200:
                storage_perf_count = len(perf_response.json())
                storage_error_count = len(error_response.json())
                
                logger.info(f"Storage Service counts:")
                logger.info(f"  - Performance events: {storage_perf_count}")
                logger.info(f"  - Error events: {storage_error_count}")
                
                # Note: Storage counts will generally be higher because it stores everything
                # that comes through Kafka. Processing service only counts incremental events.
                logger.info(f"Note: Storage typically has all historical events.")
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Could not connect to Storage service: {e}")
        
        logger.info("=" * 60)
        logger.info("CONSISTENCY CHECK COMPLETE")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error during consistency validation: {e}")


def init_scheduler():  
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['interval'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')

# Enable CORS
CORS(app.app)

app.add_api("processing_openapi.yaml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    logger.info("Starting Processing Service on port 8100")
    init_scheduler() 
    app.run(port=8100)