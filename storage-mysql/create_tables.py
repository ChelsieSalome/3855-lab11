from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
import yaml
import logging
import logging.config
import time


# Load configuration
with open('/config/storage_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

db_conf = app_config['datastore']

with open("/config/storage_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

# MySQL connection string
DATABASE_URL = (
    f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}"
    f"@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"
)

logger.info(f"Database URL configured for {db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}")

# ==========================================
# ISSUE 4 FIX: Database Connection Pooling
# ==========================================
# The problem: After a while, MySQL closes idle connections or the pool gets exhausted
# Solution: Configure SQLAlchemy connection pool to handle this properly
#
# Key settings:
# - pool_size: Number of persistent connections to maintain
# - pool_recycle: Recycle connections every N seconds (default MySQL timeout is 28800s/8hrs)
# - pool_pre_ping: Test connection before using it; reconnect if dead
# - max_overflow: Allow extra connections above pool_size if needed

engine = create_engine(
    DATABASE_URL,
    echo=False,
    
    # FIX FOR ISSUE 4: Pool configuration
    pool_size=10,              # Keep 10 persistent connections in the pool
    max_overflow=20,           # Allow up to 20 additional connections above pool_size
    pool_recycle=3600,         # Recycle connections every 1 hour (before 8-hour timeout)
    pool_pre_ping=True,        # Test connection before using (reconnect if dead)
    connect_args={'connect_timeout': 10}  # Connection timeout of 10 seconds
)

logger.info("✅ Database engine created with connection pooling configured")
logger.info("   Pool settings: size=10, max_overflow=20, recycle=3600s, pre_ping=True")


def make_session():
    """
    Create and return a new database session.
    
    FIX FOR ISSUE 4: Session will use the pooled connections with proper recycling
    """
    Session = sessionmaker(bind=engine)
    return Session()


def init_db(retries=10, delay=5):
    """
    Wait for MySQL to be ready, then create tables.
    Called explicitly at startup — NOT at import time.
    
    This addresses ISSUE 1 (dependencies) by waiting for MySQL to be ready
    before initializing the app.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"MySQL connection attempt {attempt}/{retries}...")
            # This actually tests the connection
            Base.metadata.create_all(engine)
            logger.info("✅ MySQL connected and tables created successfully")
            return True
        except Exception as e:
            logger.warning(f"MySQL not ready (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("❌ Could not connect to MySQL after all retries. Exiting.")
                raise

if __name__ == "__main__":
    init_db()