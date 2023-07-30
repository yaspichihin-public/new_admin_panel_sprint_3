import logging
from etl_app.config import settings

# handlers
console_out = logging.StreamHandler()

# config
logging.basicConfig(
    handlers=(console_out,),
    level=settings.DEBUG_LEVEL,
    format='%(asctime)s %(levelname)s %(funcName)s %(message)s',
    # format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8',
)

logger = logging.getLogger('etl_app')
