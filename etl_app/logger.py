import os
import logging
from dotenv import load_dotenv

load_dotenv()

# handlers
console_out = logging.StreamHandler()

logging.basicConfig(
    handlers=(console_out,),
    level=os.environ.get('DEBUG_LEVEL'),
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8',
)

logger = logging.getLogger('etl_app')
