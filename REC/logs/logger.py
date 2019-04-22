import logging
import logging.handlers
import os

def info_log(content):
    path = os.getcwd() + '/REC/logs/REC.log'
    logger = logging.getLogger('REC')
    if not logger.handlers:
        LOG_FILE = path
        handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5, encoding='utf-8')
        fmt = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        logger = logging.getLogger('book_tool')
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    logger.info(content)
    