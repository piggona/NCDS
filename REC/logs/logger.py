# date:2019/4/22
# -*- coding: utf-8 -*-
# auth：Haohao

import logging
import logging.handlers
import os
import sys

def info_log(content):
    path = os.getcwd() + '/REC/logs/REC.log'
    logger = logging.getLogger('REC')
    if not logger.handlers:
        LOG_FILE = path
        handler = logging.StreamHandler(sys.stdout)
        fmt = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    logger.info(content)

def error_log(content):
    path = os.getcwd() + '/REC/logs/REC.log'
    logger = logging.getLogger('REC_ERROR')
    if not logger.handlers:
        LOG_FILE = path
        handler = logging.StreamHandler(sys.stdout)
        fmt = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    logger.error(content)

def vec_info_log(content):
    path = os.getcwd() + '/REC/logs/VEC.log'
    logger = logging.getLogger('VEC')
    if not logger.handlers:
        LOG_FILE = path
        handler = logging.StreamHandler(sys.stdout)
        fmt = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    logger.info(content)

def vec_error_log(content):
    path = os.getcwd() + '/REC/logs/VEC.log'
    logger = logging.getLogger('VEC_ERROR')
    if not logger.handlers:
        LOG_FILE = path
        handler = logging.StreamHandler(sys.stdout)
        fmt = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    logger.error(content)
    