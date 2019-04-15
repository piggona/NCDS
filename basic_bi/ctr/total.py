# date:2019/04/11
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import csv
import copy

from datetime import *
import pymongo
from odps import ODPS
import pymysql
import pandas as pd

import basic_bi.aspects.TimeSpent
from basic_bi.config.ctr import *
from basic_bi.config.basic import *
from basic_bi.aspects.TmpTableDep import *

class Total:
    def __init__(self):
        self.conn = pymysql.connect(
            host=TMP_DB_HOST,
            port=TMP_DB_PORT, db=TMP_CTR_DB, user=TMP_DB_USER, password=TMP_DB_PSWD,
            charset='utf8')
        self.cursor = self.conn.cursor()  # 建立游标

        self.conn_online = pymysql.connect(
            host=ONLINE_DB_HOST, port=ONLINE_DB_PORT, user=ONLINE_DB_USER, db=ONLINE_DB, passwd=ONLINE_DB_PSWD)
        self.cursor_online = self.conn_online.cursor()
    
    def stop_conn(self):
        self.cursor.close()
        self.conn.close()
        self.conn_online.close()
        self.cursor_online.close()
    
    def max_compute(self):
        print("=============================ODPS模式============================")
        self.odps = ODPS('LTAIMF0pnDg3k2cT','b33pMUvBuOXyqLL8SUQCSoEJtIpsCM','ODPS_BI','http://service.cn.maxcompute.aliyun.com/api')
        print("===========================ODPS句柄获得===========================")
        print(" ")
        print(" ")
        print("ODPS配置："+self.odps.options)