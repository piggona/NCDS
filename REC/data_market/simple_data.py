# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import pymysql
import time

from REC.config.basic import *

'''
Fetchs article_data from database then structure.
fetch from: mysql-online(Aliyun RDS), db: dp_bi, table: article_ctr 
data: title,tags,source,summary
structure: 清洗，分词，卡方，稀疏存储压缩
'''
class fetch_data:
    def __init__(self):
        self.conn = pymysql.connect(
            host=TMP_DB_HOST,
            port=TMP_DB_PORT, db=TMP_DB, user=TMP_DB_USER, password=TMP_DB_PSWD,
            charset='utf8')
        self.cursor = self.conn.cursor()  # 建立游标

        self.conn_online = pymysql.connect(
            host=ONLINE_DB_HOST, port=ONLINE_DB_PORT, user=ONLINE_DB_USER, db=ONLINE_DB, passwd=ONLINE_DB_PSWD)
        self.cursor_online = self.conn_online.cursor()
    
    def fetch_data_from_sql():
        pass

    def data_structure_scheduler():
        pass

