# date:2019/4/21
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import pymysql
from sshtunnel import SSHTunnelForwarder
import time
import pickle
import multiprocessing
import pandas as pd
import numpy as np
from sklearn.datasets.base import Bunch

from REC.config.basic import *
from REC.logs.logger import *

result_pos = []
result_neg = []

def _read_bunch(path):
    with open(path, "rb") as f:
        content = pickle.load(f)
    f.close()
    return content

def get_pos_channel(a,b,channel_pos):
    if str(a) in channel_pos:
        result_pos.append(b)

def get_neg_channel(a,b,channel_neg):
    if str(a) in channel_neg:
        result_neg.append(b)

class OnlineOutput:
    def __init__(self,mode="local"):
        if mode == "local":
            self.conn = pymysql.connect(
                host=TMP_DB_HOST,
                port=TMP_DB_PORT, db=TMP_DB, user=TMP_DB_USER, password=TMP_DB_PSWD,
                charset='utf8')
            self.cursor = self.conn.cursor()  # 建立游标

            self.conn_online = pymysql.connect(
                host=ONLINE_DB_HOST, port=ONLINE_DB_PORT, user=ONLINE_DB_USER, db=ONLINE_DB, passwd=ONLINE_DB_PSWD)
            self.cursor_online = self.conn_online.cursor()
        elif mode == "ssh":
            server = SSHTunnelForwarder(
                ssh_address_or_host=(SSH_IP, SSH_PORT),  # 指定ssh登录的跳转机的address
                ssh_username=SSH_USER_NAME,  # 跳转机的用户
                ssh_pkey=SSH_PEM_PATH,  # 跳转机的密码
                remote_bind_address=('127.0.0.1', 3306))
            server.start()
            self.conn = pymysql.connect(
                    user="fuyu",
                    passwd="Sjfy0114!!",
                    host="127.0.0.1",  # 此处必须是 127.0.0.1
                    db=TMP_DB,
                    port=server.local_bind_port)
            self.cursor =self.conn.cursor()
            # self.conn_online = pymysql.connect(
            #     host=ONLINE_DB_HOST, port=ONLINE_DB_PORT, user=ONLINE_DB_USER, db=ONLINE_DB, passwd=ONLINE_DB_PSWD)
            server_online = SSHTunnelForwarder(
                ssh_address_or_host=(SSH_IP, SSH_PORT),  # 指定ssh登录的跳转机的address
                ssh_username=SSH_USER_NAME,  # 跳转机的用户
                ssh_pkey=SSH_PEM_PATH,  # 跳转机的密码
                remote_bind_address=('127.0.0.1', 3306))
            server.start()
            self.conn_online = pymysql.connect(
                    user=ONLINE_DB_USER,
                    passwd=ONLINE_DB_PSWD,
                    host=ONLINE_DB_HOST,  # 此处必须是 127.0.0.1
                    db=TMP_DB,
                    port=server.local_bind_port)
            self.cursor_online =self.conn_online.cursor()
        info_log("连接到mysql")
        self.article_frame = ""
        self.SpecialVec = Bunch(source_pos=np.array([1, 2, 3]), source_neg=np.array(
            [1, 2, 3]), source_channel_pos=[], source_channel_neg=[], channel_pos=np.array([1, 2, 3]), channel_neg=np.array([1, 2, 3]))

    def put_work(self):
        info_log("put on online work...")
        self.get_bunch()
        self.get_article()
        self.vector_manager()
        self.put_weight()
        self.kill_conn()
        info_log("OK!")

    def get_bunch(self):
        info_log("get_bunch")
        path = os.getcwd() + '/REC/static/specialVec.dat'
        self.SpecialVec = _read_bunch(path)
        print(self.SpecialVec.channel_pos)
        print(self.SpecialVec.channel_neg)
        info_log("OK!")
    
    '''
    取一个小时的新文章数据
    '''
    def get_article(self):
        info_log("get_article")
        sql = """
        SELECT id,site_id,title,category,tags,extend-> '$.source' AS source FROM infomation.article_resource WHERE create_time> (UNIX_TIMESTAMP(NOW())-1800)
        """
        self.article_frame = pd.read_sql(sql,self.conn_online)
        info_log("OK!")
        
    def kill_conn(self):
        self.cursor.close()
        self.cursor_online.close()
        self.conn.close()
        self.conn_online.close()
    
    def vector_manager(self):
        info_log("vector_manager")
        af = self.article_frame
        channel_pos = self.SpecialVec.channel_pos
        channel_neg = self.SpecialVec.channel_neg
        af.apply(lambda row: get_pos_channel(row['category'],row['id'],channel_pos),axis=1)
        af.apply(lambda row: get_neg_channel(row['category'],row['id'],channel_neg),axis=1)
        info_log("OK!")
    
    def put_weight(self):
        info_log("put_weight")
        print(result_neg)
        print(result_pos)
        for item in result_pos:
            sql = """
            UPDATE article_resource SET weight = 100,update_time = UNIX_TIMESTAMP(NOW()) WHERE id = '{}';
            """.format(item)
            self.cursor_online.execute(sql)
        for item in result_neg:
            sql = """
            UPDATE article_resource SET weight = 1,update_time = UNIX_TIMESTAMP(NOW()) WHERE id = '{}';
            """.format(item)
            self.cursor_online.execute(sql)
        info_log("OK!")
    

        