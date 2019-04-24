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
from REC.aspects.IsConn import *
from REC.aspects.IsArticleGot import *

class OnlineOutput:
    def __init__(self):
        self.article_frame = ""
        self.SpecialVec = Bunch(source_pos=np.array([1, 2, 3]), source_neg=np.array(
            [1, 2, 3]), source_channel_pos=[], source_channel_neg=[], channel_pos=np.array([1, 2, 3]), channel_neg=np.array([1, 2, 3]))
        self.is_conn = False
        self.is_article = False

    def connect_sql(self,mode="local"):
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
        self.is_conn = True

    '''
    需要添加是否connect_sql
    '''
    @isConn_no()
    def put_work(self,result_vec):
        info_log("put on online work...")
        self.vector_manager(result_vec)
        self.put_weight()
        self.kill_conn()
        info_log("put_work OK!")
    
    '''
    取一个小时的新文章数据
    '''
    @isConn_no()
    def get_article(self):
        info_log("get_article")
        sql = """
        SELECT id,site_id,title,category as channel,tags,extend-> '$.source' AS source FROM infomation.article_resource WHERE create_time> (UNIX_TIMESTAMP(NOW())-1800)
        """
        article_frame = pd.read_sql(sql,self.conn_online)
        self.is_article = True
        return article_frame
        info_log("get_article OK!")
        
    def kill_conn(self):
        self.cursor.close()
        self.cursor_online.close()
        self.conn.close()
        self.conn_online.close()
        print("数据库连接断开...")
        self.is_conn = False
    
    '''
    !!!应当被放置到模型模块中，模型最终输出result_pos与result_neg
    '''
    @isArticleGot()
    def vector_manager(self,result_vec):
        info_log("vector_manager")
        self.result_pos = result_vec['positive']
        self.result_neg = result_vec['negative']
        info_log("vector_manager OK!")
    
    @isConn_no()
    def put_weight(self):
        info_log("put_weight")
        # print(result_neg)
        # print(result_pos)
        now = int(time.time())
        for item in self.result_pos:  
            sql = """
            UPDATE infomation.article_resource SET weight = 100,update_time = '{0}' WHERE id = '{1}';
            """.format(now,item)
            self.cursor_online.execute(sql)
            self.conn_online.commit()
        info_log("modified {} articles as positive.".format(len(self.result_pos)))
        for item in self.result_neg:
            sql = """
            UPDATE infomation.article_resource SET weight = 1,update_time = '{0}' WHERE id = '{1}';
            """.format(now,item)
            self.cursor_online.execute(sql)
            self.conn_online.commit()
        info_log("modified {} articles as negative.".format(len(self.result_neg)))
        info_log("put time: "+str(now))
        info_log("put_weight OK!")
    

        