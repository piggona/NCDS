# date:2019/4/21
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import pymysql
from sshtunnel import SSHTunnelForwarder
import time
import pandas as pd

from REC.config.basic import *
from REC.logs.logger import *
from REC.aspects.IsConn import *


'''
Fetchs article_data from database then structure.
fetch from: mysql-online(Aliyun RDS), db: dp_bi, table: article_ctr,article_ctr_all
structure: 清洗
@param mode chooses a connecttion mode from local&ssh
@output: dataFrame
'''
class FetchData:
    def __init__(self):
        self.is_conn = False
    
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
            server_online = SSHTunnelForwarder(
                ssh_address_or_host=(SSH_IP, SSH_PORT),  # 指定ssh登录的跳转机的address
                ssh_username=SSH_USER_NAME,  # 跳转机的用户
                ssh_pkey=SSH_PEM_PATH,  # 跳转机的密码
                remote_bind_address=(ONLINE_DB_HOST, 3306))
            server.start()
            self.conn_online = pymysql.connect(
                    user="fuyu",
                    passwd="Sjfy0114!!",
                    host="127.0.0.1",  # 此处必须是 127.0.0.1
                    db=TMP_DB,
                    port=server.local_bind_port)
            self.cursor_online =self.conn_online.cursor()
        info_log("连接到mysql")
        self.is_conn = True

    def kill_conn(self):
        self.cursor.close()
        self.cursor_online.close()
        self.conn.close()
        self.conn_online.close()
        print("数据库连接断开...")
        self.is_conn = False
        

    def frame(self,content):
        print("-----------------")
        print("|---{}---|".format(content))
        print("-----------------")
        print("  ")
        print("  ")

    @isConn()
    def fetch_source_data(self):
        vec_info_log("获取关于作者数据...(四天内）")
        self.conn.ping(reconnect=True)
        sql = """
        SELECT*FROM (
        SELECT extend-> "$.source" AS source,count(*) as article_count,sum(CASE WHEN expose_num !='' THEN expose_num ELSE 0 END) AS expose_num,sum(CASE WHEN click_num !='' THEN click_num ELSE 0 END) AS click_num,sum(CASE WHEN click_num !='' THEN click_num ELSE 0 END)/sum(CASE WHEN expose_num !='' THEN expose_num ELSE 0 END) AS source_ctr FROM article_ctr_all WHERE date_sub(CURDATE(),INTERVAL 4 DAY) <= DATE(dat) GROUP BY source ORDER BY source_ctr DESC) AS a WHERE a.source_ctr>=0 and a.source_ctr<=1;
        """
        source_dataframe = pd.read_sql(sql,self.conn)
        vec_info_log("fetch_source_data OK!")
        return source_dataframe

    '''
    获取source_detail.
    '''
    @isConn()
    def fetch_bias_data(self):
        vec_info_log("获取article_ctr_all特殊向量的数据...（四天内）")
        self.conn.ping(reconnect=True)
        sql="""
        SELECT pt,item_id,expose_num,click_num,ctr,title,tags,extend->"$.source" AS source,dat,url,category FROM article_ctr_all WHERE date_sub(CURDATE(),INTERVAL 4 DAY) <= DATE(dat);
        """
        source_detail = pd.read_sql(sql,self.conn)
        # print(source_detail)
        print("fetch_bias_data OK!")
        vec_info_log("fetch_bias_data OK!")
        return source_detail
    
    '''
    获取训练数据
    '''
    @isConn()
    def fetch_vec_data(self):
        vec_info_log("获取article_ctr文章的数据...（九天内）")
        self.conn.ping(reconnect=True)
        sql = """
        SELECT pt,item_id,expose_num,click_num,ctr,title,tags,extend->"$.source" AS source,dat,url,category FROM article_ctr WHERE date_sub(CURDATE(),INTERVAL 9 DAY) <= DATE(dat);
        """
        source_vec = pd.read_sql(sql,self.conn)
        print("fetch_vec_data OK!")
        vec_info_log("fetch_vec_data OK!")
        return source_vec


    '''
    method为block则分块进行yield bunch
    '''
    def fetch_data_from_sql(self,method='total'):
        pass
    
    def fetch_testing_data_from_sql(self,method='total'):
        pass

    '''
    Patches up unpropriate data.
    Unpropriate data:
    NaN data
    '''
    def data_cleaning():
        pass

if __name__ == "__main__":
    fetch_data =fetch_data("ssh")
    fetch_data.fetch_source_data()

