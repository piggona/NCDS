# date:2019/04/08
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

'''
Gets article-ctr information.
1. single expire-time cycle article-ctr distribution
2. accumulated article-ctr distribution
target:
1. 可以进行<code>date-month-year<code>累计计算（存储有累计值）
2. 可以指定时间段计算(random time interval)
'''
class Article:
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
    
    '''
    Gets article-related attributes from database infomation.
    !Every two days runs once.
    '''
    def get_accu_article_info(self):
        sql = """
        INSERT INTO accu_article_info
        SELECT*FROM (
        SELECT*FROM (
        SELECT id,title,expire_time,category,tags,extend-> "$.source" as source FROM infomation.article_resource WHERE expire_time> UNIX_TIMESTAMP(NOW())) AS a JOIN (
        SELECT item_id,bhv_type,bhv_value,user_id,bhv_time FROM infomation.aliyun_behavior_info WHERE bhv_time> UNIX_TIMESTAMP(NOW()-INTERVAL 2 DAY)) AS b ON a.id=b.item_id) AS a_outer,(SELECT DATE_FORMAT(NOW(),'%y-%m-%d') AS operate_date) AS b_outer
        """
        self.cursor.execute(sql)
    
    '''
    Displays how many articles users read/click in specified date interval.
    @param time time-start-selection datetime
    @param range time-range unix_timestamp
    '''
    def display_read_article_count(self,end_time=ARTICLE_START_TIME,time_range=DEFAULT_TIME_RANGE):
        result = {"exposed_article":"","clicked_article":""}
        if time == "NOW()":
            start_time = int(time.time()) - time_range
        else:
            start_time = int(time.mktime(string_toDatetime(end_time).timetuple())) - time_range
        if time_range <= 172800:
            sql = """
            SELECT count(1) as read_count FROM (
            SELECT DISTINCT id FROM accu_article_info WHERE operate_date=date_format({0},'%y-%m-%d') AND bhv_time< UNIX_TIMESTAMP({1}) AND bhv_time> '{2}') as distinct_id
            """.format(end_time,end_time,start_time)
            self.cursor.execute(sql)
            exposed_article = self.cursor.fetchall()[0]
            sql = """
            SELECT count(1) as click_article_count FROM (
            SELECT DISTINCT id FROM accu_article_info WHERE bhv_type='click' AND operate_date=date_format({0},'%y-%m-%d') AND bhv_time< UNIX_TIMESTAMP({1}) AND bhv_time> '{2}') AS click
            """.format(end_time,end_time,start_time)
            self.cursor.execute(sql)
            clicked_article = self.cursor.fetchall()[0]
        else:
            sql = """
            SELECT count(1) as read_count FROM (
            SELECT DISTINCT id FROM accu_article_info WHERE bhv_time< UNIX_TIMESTAMP({0}) AND bhv_time> '{1}') as distinct_id
            """.format(end_time,start_time)
            self.cursor.execute(sql)
            exposed_article = self.cursor.fetchall()[0]
            sql = """
            SELECT count(1) as click_article_count FROM (
            SELECT DISTINCT id FROM accu_article_info WHERE bhv_type='click' AND bhv_time< UNIX_TIMESTAMP({0}) AND bhv_time> '{1}') AS click
            """.format(end_time,start_time)
            self.cursor.execute(sql)
            clicked_article = self.cursor.fetchall()[0]
        result["exposed_article"] = exposed_article
        result["clicked_article"] = clicked_article
        return result
    
    '''
    Displays how many users active/click in specified date interval.
    @param time time-start-selection datetime
    @param range time-range unix_timestamp
    '''
    def display_active_user(self,end_time=ARTICLE_START_TIME,time_range=DEFAULT_TIME_RANGE):
        result = {"active_user_count":"","click_user_count":""}
        if time == "NOW()":
            start_time = int(time.time()) - time_range
        else:
            start_time = int(time.mktime(string_toDatetime(end_time).timetuple())) - time_range
        if time_range <= 172800:
            sql = """
            SELECT count(1) as active_user FROM (SELECT DISTINCT user_id FROM accu_article_info WHERE operate_date=date_format({0},'%y-%m-%d') AND bhv_time< UNIX_TIMESTAMP({1}) AND bhv_time> '{2}') as distinct_user
            """.format(end_time,end_time,start_time)
            self.cursor.execute(sql)
            active_user_count = self.cursor.fetchall()[0]
            sql = """
            SELECT count(1) FROM (SELECT DISTINCT user_id FROM accu_article_info WHERE bhv_type = 'click' AND operate_date=date_format({0},'%y-%m-%d') AND bhv_time< UNIX_TIMESTAMP({1}) AND bhv_time> '{2}') as click
            """.format(end_time,end_time,start_time)
            self.cursor.execute(sql)
            click_user_count = self.cursor.fetchall()[0]
        else:
            sql = """
            SELECT count(1) as active_user FROM (SELECT DISTINCT user_id FROM accu_article_info WHERE bhv_time< UNIX_TIMESTAMP({0}) AND bhv_time> '{1}') as distinct_user
            """.format(end_time,start_time)
            self.cursor.execute(sql)
            active_user_count = self.cursor.fetchall()[0]
            sql = """
            SELECT count(1) FROM (SELECT DISTINCT user_id FROM accu_article_info WHERE bhv_type = 'click' AND bhv_time< UNIX_TIMESTAMP({0}) AND bhv_time> '{1}') as click
            """.format(end_time,start_time)
            self.cursor.execute(sql)
            click_user_count = self.cursor.fetchall()[0]
        result["active_user_count"] = active_user_count
        result["click_user_count"] = click_user_count
        return result


    def display_total_ctr(self,end_time=ARTICLE_START_TIME,time_range=DEFAULT_TIME_RANGE):
        if end_time == "NOW()":
            start_time = int(time.time()) - time_range
        else:
            start_time = int(time.mktime(end_time)) - time_range
        if time_range <= 172800:
            sql="""
            SELECT bhv_type,count(*) as number FROM accu_article_info WHERE operate_date=date_format({0},'%y-%m-%d') AND bhv_time< UNIX_TIMESTAMP({1}) AND bhv_time> '{2}' GROUP BY bhv_type
            """.format(end_time,end_time,start_time)
            self.cursor.execute(sql)
            items = self.cursor.fetchall()
        else:
            sql="""
            SELECT bhv_type,count(*) as number FROM accu_article_info WHERE bhv_time< UNIX_TIMESTAMP({0}) AND bhv_time> '{1}' GROUP BY bhv_type
            """.format(end_time,start_time)
            self.cursor.execute(sql)
            items = self.cursor.fetchall()
        for item in items:
            if (item[0] == 'click'):
                click_count = item[1]
            elif (item[0] == 'expose'):
                expose_count = item[1]
        total_ctr = {"total_ctr:":click_count / expose_count,"click_count":click_count,"expose_count":expose_count}
        return total_ctr
    

    '''
    Gets article-ctr-distribution of articles which had not expired in selected time.
    1. gets article who was not expired.
    2. save them to MongoDB for further use.
    3. calculate article-ctr-distribution
    @params time selected-time mysql-datetime
    '''
    def available_article_distribution(self,time=ARTICLE_START_TIME):
        sql = """
        SELECT count()
        """.format(time,time)
        self.cursor.execute(sql)
        items = self.cursor.fetchall()

if __name__ == "__main__":
    pass
