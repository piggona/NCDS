# date:2019/3/30
# -*- coding: utf-8 -*-
# auth：Haohao,fuyu

import pymysql
import pandas as pd
import numpy as np
import datetime
import time

from basic_bi.config.basic import *


class init_tables:
    def __init__(self, date=ANALYZE_DATE):
        self.analyze_date = date
        self.conn = pymysql.connect(
            host=TMP_DB_HOST,
            port=TMP_DB_PORT, db=TMP_DB, user=TMP_DB_USER, password=TMP_DB_PSWD,
            charset='utf8')
        self.cur = self.conn.cursor()  #建立游标

        # set date,get table name
        oneday = datetime.timedelta(days=1)
        d = str(self.analyze_date - oneday).replace('-','')
        self.new_table = 'fuyu.mine_user_action_'+ d 
        self.source_table = 'test_v2_log.mine_user_action_'+ d 

    # 留出接口对初始化做更多操作,比如让用户输入一个时间范围，
    # 这里可以生成对应时间去进行初始化操作
    @classmethod
    def set_date(cls, date):
        inited = cls(date)
        return inited
    
    def display_date(self):
        return self.analyze_date

    def display_new_table_action(self):
        return self.new_table

    def _toDataFrame(self, dat):
        return pd.DataFrame(list(dat))

    def stop_conn(self):
        self.cur.close()
        self.conn.close()

    def start_init(self):
        '''
        标准启动方法()
        '''
        self.create_new_table_action()
        self.create_normal_table()
        self.stop_conn()
    
    def create_new_table_action(self):
        oneday = datetime.timedelta(days=1)
        old = str(self.analyze_date - 3* oneday).replace('-','')

        old_table = 'fuyu.mine_user_action_'+ old

        sql = "drop table if exists %s;" % (old_table)
        self.cur.execute(sql)
        print("删除前前日日志表")

        # 复制昨日日志表
        sql = "drop table if exists %s;" % (self.new_table)
        self.cur.execute(sql)

        sql = "create table %s as select * from  %s ;" % (self.new_table,self.source_table)
        self.cur.execute(sql)
        print("复制昨日日志表")
    
    def create_normal_table(self):
        '''
        复制最新mine_user表/mine_withdraw表/mine_user_device表/mine_user_account_mibi_log表/mine_task_system_logs表
        !!存在未来隐患：复制最新的表就说明无法准确对应时间段内的数据
        !!需要在后面的sql部分明确时间
        '''
        sql = "drop table if exists fuyu.mine_user;"
        self.cur.execute(sql)

        sql = "create table fuyu.mine_user as select * from test_v2.mine_user ;"
        self.cur.execute(sql)
        print("更新mine_user")
        ##
        sql = "drop table if exists fuyu.mine_withdraw;" 
        self.cur.execute(sql)

        sql = "create table fuyu.mine_withdraw as select * from test_v2.mine_withdraw;" 
        self.cur.execute(sql)
        print("更新mine_withdraw")

        ##
        sql = "drop table if exists fuyu.mine_user_device;"
        self.cur.execute(sql)

        sql = "create table fuyu.mine_user_device as select * from test_v2.mine_user_device;" 
        self.cur.execute(sql)
        print("更新mine_user_device")

        ##
        sql = "drop table if exists fuyu.mine_user_account_mibi_log;"
        self.cur.execute(sql)

        sql = "create table fuyu.mine_user_account_mibi_log as select * from test_v2.mine_user_account_mibi_log;" 
        self.cur.execute(sql)
        print("更新mine_user_account_mibi_log")

        ## 
        sql = "drop table if exists fuyu.mine_task_system_logs;"
        self.cur.execute(sql)

        sql = "create table fuyu.mine_task_system_logs as select * from test_v2.mine_task_system_logs;" 
        self.cur.execute(sql)
        print("更新mine_task_system_logs")
