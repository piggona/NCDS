# date:2019/4/21
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import pickle
import multiprocessing
import numpy as np
import pandas as pd
from sklearn.datasets.base import Bunch

from REC.config.basic import *
from REC.utils.frame import *
from REC.logs.logger import *
from REC.aspects.IsStrategy import *

from REC.data_market.SimpleData import FetchData
from REC.data_output.OnlineOutput import OnlineOutput
from REC.models.SimpleStrategy import SimpleStrategy

def _writebunchobj(path, bunchobj):
    with open(path, "wb") as f:
        pickle.dump(bunchobj, f)

class Scheduler:
    def __init__(self):
        frame("Welcome to REC")

        print("开始初始化fetch_data...")
        info_log("开始初始化fetch_data...")
        self.SimpleData = FetchData()

        print("开始初始化OnlineOutput...")
        info_log("开始初始化OnlineOutput...")
        self.OnlineOutput = OnlineOutput()

        self.Strategy = ""

        self.page = ""
        self.isPageGot = False
        self.isTrained = False
    
    def init_strategy(self,mode="simple"):
        '''
        Fetchs a model strategy.
        @param mode String Mode name
        '''
        if mode == "simple":
            self.Strategy = SimpleStrategy(SIMPLE_MODEL_PATH)

    def train_simple(self):
        '''
        Trains model with data from data_market.
        '''
        while True:
            print("启动模型...")
            vec_info_log("启动模型...")
            try:
                vec_info_log("get_train_vec...")
                # 获取相应Strategy->train需要的DataFrame数据：
                # 获取source
                source = self.SimpleData.fetch_source_data()
                # 获取source_detail
                source_detail = self.SimpleData.fetch_bias_data()
                # 获取source_vec
                source_vec = self.SimpleData.fetch_vec_data()
                # 训练可用模型，存储在Strategy对象中
                self.Strategy.train(source,source_detail,source_vec)
                self.kill_data_conn()
                vec_info_log("get_train_vec OK!")
                self.isTrained = True
            except Exception as e:
                print(e)
                error_log("Scheduler-line68")
                vec_error_log(e)
            print("等待1day...")
            vec_info_log("等待1day...")
            time.sleep(TRAIN_SLEEP)
    
    def process_sp(self):
        while True:
            print("启动筛选器...")
            info_log("启动筛选器...")
            try:
                info_log("online_output...")
                refresh_data = self.OnlineOutput.get_article()
                result_vec = self.Strategy.judge(refresh_data)
                self.OnlineOutput.put_work(result_vec)
                info_log("online_output OK!")
            except Exception as e:
                print(e)
                error_log("Scheduler-line86")
                error_log(e)
            print("等待30min...")
            info_log("等待30min...")
            time.sleep(PROCESS_SLEEP)
    
    def process_article(self):
        print("启动筛选器...")
        info_log("启动筛选器...")
        try:
            if not self.isTrained:
                print("No trained vector yet. Wait for 10 minutes...")
                info_log("No trained vector yet. Wait for 10 minutes...")
                time.sleep(600)
            while True:
                info_log("online_output...")
                # 数据 Dataframe
                refresh_data = self.OnlineOutput.get_article()
                # 判别(judge) {"positive":result_pos,"negative":result_neg}
                result_vec = self.Strategy.mlp_judge(refresh_data)
                # 设置权值
                self.OnlineOutput.put_work(result_vec)
                self.kill_output_conn()
                info_log("online_output OK!")
                print("等待30min...")
                info_log("等待30min...")
                time.sleep(PROCESS_SLEEP)
        except Exception as e:
            print(e)
            error_log("Scheduler-line116")
            error_log(e)
   
    def push_top(self):
        '''
        推送高ctr文章
        '''
        while True:
            print("启动高ctr推送...")
            info_log("启动高ctr推送...")
            try:
                info_log("online_output...")
                refresh_data = self.OnlineOutput.put_top_articles()
                info_log("online_output OK!")
            except Exception as e:
                print(e)
                error_log("Scheduler-line112")
                error_log(e)
            print("等待2h...")
            info_log("等待2h...")
            time.sleep(TOP_PUSH_SLEEP)

    def kill_data_conn(self):
        self.SimpleData.kill_conn()
    
    def kill_output_conn(self):
        self.OnlineOutput.kill_conn()