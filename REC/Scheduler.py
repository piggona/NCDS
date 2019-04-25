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
                source = self.SimpleData.fetch_source_data()
                source_detail = self.SimpleData.fetch_bias_data()

                self.Strategy.train(source,source_detail)
                self.kill_conn()
                vec_info_log("get_train_vec OK!")
            except Exception as e:
                print(e)
                error_log("Scheduler-line68")
                vec_error_log(e)
            print("等待1day...")
            vec_info_log("等待1day...")
            time.sleep(TRAIN_SLEEP)
    
    def process_simple(self):
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

    def kill_conn(self):
        self.SimpleData.kill_conn()