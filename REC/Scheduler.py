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
from REC.data_market.SimpleData import fetch_data
from REC.data_handling.AdditionalVector import handle_source, handle_bias_format, handle_channel_source_bias, handle_channel_bias
from REC.utils.frame import *
from REC.logs.logger import *
from REC.data_output.OnlineOutput import *

def _writebunchobj(path, bunchobj):
    with open(path, "wb") as f:
        pickle.dump(bunchobj, f)

class Scheduler:
    def __init__(self):
        frame("Welcome to REC")
        self.SimpleData = fetch_data(CONNECTION_MODE)
        self.OnlineOutput = OnlineOutput(CONNECTION_MODE)
        self.SpecialVec = Bunch(source_pos=np.array([1, 2, 3]), source_neg=np.array(
            [1, 2, 3]), source_channel_pos=[], source_channel_neg=[], channel_pos=np.array([1, 2, 3]), channel_neg=np.array([1, 2, 3]))
        self.page = ""
    
    def get_special_vec(self):
        path = os.getcwd() + "/REC/static/specialVec.dat"
        while True:
            try:
                info_log("get_special_vec...")
                self.get_page()
                self.get_source_vec()
                self.get_source_channel_vec()
                self.get_channel_vec()
                _writebunchobj(path,self.SpecialVec)
                self.kill_conn()
                info_log("OK!")
            except Exception as e:
                print(e)
                error_log(e)
            time.sleep(86400)
    
    def online_output(self):
        while True:
            try:
                info_log("online_output...")
                self.OnlineOutput.put_work()
                info_log("OK!")
            except Exception as e:
                print(e)
                error_log(e)
            time.sleep(1800)

    def get_source_vec(self):
        info_log("Source Starts...")
        source = self.SimpleData.fetch_source_data()
        vec_result = handle_source(source)
        self.SpecialVec.source_pos = vec_result["positive"]
        self.SpecialVec.source_neg = vec_result["negative"]
        info_log("OK!")

    def get_page(self):
        info_log("Source_Channel Starts...")
        info_log("Format Data...")
        source = self.SimpleData.fetch_bias_data()
        pre = handle_bias_format(source)
        self.page = pre
        info_log("OK!")
    
    def kill_conn(self):
        self.SimpleData.kill_conn()

    # @is_got_page
    def get_source_channel_vec(self):
        info_log("Gets Source_Channel Vec...")
        vec_result = handle_channel_source_bias(self.page)
        self.SpecialVec.source_channel_pos = vec_result["positive"]
        self.SpecialVec.source_channel_neg = vec_result["negative"]
        info_log("OK!")
    
    def get_channel_vec(self):
        info_log("Gets Channel Vec...")
        vec_result = handle_channel_bias(self.page)
        self.SpecialVec.channel_pos = vec_result["positive"]
        self.SpecialVec.channel_neg = vec_result["negative"]
        info_log("OK!")
