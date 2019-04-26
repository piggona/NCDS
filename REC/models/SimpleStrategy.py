# date:2019/4/21
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import re
import pickle

from sklearn.datasets.base import Bunch
import pandas as pd
import numpy as np
from datetime import *

from REC.data_handling.FusionVector import FusionVector
from REC.utils.frame import *
from REC.logs.logger import *
from REC.data_handling.AdditionalVector import *
from REC.aspects.IsVecTrained import *

result_pos = []
result_neg = []
def get_pos(a,b,c,d):
    point = a
    if point == 2:
        result_pos.append(d)

def get_neg(a,b,c,d):
    point = a
    if point == 0:
        result_neg.append(d)

class SimpleStrategy:
    def __init__(self,model_path=""):
        if model_path == "":
            self.fusionVec = FusionVector(sp_vec="/Users/haohao/Documents/NCDS/REC/static/specialVec.dat", ar_vec="", cm_vec="")
        else:
            self.fusionVec = FusionVector(sp_vec=model_path['sp_vec'], ar_vec=model_path['ar_vec'], cm_vec=model_path['cm_vec'])
    
    def train(self,source, source_detail, article_ctr=""):
        self.fusionVec.train_vec(source,source_detail,article_ctr)
    
    def judge(self,data):
        result_pos.clear()
        result_neg.clear()
        vec_bunch = self.fusionVec.pack_vec(data)
        sp_vec = vec_bunch.sp_vec
        sp_vec.apply(lambda row: get_pos(row['channel'],row['source'],row['channelSource'],row['item_id']),axis=1)
        sp_vec.apply(lambda row: get_neg(row['channel'],row['source'],row['channelSource'],row['item_id']),axis=1)
        result_j = {}

        info_log('vec dumping...')
        try:
            with open(os.getcwd()+'/REC/static/files/result_vec.json','r') as r:
                result_j = json.load(r)
        except Exception as e:
            print(e)
        result_j[str(int(time.time()))] = {"result_pos":result_pos,"result_neg":result_neg}
        with open(os.getcwd()+'/REC/static/files/result_vec.json','w') as w:
                json.dump(result_j,w)
        print('vec dump success!')
        info_log('vec dump success!')

        info_log(result_pos)
        info_log(result_neg)
        result = {"positive":result_pos,"negative":result_neg}
        return result

