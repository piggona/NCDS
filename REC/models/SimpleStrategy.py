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

from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import GridSearchCV

from REC.data_handling.FusionVector import FusionVector
from REC.utils.frame import *
from REC.logs.logger import *
from REC.data_handling.AdditionalVector import *
from REC.aspects.IsVecTrained import *

def _read_bunch(path):
    with open(path, "rb") as f:
        content = pickle.load(f)
    f.close()
    return content

result_pos = []
result_neg = []
def get_pos(a,b,c,d):
    point = a
    if point == 2:
        result_pos.append(str(d))

def get_neg(a,b,c,d):
    point = a
    if point == 0:
        result_neg.append(str(d))

class SimpleStrategy:
    def __init__(self,model_path=""):
        if model_path == "":
            self.fusionVec = FusionVector(sp_vec=os.getcwd() + "/REC/static/SpecialVec.dat", ar_vec="", cm_vec="")
        else:
            self.fusionVec = FusionVector(sp_vec=model_path['sp_vec'], ar_vec=model_path['ar_vec'], cm_vec=model_path['cm_vec'])
    
    def train(self,source, source_detail, article_ctr=""):
        self.fusionVec.train_vec(source,source_detail,article_ctr)
        bunch = _read_bunch(os.getcwd() + "/REC/static/SpecialVec.dat")
        article_vec = bunch.ArticleVec
        tf_idf_vec = article_vec.tf_idf
        y_train = article_vec.y_train
        self.mlp = self.mlp_classifier(tf_idf_vec,y_train)

    def mlp_classifier(tf_idf_vec,y_train):
        model = MLPClassifier(solver='adam',random_state=1)   
        param_grid = {'alpha': [1e-3, 1e-2, 1e-1, 1e-4, 1e-5]}    
        grid_search = GridSearchCV(model, param_grid, n_jobs = 8, verbose=1)    
        grid_search.fit(tf_idf_vec,y_train)    
        best_parameters = grid_search.best_estimator_.get_params()
        print(best_parameters)
        mlp = MLPClassifier(solver='adam', alpha=best_parameters['alpha'],hidden_layer_sizes=(5, 5), random_state=1)
        mlp.fit(tf_idf_vec,y_train)
        return mlp
    
    def mlp_judge(self,data):
        tfidf_vec = self.fusionVec.article_vec_generate(data)
        re_mlp = self.mlp.predict(tfidf_vec)
        data['predict'] = re_mlp
        result_pos = data.query('predict == 2')['id'].tolist()
        result_neg = data.query('predict == 0')['id'].tolist()
        result = {"positive":result_pos,"negative":result_neg}
        return result
    
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

