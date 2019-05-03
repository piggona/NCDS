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

def _writebunchobj(path, bunchobj):
    with open(path, "wb") as f:
        pickle.dump(bunchobj, f)

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
        self.mlp = ""
    
    def train(self,source, source_detail, article_ctr=""):
        '''
        使用得到的数据训练模型
        '''
        vec_info_log("Is getting training vectors...")
        # 生成并获取表示向量
        self.fusionVec.train_vec(source,source_detail,article_ctr)
        bunch = _read_bunch(os.getcwd() + "/REC/static/SpecialVec.dat")
        article_vec = bunch.ArticleVec
        tf_idf_vec = article_vec.tf_idf
        y_train = article_vec.y_train

        # 临时调试输出>>
        print("tf-idf向量：{}".format(tf_idf_vec))
        print("训练向量集：{}".format(y_train))

        vec_info_log("Training vec got!")
        # 训练模型，将结果存储在对象中
        vec_info_log("Is training models...")
        self.mlp = self.mlp_classifier(tf_idf_vec,y_train)
        bunch = Bunch(mlp_vec=self.mlp)
        _writebunchobj(os.getcwd()+'/REC/static/mlp.dat',bunch)
        print("Model trained!")
        vec_info_log("Model trained!")
    
    def mlp_judge(self,data):
        '''
        使用多层感知器模型对Output的新数据进行分类输出.
        '''
        info_log("data size:".format(str(data.size)))
        mlp_bunch = _read_bunch(os.getcwd()+'/REC/static/mlp.dat')
        mlp = mlp_bunch.mlp_vec

        # 临时调试输出>>
        info_log("mlp判别器：")
        info_log(mlp)

        # 处理原始数据使其向量化
        tfidf_vec = self.fusionVec.article_vec_generate(data)

        # 临时调试输出>>
        info_log("被作用向量化结果：{}".format(tfidf_vec))

        # 使用模型判别
        re_mlp = mlp.predict(tfidf_vec)

        # 临时调试输出>>
        info_log("mlp判别结果")
        info_log(re_mlp)

        # 得到result list
        data['predict'] = re_mlp

        writer = pd.ExcelWriter(os.getcwd()+"/REC/static/files/预测结果.xlsx")
        data.to_excel(writer,'Sheet1')
        writer.save()

        result_pos = data.query('predict == 2')['id'].tolist()
        result_neg = data.query('predict == 0')['id'].tolist()
        result = {"positive":result_pos,"negative":result_neg}
        # return list结果
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
    
    # mlp多层感知模型分类器
    def mlp_classifier(self,tf_idf_vec,y_train):
        model = MLPClassifier(solver='adam',random_state=1)   
        param_grid = {'alpha': [1e-3, 1e-2, 1e-1, 1e-4, 1e-5]}    
        grid_search = GridSearchCV(model, param_grid, n_jobs = 1, verbose=1)    
        grid_search.fit(tf_idf_vec,y_train)    
        best_parameters = grid_search.best_estimator_.get_params()
        print(best_parameters)
        mlp = MLPClassifier(solver='adam', alpha=best_parameters['alpha'],hidden_layer_sizes=(5, 5), random_state=1)
        mlp.fit(tf_idf_vec,y_train)
        return mlp

