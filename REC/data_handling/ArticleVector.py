# date:2019/4/28
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import re
import pickle
import json
from datetime import *

import jieba

from sklearn.datasets.base import Bunch
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
import pandas as pd
import numpy as np

from REC.logs.logger import *



def getTrainArticleVec(source_detail):
    source_detail["url"] = source_detail["url"].apply(_url_to_source)
    source_detail["category"] = source_detail["category"].apply(_catenum_to_cate)
    source_data = source_detail[['item_id','ctr','expose_num','title']].fillna(0)
    source_data['ctr'] = source_data[['ctr','expose_num']].apply(lambda row: _divide(row['ctr'],row['expose_num']),axis=1)
    vec_info_log("训练集ctr分类分布：")
    vec_info_log(source_data.groupby(['ctr']).count())
    source_data['title'] = source_data['title'].apply(_seperate_data)
    num_vec = _train_vectorizer(source_data)
    tf_idf_vec = tf_idf_vectorizer(num_vec)
    train_vec = Bunch(tf_idf=tf_idf_vec,y_train=source_data['ctr'].values)
    return train_vec

def getArticleVec(source_new):
    source_data = source_new[['id','title']].fillna(0)
    source_data['title'] = source_data['title'].apply(_seperate_data)
    num_vec = _vectorizer(source_data)
    tf_idf_vec = tf_idf_vectorizer(num_vec)
    return tf_idf_vec

def tf_idf_vectorizer(vec_count_title):
    tf_idf_transformer = TfidfTransformer()
    tf_idf_vec = tf_idf_transformer.fit_transform(vec_count_title)
    return tf_idf_vec

def _read_bunch(path):
    with open(path, "rb") as f:
        content = pickle.load(f)
    f.close()
    return content


def _writebunchobj(path, bunchobj):
    with open(path, "wb") as f:
        pickle.dump(bunchobj, f)

def _train_vectorizer(list_data):
    vectorizer = CountVectorizer(stop_words=[" "],max_df=0.99, min_df=0.001)
    vec_count_title = vectorizer.fit_transform(list_data['title'])
    vec_feature = vectorizer.get_feature_names()
    bunch = Bunch(vec_feature=vec_feature)
    _writebunchobj(os.getcwd()+'/REC/static/vec_feature.dat',bunch)
    return vec_count_title

def _vectorizer(list_data):
    bunch = _read_bunch(os.getcwd()+'/REC/static/vec_feature.dat')
    vec_feature = bunch.vec_feature
    vectorizer = CountVectorizer(stop_words=[" "],max_df=0.99, min_df=0.001,vocabulary=vec_feature)
    vec_count_title = vectorizer.fit_transform(list_data['title'])
    return vec_count_title

def _stopwordslist():
    stopwords = [line.strip() for line in open(
        os.getcwd() + '/REC/static/chinesestop.txt', encoding='UTF-8').readlines()]
    return stopwords

def _seperate_data(line):
    wordsep = jieba.cut(str(line),cut_all=False)
    stopwords = _stopwordslist()
    outstr = ''
    for word in wordsep:
        if word not in stopwords:
            if word != '\t':
                outstr += word
                outstr += ' '
    return outstr

def _divide(x,y):
    if x >= 0.15 and y > 200:
        return 2
    elif x <= 0.06:
        return 0
    else:
        return 1

def _url_to_source(x):
    pattern = re.compile('.*?\.(.*?)\.')
    match = pattern.match(x)
    if match:
        if match[1] == "yidianzixun":
            return "一点资讯"
        elif match[1] == "eastday":
            return "东方头条"
        elif match[1] == "18sjkj":
            return "数加源"
        elif match[1] == "shouhj":
            return "唔哩头条"
        else:
            return "未知"
    else:
        return 0


def _catenum_to_cate(x):
    if x == '1000':
        return "推荐"
    elif x == '1001':
        return "娱乐"
    elif x == '1002':
        return "健康"
    elif x == '1003':
        return "体育"
    elif x == '1004':
        return "汽车"
    elif x == '1005':
        return "国际"
    elif x == '1006':
        return "科技"
    elif x == '1007':
        return "财经"
    elif x == '1008':
        return "热点"
    elif x == '1009':
        return "社会"
    elif x == '1010':
        return "文化"
    elif x == '1011':
        return "运势"
    elif x == '1012':
        return "美食"
    elif x == '1013':
        return "科学"
    elif x == '1014':
        return "旅游"
    elif x == '1015':
        return "房产"
    elif x == '1016':
        return "动漫"
    elif x == '1017':
        return "教育"
    elif x == '1018':
        return "时尚"
    elif x == '1019':
        return "情感"
    elif x == '1020':
        return "育儿"
    elif x == '1021':
        return "游戏"
    elif x == '1022':
        return "职场"
    elif x == '1023':
        return "家居"
    elif x == '3001':
        return "搞笑"
    elif x == '3002':
        return "影视"
    elif x == '3003':
        return "娱乐视频"
    elif x == '3004':
        return "美食视频"
    elif x == '3005':
        return "综艺"
    elif x == '3006':
        return "军事"
    elif x == '3007':
        return "社会视频"
    elif x == '3008':
        return "萌宠"
    elif x == '3009':
        return "游戏视频"
    elif x == '3010':
        return "音乐"
    elif x == '3011':
        return "生活"
    elif x == '3012':
        return "猎奇"
    elif x == '3013':
        return "科技视频"
    elif x == '3014':
        return "体育视频"
    else:
        return "其它"
