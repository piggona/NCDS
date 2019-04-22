# date:2019/4/21
# -*- coding: utf-8 -*-
# auth：Haohao

import jieba
import json
import os

def stopwordslist():
    path = os.getcwd()
    stopwords = [line.strip() for line in open(
        path + '/REC/static/chinesestop.txt', encoding='UTF-8').readlines()]
    return stopwords

'''
Seperates dataframe lines with jieba.
@Param obj object unseperated
'''
def seperate(obj):
    pass