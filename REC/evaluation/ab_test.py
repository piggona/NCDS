# date:2019/5/06
# -*- coding: utf-8 -*-
# auth：Haohao
# Simple

import os
import time
import re
import pickle

import pandas as pd

from REC.logs.logger import *

def ab_test(test_data):
    # data handling
    test_data_raw = test_data.copy()
    test_data = test_data[['id','predict']]
    test_data_pos = test_data.query('predict == 2')
    test_data_norm = test_data.query('predict == 1')
    test_data_neg = test_data.query('predict == 0')

    # get sample count
    data_distribution = test_data.groupby(['predict'],as_index=False).count()
    info_log("data distribution:")
    info_log(data_distribution)
    pos_dis = data_distribution['id'].values[2]
    neg_dis = data_distribution['id'].values[0]
    norm_dis = data_distribution['id'].values[1]
    total = pos_dis + neg_dis + norm_dis
    pos_partition = pos_dis * (pos_dis / total)
    norm_partition = pos_dis * (norm_dis / total)
    neg_partition = pos_dis * (neg_dis / total)
    pos = int(round(pos_partition))
    norm = int(round(norm_partition))
    neg = int(round(neg_partition))
    info_log("pos count: {}".format(pos))
    info_log("norm count: {}".format(norm))
    info_log("neg count: {}".format(neg))

    # sample
    pos_result  = test_data_pos.sample(n=pos)
    norm_result  = test_data_norm.sample(n=norm)
    neg_result = test_data_neg.sample(n=neg)

    norm_list = norm_result['id'].values.tolist()
    pos_list = pos_result['id'].values.tolist()
    neg_list = neg_result['id'].values.tolist()
    result = []
    result.extend(pos_list)
    result.extend(norm_list)
    result.extend(neg_list)

    return result






