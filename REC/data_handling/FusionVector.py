# date:2019/4/21
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import re
import pickle
from datetime import *

from sklearn.datasets.base import Bunch
import pandas as pd
import numpy as np

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


def judge_channel(x, channel_pos, channel_neg):
    if str(x) in channel_pos:
        return 2
    elif str(x) in channel_neg:
        return 0
    else:
        return 1


def judge_source(x, source_pos, source_neg):
    if str(x) in source_pos:
        return 2
    elif str(x) in source_neg:
        return 0
    else:
        return 1


def judge_source_channel(a, b, source_channel_pos, source_channel_neg):
    x = (a, str(b))
    if x in source_channel_pos:
        return 2
    elif x in source_channel_neg:
        return 0
    else:
        return 1


class FusionVector:
    def __init__(self, sp_vec="", ar_vec="", cm_vec=""):
        self.sp_vec_path = sp_vec
        self.ar_vec_path = ar_vec
        self.cm_vec_path = cm_vec # 合并的向量

    '''
    协调计算训练数据的ArticleVector和AdditionalVector,并存储到文件中.
    '''

    def train_vec(self, source, source_detail, article_ctr):
        vec_info_log("training vector...")
        path = os.getcwd()
        self.sp_vec_path = path + '/REC/static/SpecialVec.dat'
        SpecialVec = getAdditionalVec(source, source_detail)
        _writebunchobj(self.sp_vec_path, SpecialVec)
        vec_info_log("生成的channel判别-positive：")
        vec_info_log(SpecialVec.channel_pos)
        vec_info_log("生成的channel判别-negative：")
        vec_info_log(SpecialVec.channel_neg)
        vec_info_log("vector training completed!")

    @isVecTrained()
    def special_vec_generate(self, data):
        info_log("special_vec_generating...")

        sp_bunch = _read_bunch(self.sp_vec_path)
        source_pos = sp_bunch.source_pos
        source_neg = sp_bunch.source_neg
        channel_pos = sp_bunch.channel_pos
        channel_neg = sp_bunch.channel_neg
        source_channel_pos = sp_bunch.source_channel_pos
        source_channel_neg = sp_bunch.source_channel_neg

        vec_df = pd.DataFrame(columns=['item_id','channel', 'source', 'channelSource'])
        vec_df['item_id'] = data['id']
        vec_df['channel'] = data['channel'].apply(
            judge_channel, args=(channel_pos, channel_neg,))
        vec_df['source'] = data['source'].apply(
            judge_source, args=(source_pos, source_neg,))
        vec_df['channelSource'] = data[['channel', 'source']].apply(lambda row: judge_source_channel(
            row['source'], row['channel'], source_channel_pos, source_channel_neg), axis=1)
        
        # 计算统计量
        channel_source_desc = vec_df[['channelSource','item_id']].groupby(['channelSource'],as_index=False).count()
        channel_desc = vec_df[['channel','item_id']].groupby(['channel'],as_index=False).count()
        source_desc = vec_df[['source','item_id']].groupby(['source'],as_index=False).count()

        writer = pd.ExcelWriter(os.getcwd()+"/REC/static/files/"+str(date.today())+"-vector_distribution.xlsx")
        channel_source_desc.to_excel(writer,sheet_name='channelSource')
        channel_desc.to_excel(writer,sheet_name='channel')
        source_desc.to_excel(writer,sheet_name='source')
        writer.save()

        info_log(channel_source_desc)
        info_log(channel_desc)
        info_log(source_desc)   
        info_log("special_vec_generated!")
        
        return vec_df

    def article_vec_generate(self,data):
        return pd.DataFrame(columns=['item_id'])

    def pack_vec(self,data):
        '''
        打包为bunch向量输出.
        @return bunch_path
        '''
        sp_vec = self.special_vec_generate(data)
        ar_vec = self.article_vec_generate(data)
        pack_bunch = Bunch(sp_vec=sp_vec,ar_vec=ar_vec)
        return pack_bunch

    def compose_vec(self):
        '''
        合并为一个向量输出.
        @return composed_vec
        '''
        pass
