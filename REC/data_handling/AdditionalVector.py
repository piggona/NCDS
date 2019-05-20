# date:2019/4/21
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import json
import re
import pickle

from sklearn.datasets.base import Bunch
import pandas as pd
import numpy as np
from datetime import *

from REC.utils.frame import *
from REC.logs.logger import *

def getAdditionalVec(source, source_detail):
    '''
    得到判别文章重点向量的判别bunch.
    @param source DataFrame from data_market relevant to handle_source
    @param source DataFrame from data_market relevant to page
    @return vec_bunch pickle.bunch Bunch contains 
    '''
    vec_info_log("getAdditionalVec...")
    SpecialVec = Bunch(source_pos=np.array([1, 2, 3]), source_neg=np.array(
        [1, 2, 3]), source_channel_pos=[], source_channel_neg=[], channel_pos=np.array([1, 2, 3]), channel_neg=np.array([1, 2, 3]))
    result_source = handle_source(source)
    page = handle_bias_format(source_detail)
    vec_info_log("总体ctr:")
    vec_info_log(calculate_ctr(page))
    channel_result = handle_channel_bias(page)
    channel_source_result = handle_channel_source_bias(page)
    SpecialVec.source_pos = result_source['positive']
    SpecialVec.source_neg = result_source['negative']
    SpecialVec.source_channel_pos = channel_source_result['positive']
    SpecialVec.source_channel_neg = channel_source_result['negative']
    SpecialVec.channel_pos = channel_result['positive']
    SpecialVec.channel_neg = channel_result['negative']
    vec_info_log("positive channel: {}".format(channel_result['positive']))
    vec_info_log("negative channel: {}".format(channel_result['negative']))
    vec_info_log("gotAdditionalVec!")
    return SpecialVec

def handle_source(source, sep_point=20):
    '''
    判别与内容相关的信息，得到向量判别.
    @param source dataFrame from data_market
    @param sep_point seperate rank Series
    '''
    try:
        writer = pd.ExcelWriter(os.getcwd()+"/REC/static/files/"+str(date.today())+"-source.xlsx")
        source = source.query('expose_num > 100')
        source_head = source.query('source_ctr > 0.05')
        # print("ctr统计量：")
        # print("=========================")
        # print(source["source_ctr"].describe())
        # print("expose统计量：")
        # print("=========================")
        # print(source["expose_num"].describe())
        # print("click统计量：")
        # print("=========================")

        vec_info_log("ctr统计量：")
        vec_info_log("=========================")
        vec_info_log(source["source_ctr"].describe())
        vec_info_log("expose统计量：")
        vec_info_log("=========================")
        vec_info_log(source["expose_num"].describe())
        vec_info_log("click统计量：")
        vec_info_log("=========================")
        source.to_excel(writer,'Sheet1')
        writer.save()

        row_num = source_head.size / 5
        sep = int(row_num * (sep_point/100))
        positive = source_head.head(sep)['source'].values
        negative = source.query('source_ctr < 0.05')['source'].values
        result = {"positive": positive, "negative": negative}
        return result
    except Exception as e:
        # print(e)
        error_log("AdditionalVector-line75")
        error_log(e)

def handle_bias_format(source_detail):
    '''
    预处理DataFrame，得到page.
    @param source_detail Raw data from data_market.
    @return page Preprocessed data.
    '''
    try:
        writer = pd.ExcelWriter(os.getcwd()+"/REC/static/files/"+str(date.today())+"-page.xlsx")
        vec_info_log("是否存在空值")
        # print(source_detail.isnull().any())
        vec_info_log(source_detail.isnull().any())

        vec_info_log("整理标题")
        source_detail["url"] = source_detail["url"].apply(__url_to_source)
        # source_detail["category"] = source_detail["category"].apply(__catenum_to_cate)
        page = source_detail.rename(columns={"pt": "日期", "item_id": "文章ID", "expose_num": "曝光数", "click_num": "点击数",
                                             "ctr": "CTR", "title": "文章标题", "tags": "标签", "source": "作者", "url": "URL", "dat": "文章创建日期", "category": "频道"})
        page = page.fillna(0)
        page.to_excel(writer,'Sheet1')
        writer.save()
        return page
    except Exception as e:
        # print(e)
        error_log("AdditionalVector-line98")
        error_log(e)

def handle_channel_bias(page):
    '''
    判别与频道相关的信息，得到正样本频道及负样本频道.
    @param page Preprocessed data from article_ctr_all.
    @return channel_result_vec
    '''
    try:
        writer = pd.ExcelWriter(os.getcwd()+"/REC/static/files/"+str(date.today())+"-channel.xlsx")
        vec_info_log("整合数据")
        channel_bias_sum = page[['曝光数', '点击数', '频道']
                                ].groupby(['频道'], as_index=False).sum()
        channel_bias_count = page[['频道', 'CTR']].groupby(
            ['频道'], as_index=False).count()
        channel_bias_mean = page[['频道', 'CTR']].groupby(
            ['频道'], as_index=False).mean()
        channel_bias_mean['real_ctr'] = channel_bias_sum['点击数'] / \
            channel_bias_sum['曝光数']
        channel_bias_all = channel_bias_sum.merge(channel_bias_count, on='频道')
        channel_bias_all = channel_bias_sum.merge(channel_bias_mean, on='频道')
        channel_bias_all = channel_bias_all[~(channel_bias_all["频道"].str.contains("9999"))]
        dec_all = channel_bias_all['real_ctr'].describe()
        upper_quantile = dec_all.values[4]
        lower_quantile = dec_all.values[6]
        std = dec_all.values[1]

        vec_info_log("分组-得到结果")
        group1 = channel_bias_all.query('曝光数 < 4500').sort_values(
            by='real_ctr', ascending=False)
        group2 = channel_bias_all.query('曝光数 >= 4500 & 曝光数 < 20000').sort_values(
            by='real_ctr', ascending=False)
        group3 = channel_bias_all.query('曝光数 > 20000').sort_values(
            by='real_ctr', ascending=False)
        
        channel_bias_all.to_excel(writer,sheet_name='channel')
        group1.to_excel(writer,sheet_name='group1')
        group2.to_excel(writer,sheet_name='group2')
        group3.to_excel(writer,sheet_name='group3')
        dec_all.to_excel(writer,sheet_name='desc')
        group1['real_ctr'].describe().to_excel(writer,sheet_name='group1_desc')
        group2['real_ctr'].describe().to_excel(writer,sheet_name='group2_desc')
        group3['real_ctr'].describe().to_excel(writer,sheet_name='group3_desc')
        writer.save()


        vec_info_log("得到判别向量")
        # group1: 75%(下四分位数) > 总75% 则取75%以上的类为positive
        # group1: 25% < 总25% 则取总25%以下的类为negative
        # group1: 75%(下四分位数) < 总75% 则取总75%以上的类为positive
        # group1: 25% > 总25% 则取25%以下的类为negative
        g1_upper_quantile = group1['real_ctr'].describe().values[4]
        g1_lower_quantile = group1['real_ctr'].describe().values[6]
        if g1_lower_quantile > lower_quantile:
            g1_pos = group1.query(
                'real_ctr > ' + str(g1_lower_quantile.item()))['频道'].values
        else:
            g1_pos = group1.query(
                'real_ctr > ' + str(lower_quantile.item()))['频道'].values
        if g1_upper_quantile > upper_quantile:
            g1_neg = group1.query(
                'real_ctr < ' + str(g1_upper_quantile.item()))['频道'].values
        else:
            g1_neg = group1.query(
                'real_ctr < ' + str(upper_quantile.item()))['频道'].values

        # group2: std < 0.01 & 75%(下四分位数) > 总75% 则取总75%以上的类为positive
        # group2: std < 0.01 & 25% < 总25% 则取总25%以下的类为negative
        # group2: std < 0.01 & 75%(下四分位数) < 总75% 则取75%以上的类为positive
        # group2: std < 0.01 & 25% > 总25% 则取25%以下的类为negative
        # group2: std > 0.01 & 75%(下四分位数) > 总75% 则取75%以上的类为positive
        # group2: std > 0.01 & 25% < 总25% 则取25%以下的类为negative
        # group2: std > 0.01 & 75%(下四分位数) < 总75% 则取总75%以上的类为positive
        # group2: std > 0.01 & 25% > 总25% 则取总25%以下的类为negative
        g2_upper_quantile = group2['real_ctr'].describe().values[4]
        g2_lower_quantile = group2['real_ctr'].describe().values[6]
        g2_std = group2['real_ctr'].describe().values[1]
        if g2_std < 0.01:
            if g2_lower_quantile > lower_quantile:
                g2_pos = group2.query(
                    'real_ctr > ' + str(lower_quantile.item()))['频道'].values
            else:
                g2_pos = group2.query(
                    'real_ctr > ' + str(g2_lower_quantile.item()))['频道'].values
            if g2_upper_quantile > upper_quantile:
                g2_neg = group2.query(
                    'real_ctr < ' + str(g2_upper_quantile.item()))['频道'].values
            else:
                g2_neg = group2.query(
                    'real_ctr < ' + str(upper_quantile.item()))['频道'].values
        else:
            if g2_lower_quantile > lower_quantile:
                g2_pos = group2.query(
                    'real_ctr > ' + str(g2_lower_quantile.item()))['频道'].values
            else:
                g2_pos = group2.query(
                    'real_ctr > ' + str(lower_quantile.item()))['频道'].values
            if g2_upper_quantile > upper_quantile:
                g2_neg = group2.query(
                    'real_ctr < ' + str(g2_upper_quantile.item()))['频道'].values
            else:
                g2_neg = group2.query(
                    'real_ctr < ' + str(upper_quantile.item()))['频道'].values

        # group3: 75%(下四分位数) > 总75% 则取总75%以上的类为positive
        # group3: 25% < 总25% 则取25%以下的类为negative
        # group3: 75%(下四分位数) < 总75% 则取75%以上的类为positive
        # group3: 25% > 总25% 则取总25%以下的类为negative
        g3_upper_quantile = group3['real_ctr'].describe().values[4]
        g3_lower_quantile = group3['real_ctr'].describe().values[6]
        g3_std = group3['real_ctr'].describe().values[1]
        if g3_lower_quantile > lower_quantile:
            g3_pos = group3.query(
                'real_ctr > ' + str(lower_quantile.item()))['频道'].values
        else:
            g3_pos = group3.query(
                'real_ctr > ' + str(g3_lower_quantile.item()))['频道'].values
        if g3_upper_quantile > upper_quantile:
            g3_neg = group3.query(
                'real_ctr < ' + str(upper_quantile.item()))['频道'].values
        else:
            g3_neg = group3.query(
                'real_ctr < ' + str(g3_upper_quantile.item()))['频道'].values
        channel_pos = np.append(g1_pos, g2_pos)
        channel_pos = np.append(channel_pos, g3_pos)
        channel_neg = np.append(g1_neg, g2_neg)
        channel_neg = np.append(channel_neg, g3_neg)
        result = {"positive": channel_pos, "negative": channel_neg}
        return result

    except Exception as e:
        # print(e)
        error_log("AdditionalVector-line218")
        error_log(e)

def calculate_ctr(page):
    '''
    计算总共的ctr（4days）
    '''
    ctr = page['点击数'].sum()/page['曝光数'].sum()
    vec_info_log(str(ctr))
    vec_info_log("ctr：" + str(ctr))
    days_ctr = {}
    info_log('ctr dumping...')
    try:
        with open(os.getcwd()+'/REC/static/files/days_ctr.json','r') as f:
            days_ctr = json.load(f)
    except Exception as e:
        print(e)
    days_ctr[str(date.today())] = ctr
    with open(os.getcwd()+'/REC/static/files/days_ctr.json','w') as w:
        json.dump(days_ctr,w)
    info_log('ctr dumped!')
    return ctr


channel_source_positive = []
channel_source_negative = []

def handle_channel_source_bias(page):
    '''
    判别与频道-作者相关的信息，得到向量判别.(set)
    @param page Preprocessed data from article_ctr_all.
    @return channel_source_vec
    '''
    try:
        writer = pd.ExcelWriter(os.getcwd()+"/REC/static/files/"+str(date.today())+"-channel-source.xlsx")
        channel_writer_bias_sum = page[['作者', '曝光数', '点击数', '频道']].groupby(
            ['作者', '频道'], as_index=False).sum()
        channel_writer_bias_count = page[['作者', '频道', 'CTR']].groupby(
            ['作者', '频道'], as_index=False).count()
        channel_writer_bias_mean = page[['作者', '频道', 'CTR']].groupby(
            ['作者', '频道'], as_index=False).mean()
        channel_writer_bias_mean['real_ctr'] = channel_writer_bias_sum['点击数'] / \
            channel_writer_bias_sum['曝光数']
        channel_writer_bias_all = channel_writer_bias_sum.merge(
            channel_writer_bias_count, on=['作者', '频道'])
        channel_writer_bias_all = channel_writer_bias_sum.merge(
            channel_writer_bias_mean, on=['频道', '作者'])
        channel_writer_bias_all = channel_writer_bias_all.fillna(0)
        channel_writer_bias_all.to_excel(writer,'Sheet1')
        writer.save()
        channel_distinct = channel_writer_bias_all[['频道', '曝光数']].groupby(
            ['频道'], as_index=False).sum().sort_values(by='曝光数', ascending=False)
        channel_distinct['频道'].apply(
            every_channel_top, args=(channel_writer_bias_all,))
        result = {"positive": channel_source_positive,
                  "negative": channel_source_negative}
        return result
    except Exception as e:
        # print(e)
        error_log("AdditionalVector-line263")
        error_log(e)


def every_channel_top(x, channel_writer_bias_all):
    q = '频道 == "' + x + '"'
    # print(q)
    channel_source = channel_writer_bias_all.query(
        q).sort_values(by='real_ctr', ascending=False)
    channel_source[['作者', '频道', 'real_ctr']].head(30).apply(
        lambda row: put_in_array(row['作者'], row['频道'], channel_source_positive), axis=1)
    channel_source[['作者', '频道', 'real_ctr']].query('real_ctr == 0').apply(
        lambda row: put_in_array(row['作者'], row['频道'], channel_source_negative), axis=1)


def put_in_array(a, b, channel_source):
    tup = (a, b)
    channel_source.append(tup)


def __url_to_source(x):
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


def __catenum_to_cate(x):
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
