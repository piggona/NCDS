# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import json

import pymongo
import pymysql

def fetch_new_user(user_time_range):
    conn = pymysql.connect(host='127.0.0.1',port=3306,user="jinyuanhao",db="infomation",passwd="Sjk0213%$")
    cursor = conn.cursor()
    user_query = "SELECT user_id from aliyun_user_info Where register_time > "+str(int(time.time()) - user_time_range)
    print("user_query:{}".format(user_query))
    cursor.execute(user_query)
    users = cursor.fetchall()
    print("users:{}".format(users))
    user_list = []
    for user in users:
        user_list.append(user[0])
    conn.commit()
    cursor.close()
    conn.close()
    return user_list

def get_start_ctr(user_time_range):
    '''
    得到每个user_list用户的ctr信息,存储到mongodb中
    计算用户首刷ctr
    计算用户首click的expose次数
    计算用户100次操作ctr
    '''
    client = pymongo.MongoClient(host="localhost",port=27017)
    db = client.ctrAnalytics
    collection = db["start_ctr"]
    user_ids = fetch_new_user(user_time_range)
    print("user_ids:{}".format(user_ids))
    conn = pymysql.connect(host='127.0.0.1',port=3306,user="jinyuanhao",db="infomation",passwd="Sjk0213%$")
    cursor = conn.cursor()
    for user_id in user_ids:
        print(user_id)
        user_ctr_item = {"user_id":user_id,"user_data":{"first_ctr":"","first_click":"","total_ctr":"","raw_data":""}}
        first_query = "SELECT item_id,bhv_type FROM aliyun_behavior_info WHERE user_id = {} ORDER BY bhv_time ASC LIMIT 100".format("'"+str(user_id)+"'")
        cursor.execute(first_query)
        user_items = cursor.fetchmany(100)
        user_ctr_item["user_data"]["raw_data"] = user_items
        expose_count = 0
        click_count = 0
        first_ctr = 0
        for user_item in user_items:
            if user_item[1] == "expose":
                expose_count += 1
                if (expose_count == 10) and (first_ctr == 0):
                    user_ctr_item["user_data"]["first_ctr"] = 0
                    first_ctr = 1
            if user_item[1] == "click":
                if expose_count == 0:
                    user_ctr_item["user_data"]["first_ctr"] = 0.333
                else:
                    if first_ctr == 0:
                        user_ctr_item["user_data"]["first_ctr"] = 1 / expose_count
                        first_ctr = 1
                    if click_count == 0:
                        user_ctr_item["user_data"]["first_click"] = expose_count
                    click_count += 1
        if expose_count == 0:
            user_ctr_item["user_data"]["total_ctr"] = 0
        else:
            user_ctr_item["user_data"]["total_ctr"] = click_count / expose_count
        collection.insert_one(user_ctr_item)
    conn.commit()
    cursor.close()
    conn.close()

def ctr_analysis():
    '''
    user_time_range内所有用户
    计算所有user_list ctr信息的统计量，包括均值，上分位数，标准差等
    要判断新用户（新用户未浏览即流失，新用户停留->停留时间）类型的分布
    对这个用户群体进行追踪分析（可选可触发）
    评估各个渠道新闻源的质量（使用ctr进行判断）
    '''
    path = os.getcwd()
    with open(path+"/config/ctr_config.json","r") as r:
        ctr_config = json.load(r)
    user_time_range = ctr_config["user_time_range"]
    get_start_ctr(user_time_range)

def article_ctr_analysis():
    '''
    article_time_range时间跨度内
    计算文章(item_id)不同时间段(create_time+range)内的article_ctr,click/expose
    计算文章被曝光次数expose:
    1. 文章(item_id)对应时间段的expose生命周期折线
    2. 在相同时间段节点，各个文章的article_ctr->expose图像
    '''
    pass

def data_flow_analysis():
    '''
    各个新闻源文章的ctr与其曝光比率的分布
    '''
    pass


if __name__ == "__main__":
    get_start_ctr(259200)

