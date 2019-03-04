# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import json
import csv
import sys
import codecs

import pymongo
import pymysql
import pandas as pd

def fetch_new_user(start_time,user_time_range):
    conn = pymysql.connect(host='127.0.0.1',port=3306,user="jinyuanhao",db="infomation",passwd="Sjk0213%$")
    cursor = conn.cursor()
    user_query = "SELECT user_id from aliyun_user_info Where register_time > "+str(start_time-user_time_range)
    # print("user_query:{}".format(user_query))
    cursor.execute(user_query)
    users = cursor.fetchall()
    # print("users:{}".format(users))
    user_list = []
    for user in users:
        user_list.append(user[0])
    conn.commit()
    cursor.close()
    conn.close()
    return user_list


def get_start_ctr(start_time,user_time_range):
    '''
    得到每个user_list用户的ctr信息,存储到mongodb中
    计算用户首刷ctr
    计算用户首click的expose次数
    计算用户100次操作ctr
    '''
    # 配置写入csv文件
    path = os.getcwd()
    with open(path+'/config/ctr_config.json','r') as r:
        ctr_config = json.load(r)
    csv_path = ctr_config["usr_csv_path"]
    ctr_analysis_interval = ctr_config["ctr_analysis_interval"]
    ctr_analysis_top = ctr_config["ctr_analysis_top"]

    data_scan_amount = ctr_config["data_scan_amount"]
    r.close()
    os.mkdir(csv_path+str(int(time.time())))
    csv_path = csv_path+str(int(time.time()))+"/"
    csv_doc_name = "usr_ctr_raw.csv"
    csvfile = open(csv_path+csv_doc_name,"w",newline='')
    writer = csv.writer(csvfile,quoting=csv.QUOTE_ALL)
    # 需要将自定义的头加上
    csv_head = ["user_id","exposes_before_first_click","total_ctr"]
    check_list = range(ctr_analysis_interval,ctr_analysis_top+1,ctr_analysis_interval)
    for check in check_list:
        csv_head.append("first"+str(check)+"expose_count")
    writer.writerow(csv_head)

    #配置写入mongoDB
    client = pymongo.MongoClient(host="localhost",port=27017)
    db = client.ctrAnalytics
    collection = db["start_ctr"]
    collection.drop()

    user_ids = fetch_new_user(start_time,user_time_range)
    # print("user_ids:{}".format(user_ids))
    conn = pymysql.connect(host='127.0.0.1',port=3306,user="jinyuanhao",db="infomation",passwd="Sjk0213%$")
    cursor = conn.cursor()
    
    for user_id in user_ids:
        # print(user_id)
        user_ctr_item = {"user_id":user_id,"user_data":{"exposes_before_first_click":"","total_ctr":"","raw_data":""}}
        first_query = "SELECT item_id,bhv_type FROM aliyun_behavior_info WHERE user_id = {0} ORDER BY bhv_time ASC LIMIT {1}".format("'"+str(user_id)+"'",str(data_scan_amount))
        cursor.execute(first_query)
        user_items = cursor.fetchmany(data_scan_amount)
        user_ctr_item["user_data"]["raw_data"] = user_items
        expose_count = 0
        click_count = 0
        first_ctr = 0
        for check in check_list:
            user_ctr_item["user_data"]["first"+str(check)+"expose_count"] = 0
        for user_item in user_items:
            if user_item[1] == "expose":
                expose_count += 1
                if expose_count in check_list:
                    user_ctr_item["user_data"]["first"+str(expose_count)+"expose_count"] = click_count / expose_count
            if user_item[1] == "click":
                if click_count == 0:
                    user_ctr_item["user_data"]["exposes_before_first_click"] = expose_count
                click_count += 1
        if expose_count == 0:
            user_ctr_item["user_data"]["total_ctr"] = 0
            user_ctr_item["user_data"]["exposes_before_first_click"] = 101
        else:
            if click_count == 0:
                user_ctr_item["user_data"]["exposes_before_first_click"] = 101
            user_ctr_item["user_data"]["total_ctr"] = click_count / expose_count
        
        # 存入mongodb
        collection.insert_one(user_ctr_item)
        
        # 生成csv文件
        csv_row = [user_ctr_item["user_id"],user_ctr_item["user_data"]["exposes_before_first_click"],user_ctr_item["user_data"]["total_ctr"]]
        for check in check_list:
            csv_row.append(user_ctr_item["user_data"]["first"+str(check)+"expose_count"])
        writer.writerow(csv_row)

    conn.commit()
    cursor.close()
    conn.close()
    csvfile.close()
    return csv_path

def ctr_run():
    '''
    user_time_range内所有用户
    '''
    path = os.getcwd()
    with open(path+"/config/ctr_config.json","r") as r:
        ctr_config = json.load(r)
    user_time_range = ctr_config["user_time_range"]
    r.close()
    start_time = ctr_config["user_start_time"]
    if start_time == "now":
        start_time = int(time.time())
    csv_path = get_start_ctr(start_time,user_time_range)
    ctr_analysis(csv_path)
    
def ctr_analysis(csv_path):
    '''
    计算所有user_list ctr信息的统计量，包括均值，上分位数，标准差等
    要判断新用户（新用户未浏览即流失，新用户停留->停留时间）类型的分布
    对这个用户群体进行追踪分析（可选可触发）
    对这些新用户所点击的内容进行分析（内容频道的分布）
    '''
    path = os.getcwd()
    with open(path+'/config/ctr_config.json','r') as r:
        ctr_config = json.load(r)
    ctr_analysis_interval = ctr_config["ctr_analysis_interval"]
    ctr_analysis_top = ctr_config["ctr_analysis_top"]

    check_list = range(ctr_analysis_interval,ctr_analysis_top+1,ctr_analysis_interval)
    describe_list = ["exposes_before_first_click","total_ctr"]
    for check in check_list:
        describe_list.append("first"+str(check)+"expose_count")
    print(csv_path+"usr_ctr_raw.csv")
    df = pd.read_csv(csv_path+"usr_ctr_raw.csv")
    df.fillna(0)
    describe = df.describe()
    print("describe:")
    print(describe_list)
    print(describe[describe_list])
    print("group_by total_ctr:")
    total_ctr_mesh_df = df.apply(lambda x:0.01*(x//0.01))
    total_ctr_group = total_ctr_mesh_df.groupby('total_ctr').count()
    print(total_ctr_group[["user_id"]])
    print("group_by exposes_before_first_click")
    first_click_group = df.groupby('exposes_before_first_click_group').count()
    print(first_click_group[["user_id"]])


    for check in check_list:
        check_ctr = df.groupby("first"+str(check)+"expose_count").count()
        check_ctr[["user_id"]].to_csv(path_or_buf=csv_path+"first"+str(check)+"expose_count_group.csv")


    describe[describe_list].to_csv(path_or_buf=csv_path+"describe_all.csv")
    total_ctr_group[["user_id"]].to_csv(path_or_buf=csv_path+"total_ctr_group.csv")
    first_click_group[["user_id"]].to_csv(path_or_buf=csv_path+"exposes_before_first_click_group.csv")

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
    get_start_ctr(int(time.time()),259200)

