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


def get_new_user_ctr(start_time,user_time_range):
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
    csv_path = get_new_user_ctr(start_time,user_time_range)
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
    print(describe[describe_list])
    print("group_by total_ctr:")
    total_ctr_mesh_df = df.apply(lambda x:0.01*(x//0.01))
    total_ctr_group = total_ctr_mesh_df.groupby('total_ctr').count()
    print(total_ctr_group[["user_id"]])
    print("group_by exposes_before_first_click")
    first_click_group = df.groupby('exposes_before_first_click').count()
    print(first_click_group[["user_id"]])

    for check in check_list:
        check_ctr = df.groupby("first"+str(check)+"expose_count").count()
        check_ctr[["user_id"]].to_csv(path_or_buf=csv_path+"first"+str(check)+"expose_count_group.csv")

    describe[describe_list].to_csv(path_or_buf=csv_path+"describe_all.csv")
    total_ctr_group[["user_id"]].to_csv(path_or_buf=csv_path+"total_ctr_group.csv")
    first_click_group[["user_id"]].to_csv(path_or_buf=csv_path+"exposes_before_first_click_group.csv")

def article_ctr_analysis():
    '''
    想知道高ctr的文章是否推荐给了新用户
    article_time_range时间跨度内
    计算文章(item_id)不同时间段(create_time+range)内的article_ctr,click/expose
    计算文章被曝光次数expose:
    1. 文章(item_id)对应时间段的expose生命周期折线
    2. 在相同时间段节点，各个文章的article_ctr->expose图像
    静态解决方案：
    1. 抽样：取user_time_range的时间区间内的用户文章（用户文章需要调用mongodb->start_ctr表寻找新用户，并取出那些新用户所被曝光的文章）
    2. 计算expose比率：计算这些文章在总共expose中的比率,根据expose比率将文章分类
    3. ctr计算：计算这些文章的ctr（计算整体的ctr,对所有用户）
    4. 文章分组：根据计算出来的ctr对文章分组。
    5. 比较expose各组元素与ctr各组元素的匹配度、
    实时方案：
    实时对文章分组，分expose组与ctr组，计算expose各组数量与ctr各组数量的回归是否契合
    主要问题点：
    1. 实时性
    2. 动态对各组中元素做调整，根据expose与ctr的变化改变各分组的元素
    '''
    # 初始化
    client = pymongo.MongoClient(host="localhost")
    db = client.ctrAnalytics

    conn = pymysql.connect(host='127.0.0.1',port=3306,user="jinyuanhao",db="infomation",passwd="Sjk0213%$")
    cursor = conn.cursor()

    # 抽样
    collection = db["start_ctr"]
    user_datas = collection.find()
    
    article_list = []
    article_distribute = {}
    article_count = {}
    article_count_group = {"less10":[],"10to50":[],"50to100":[],"upper100":[]}
    article_count_group_count = {"less10":0,"10to50":0,"50to100":0,"upper100":0}
    
    for user_data in user_datas:
        raw_datas = user_data["user_data"]["raw_data"]
        for raw_data in raw_datas:
            if raw_data[1] == "expose":
                article_list.append(raw_data[0])
    expose_amount = len(article_list)
    article_set = set(article_list)
    for article in article_set:
        article_distribute[str(article)] = article_list.count(article)/expose_amount
        article_count[str(article)] = article_list.count(article)
        if article_list.count(article) <= 10:
            article_count_group_count["less10"] += 1
            article_count_group["less10"].append(str(article))
        elif article_list.count(article) <= 50:
            article_count_group_count["10to50"] += 1
            article_count_group["10to50"].append(str(article))
        elif article_list.count(article) <= 100:
            article_count_group_count["50to100"] += 1
            article_count_group["50to100"].append(str(article))
        else:
            article_count_group_count["upper100"] += 1
            article_count_group["upper100"].append(str(article))
    # expose比率
    article_amount = len(article_set)
    print("总曝光数：")
    print(expose_amount)
    print("总取样文章数：")
    print(len(article_set))
    print("各’新用户曝光数‘区间内取样文章数量：")
    print(article_count_group_count)
    # article比率
    article_ctr_distribute = {}
    article_ctr_group = {"zero":[],"0to3":[],"3to6":[],"6to10":[],"upper10":[]}
    article_ctr_group_count = {"zero":0,"0to3":0,"3to6":0,"6to10":0,"upper10":0}
    for article in article_set:
        first_query = "SELECT count(*) as op_amount,bhv_type FROM aliyun_behavior_info WHERE item_id = '{}' GROUP BY bhv_type".format(article)
        cursor.execute(first_query)
        ctr_objs = cursor.fetchall()
        click_amount = 0
        expose_amount = 0
        for ctr_obj in ctr_objs:
            if ctr_obj[1] == "expose":
                expose_amount = ctr_obj[0]
            if ctr_obj[1] == "click":
                click_amount = ctr_obj[0]
        if expose_amount == 0:
            ctr = 0.0
            article_ctr_distribute[str(article)] = 0.0
        else:
            ctr = click_amount/expose_amount
            article_ctr_distribute[str(article)] = ctr
        if str(ctr) == "0.0":
            article_ctr_group["zero"].append(str(article))
            article_ctr_group_count["zero"] += 1
        elif ctr <= 0.03:
            article_ctr_group["0to3"].append(str(article))
            article_ctr_group_count["0to3"] += 1
        elif ctr <= 0.06:
            article_ctr_group["3to6"].append(str(article))
            article_ctr_group_count["3to6"] += 1
        elif ctr <= 0.1:
            article_ctr_group["6to10"].append(str(article))
            article_ctr_group_count["6to10"] += 1
        else:
            article_ctr_group["upper10"].append(str(article))
            article_ctr_group_count["upper10"] += 1
    print("各’ctr区间（百分数）-ctr是取样文章的总体ctr‘的取样文章数:")
    print(article_ctr_group_count)

    # 计算匹配数量
    for ctr_key,ctr_list in article_ctr_group.items():
        for count_key,count_list in article_count_group.items():
            result_list = list(set(ctr_list).intersection(set(count_list)))
            print("ctr区间 {0} , 对应的expose区间 {1} 的文章匹配度是：{2}".format(ctr_key,count_key,len(result_list)/len(ctr_list)))

    
    conn.commit()
    cursor.close()
    conn.close()        

def data_flow_analysis():
    '''
    各个新闻源文章的ctr与其曝光比率的分布
    分为对全体用户的ctr及其曝光比率的分布，及对新用户的分布
    1. aliyun_article_info:先找到article_time_range时间段内所有的文章记录，统计各个新闻源的数量分布，按新闻源做文章分类
    2. aliyun_behavior_info:各类新闻源article组中，循环文章id，将得到的expose与click累加
    3. aliyun_behavior_info:得到各新闻源的ctr分布
    4. aliyun_behavior_info:计算各新闻源的expose分布
    '''
    pass


if __name__ == "__main__":
    get_new_user_ctr(int(time.time()),259200)

