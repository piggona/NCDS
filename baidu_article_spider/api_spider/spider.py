# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import requests
import pymongo
import json
import re
import datetime
import multiprocessing
import datetime
import pymysql

from baidu_article_spider.utils.get_request import *
from baidu_article_spider.utils.get_proxy import *

requestId = 100001


def spider_op():
    global requestId
    client = pymongo.MongoClient(host="localhost", port=27017)
    db = client.baiduContent
    collection = db["baidu_err"]
    while True:
        print(requestId)
        try:
            response = requests.post(url="http://api.ydtad.com/ydt-server/cu/list", json=get_request(
                requestId), verify=False, allow_redirects=False, proxies=get_proxy_val(), timeout=3)
        except ConnectionError:
            print('Error occured')
        except:
            print("出错")
        else:
            if response.status_code == 200:
                try:
                    handle_response(response.json())
                except Exception as e:
                    err = {"err":str(e),"time":int(time.time())}
                    collection.insert_one(err)
                print("200")
            else:
                print("出现错误")
        requestId += 1

def get_str_date():
    today=datetime.date.today()
    formatted_today="20" + today.strftime('%y%m%d')
    return formatted_today

def get_sql_dat(result):
    sql_dat = {}
    sql_dat["resource_id"] = 0
    sql_dat["site_id"] = 5
    sql_dat["article_type"] = 2
    sql_dat["url"] = "'"+ get_simple_url(result["data"]["detailUrl"]) +"'"
    sql_dat["title"] = result["data"]["title"]
    cate = result["data"]["catInfo"]["id"]
    if (cate == 1001) or (cate == 1026):
        sql_dat["category"] = 1001
        sql_dat["scene_id"] = json.dumps([1000, 1001])
    elif cate == 1002:
        sql_dat["category"] = 1003
        sql_dat["scene_id"] = json.dumps([1000, 1003])
    elif cate == 1007:
        sql_dat["category"] = 1004
        sql_dat["scene_id"] = json.dumps([1000, 1004])
    elif (cate == 1005) or (cate == 1013):
        sql_dat["category"] = 1006
        sql_dat["scene_id"] = json.dumps([1000, 1006])
    elif (cate == 1006):
        sql_dat["category"] = 1007
        sql_dat["scene_id"] = json.dumps([1000, 1007])
    elif (cate == 1008):
        sql_dat["category"] = 1015
        sql_dat["scene_id"] = json.dumps([1000, 1015])
    elif (cate == 1009):
        sql_dat["category"] = 1018
        sql_dat["scene_id"] = json.dumps([1000, 1018])
    elif (cate == 1011) or (cate == 1020):
        sql_dat["category"] = 1010
        sql_dat["scene_id"] = json.dumps([1000, 1010])
    elif (cate == 1012) or (cate == 1029):
        sql_dat["category"] = 1009
        sql_dat["scene_id"] = json.dumps([1000, 1009])
    elif (cate == 1014):
        sql_dat["category"] = 1002
        sql_dat["scene_id"] = json.dumps([1000, 1002])
    elif (cate == 1015):
        sql_dat["category"] = 1020
        sql_dat["scene_id"] = json.dumps([1000, 1020])
    elif (cate == 1016):
        sql_dat["category"] = 1009
        sql_dat["scene_id"] = json.dumps([1000, 1009])
    elif (cate == 1017):
        sql_dat["category"] = 1012
        sql_dat["scene_id"] = json.dumps([1000, 1012])
    elif (cate == 1018):
        sql_dat["category"] = 1023
        sql_dat["scene_id"] = json.dumps([1000, 1023])
    elif (cate == 1019):
        sql_dat["category"] = 1021
        sql_dat["scene_id"] = json.dumps([1000, 1021])
    elif (cate == 1027):
        sql_dat["category"] = 1014
        sql_dat["scene_id"] = json.dumps([1000, 1014])
    elif (cate == 1031):
        sql_dat["category"] = 1016
        sql_dat["scene_id"] = json.dumps([1000, 1016])
    else:
        sql_dat["category"] = 9999
        sql_dat["scene_id"] = json.dumps([1000, 9999])
    sql_dat["pub_time"] = int(time.time())
    sql_dat["expire_time"] = int(time.time()) + 172800
    sql_dat["last_modify_time"] = int(time.time())
    tags = []
    try:
        for tag in result["data"]["tags"]:
            tags.append(tag["text"])
    except:
        print("no tags")
    sql_dat["tags"] = json.dumps(tags,ensure_ascii=False)
    sql_dat["weight"] = 1
    sql_dat["aliyun_info"] = json.dumps({},ensure_ascii=False)
    sql_dat["status"] = 1
    sql_dat["contents"] = ""
    extend = {"source": "", "summary": "", "media_pic": "", "video_url": [
    ], "image_urls": [], "media_name": "", "video_duration": 0, "image_thumbs_urls": []}
    extend["source"] = result["data"]["source"]
    extend["summary"] = result["data"]["brief"]
    extend["media_pic"] = result["data"]["bigPicUrl"]
    extend["video_url"] = []
    for image in result["data"]["images"]:
        extend["image_urls"].append(image)
    extend["image_thumbs_urls"] = extend["image_urls"]
    sql_dat["pub_date"] = get_str_date()
    sql_dat["extend"] = json.dumps(extend,ensure_ascii=False)
    sql_dat["create_time"] = int(time.time())
    tss1 = result["data"]["updateTime"]
    timeArray = time.strptime(tss1, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    sql_dat["update_time"] = int(time.time())
    sql_dat["cas_token"] = 0
    return sql_dat

def get_simple_url(url):
    pattern = re.compile(".*?\?")
    out = pattern.match(url).group(0)
    output = out[0:-1] + "?no_list=1&scene=2&forward=api&bside=1&api_version=2&c=24010555"
    old = "b8bb2dff"
    new = "b8a516a5"
    out = output.replace(old,new)
    return out

def handle_response(res):
    conn = pymysql.connect(host='rm-2zeg7277v9fkmj3bi.mysql.rds.aliyuncs.com',
                           port=3306, user="information", db="infomation", passwd="Infor0110")
    cursor = conn.cursor()

    client = pymongo.MongoClient(host="localhost", port=27017)
    db = client.baiduContent
    collection = db["baidu_news"]
    items = res["items"]
    for item in items:
        result = {"requestId": "", "time": int(
            time.time()), "data": {}, "doc_id": ""}
        result["requestId"] = res["requestId"]
        data_str = item["data"]
        data = json.loads(data_str)
        result["data"] = data
        result["doc_id"] = data["id"]
        if collection.find({"doc_id": data["id"]}).count() == 0:
            collection.insert_one(result)
            sql_dat = get_sql_dat(result)
            query = "INSERT INTO article_resource (resource_id,site_id,article_type,url,title,category,pub_time,expire_time,last_modify_time,scene_id,tags,weight,aliyun_info,status,contents,extend,create_time,update_time,cas_token) VALUES ({0},{1},{2},{3},'{4}',{5},{6},{7},{8},'{9}','{10}',{11},'{12}',{13},'{14}','{15}',{16},{17},{18})".format(
                sql_dat["resource_id"], sql_dat["site_id"], sql_dat["article_type"], sql_dat["url"], sql_dat["title"], sql_dat["category"], sql_dat["pub_time"], sql_dat["expire_time"], sql_dat["last_modify_time"], sql_dat["scene_id"], sql_dat["tags"], sql_dat["weight"], sql_dat["aliyun_info"], sql_dat["status"],sql_dat["contents"], sql_dat["extend"], sql_dat["create_time"], sql_dat["update_time"], sql_dat["cas_token"])
            try:
                cursor.execute(query)
            except Exception as e:
                print(e)
    conn.commit()
    cursor.close()
    conn.close()


def spider_generator():
    pool = multiprocessing.Pool(processes=10)
    for i in range(0, 10):
        pool.apply_async(spider_op)
        print("正在进行第{}个进程".format(i+1))
    pool.close()
    pool.join()
    print("进程结束")


if __name__ == "__main__":
    # tss1 = '2019-02-25 17:41:44'
    # timeArray = time.strptime(tss1, "%Y-%m-%d %H:%M:%S")
    # timeStamp = int(time.mktime(timeArray))
    # print(timeStamp)
    string = "https://cpu.baidu.com/api/1022/b8bb2dff/detail/27436438320630349/news?no_list=1&scene=2&forward=api&bside=1&api_version=2&c=24010555"
    old = "b8bb2dff"
    new = "b8a516a5"
    kk = string.replace(old,new)
    print(string.replace(old,new))
    print(string)
    print(kk)