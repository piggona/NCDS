# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import requests
import pymongo
import json
import multiprocessing

from baidu_article_spider.utils.get_request import *
from baidu_article_spider.utils.get_proxy import *

requestId = 100001

def spider_op():
    global requestId
    while True:
        print(requestId)
        try:
            response = requests.post(url = "http://api.ydtad.com/ydt-server/cu/list",json=get_request(requestId),verify=False,allow_redirects=False,proxies=get_proxy_val(),timeout=1)
            if response.status_code == 200:
                handle_response(response.json())
                print("200")
            else:
                print("出现错误")
            requestId += 1
        except ConnectionError:
            print('Error occured')
        except:
            print("出错")

def handle_response(res):
    client = pymongo.MongoClient(host="localhost",port=27017)
    db = client.baiduContent
    collection = db["baidu_news"]
    items = res["items"]
    for item in items:
        result = {"requestId":"","time":int(time.time()),"data":{},"doc_id":""}
        result["requestId"] = res["requestId"]
        data_str = item["data"]
        data = json.loads(data_str)
        result["data"] = data
        result["doc_id"] = data["id"]
        if collection.find({"doc_id":data["id"]}).count() == 0:
            collection.insert_one(result)

def spider_generator():
    pool = multiprocessing.Pool(processes= 10)
    for i in range(0,10):
        pool.apply_async(spider_op)
        print("正在进行第{}个进程".format(i+1))
    pool.close()
    pool.join()
    print("进程结束")