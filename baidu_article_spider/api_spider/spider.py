# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import requests
import pymongo
import json

from baidu_article_spider.utils.get_request import *
from baidu_article_spider.utils.get_proxy import *

requestId = 100001

def spider():
    global requestId
    try:
        response = requests.post(url = "http://api.ydtad.com/ydt-server/cu/list",json=get_request(requestId),verify=False,allow_redirects=False,proxies=get_proxy_val())
        if response.status_code == 200:
            handle_response(response.json())
            print("200")
        else:
            print("出现错误")
        requestId += 1
    except ConnectionError:
        print('Error occured')
        return []

def handle_response(res):
    client = pymongo.MongoClient(host="localhost",port=27017)
    db = client.baiduContent
    collection = db["baidu_news"]
    result = {"requestId":"","time":int(time.time()),"data":{}}
    result["requestId"] = res["requestId"]
    items = res["items"]
    for item in items:
        data_str = item["data"]
        data = json.loads(data_str)
        result["data"] = data
        collection.insert_one(result)