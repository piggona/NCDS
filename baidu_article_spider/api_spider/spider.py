# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
import requests
import pymongo

from baidu_article_spider.utils.get_request import *
from baidu_article_spider.utils.get_proxy import *

requestId = 100001

def spider():
    global requestId
    try:
        response = requests.post(url = "http://api.ydtad.com/ydt-server/cu/list",json=get_request(requestId),verify=False,allow_redirects=False,proxies=get_proxy_val())
        if response.status_code == 200:
            print(response.json())
        else:
            print("出现错误")
        requestId += 1
    except ConnectionError:
        print('Error occured')
        return []