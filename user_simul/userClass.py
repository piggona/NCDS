# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Yan,haohao


import os
import json
import time
import random
import copy

import requests
import urllib
import pymongo

from user_simul.utils.prob_util import *
from user_simul.utils.new_user import *
from user_simul.utils.get_header import *

from requests.packages.urllib3.exceptions import InsecureRequestWarning
# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class userClass:
    def __init__(self, user_id):
        '''
        1. 判断是否为导入用户。 
        2. 新建用户？ 根据config中mode的概率获取user_mode，使用user_mode得到user_profile存储到mongodb中 : 通过user_id恢复用户对象
        '''
        client = pymongo.MongoClient(host="localhost", port=27017)
        db = client.NCDS
        mode_collection = db["user_acting_mode"]
        user_collection = db["user_profile"]

        is_user = user_collection.find({"user_id": user_id}).count()

        if not is_user:
            path = os.getcwd()
            with open(path+"/config/user_config.json") as r:
                user_config = json.load(r)
            user_prob_mode = user_config["user_prob_mode"]
            user_mode_id = random_index(user_prob_mode)
            print(user_mode_id)
            user_modes = mode_collection.find({"mode_id": user_mode_id})

            for user_mode in user_modes:
                device_prob = user_mode["acting_mode"]["device"]
                self.device = device_prob["device_type"][random_index(
                    device_prob["prob"])]
                user_conf = new_user()["data"]
                self.token = user_conf["token"]
                self.header = ra_header(self.device, user_conf["token"])
                user_profile = {
                    "user_id": user_conf["uid"], "mode_id": user_mode_id, "header": self.header}
                user_collection.insert_one(user_profile)

                self.read_preference = user_mode["acting_mode"]["read_preference"]
                self.user_id = user_conf["uid"]
                self.mode_id = user_mode_id
        else:
            users = user_collection.find({"user_id": user_id})
            for user in users:
                self.device = user["header"]["os"]
                self.header = user["header"]
                self.token = user["header"]["x-token"]
                user_mode_id = user["mode_id"]
                modes = mode_collection.find({"mode_id": user_mode_id})
                for mode in modes:
                    preference = {"channel": mode["acting_mode"]["read_preference"]
                                  ["channel"], "prob": mode["acting_mode"]["read_preference"]["prob"]}
                    self.read_preference = preference
                    self.user_id = user_id
                    self.mode_id = user_mode_id

    def get_recommend(self):
        '''
        获取推荐，将得到的信息过滤为需要分析的数据。
        input:
        output:{"article_id":article["article_id"],"trace_id":article["trace_id"],"trace_info":article["trace_info"],"scene_id":str(article["scene_id"])}
        '''
        path = os.getcwd()
        with open(path+"/config/sys_config.json", "r") as r:
            sys_config = json.load(r)
        rec_url = sys_config["api_url"] + sys_config["news_path"]
        data = {
            "type": "1",
            "refresh": "1",
            "tab": sys_config["test_scene"]
        }
        response = requests.get(url=rec_url, params=data,
                                headers=self.header, verify=False)
        data = response.json()
        article_queue = []
        for article in data["data"]["list"]:
            if article.get("article_id") is not None:
                content = {"article_id": article.get("article_id"), "trace_id": article.get("trace_id"),
                        "trace_info": article.get("trace_info"), "scene_id": str(article.get("scene_id"))}
                article_queue.append(content)
        r.close()
        return article_queue

    def get_user_read(self):
        '''
        按照用户行为分布，获取阅读队列。
        input:
        output:list<integer>
        '''
        path = os.getcwd()
        with open(path+"/config/user_config.json", "r") as r:
            user_config = json.load(r)
        read_amount = user_config["user_read_amount"]
        user_read = []
        for i in range(0, read_amount):
            user_read.append(self.read_preference["channel"][random_index(self.read_preference["prob"])])
        r.close()
        return user_read

    def expose_operation(self, article_queue):
        '''
        对给定的文章队列进行曝光操作
        input: article_queue
        output: 
        '''
        path = os.getcwd()
        with open(path+"/config/sys_config.json", "r") as r:
            sys_config = json.load(r)
        expose_url = sys_config["api_url"] + \
            sys_config["user_act_path"] + "{expose}"
        
        for article in article_queue:
            article_id = article["article_id"]
            trace_info = article["trace_info"]
            trace_id = article["trace_id"]
            data = {
                "item_id": article_id,  # 行为类型         article_id
                "trace_info": trace_info,    # 回传trace_info
                "trace_id": trace_id,      # 回传 trace_id
                "item_type": "article",     # 内容类型
                "bhv_type": "expose",    # 行为类型 如点击，曝光
                # 行为详情 如点击次数详情  int (树枝)   停留随机20-30， 点击/曝光都是1
                "bhv_value": "1",
                "scene_id": sys_config["test_scene"]    # 用户所在场景
            }
            req = requests.post(url=expose_url, data=data,
                                headers=self.header, verify=False)
            print("曝光完成:", req.json())

    def click_operation(self, article):
        '''
        对给定的文章进行点击操作
        input: article
        output: 
        '''
        path = os.getcwd()
        with open(path+"/config/sys_config.json", "r") as r:
            sys_config = json.load(r)
        click_url = sys_config["api_url"] + \
            sys_config["user_act_path"] + "{click}"

        article_id = article["article_id"]
        trace_info = article["trace_info"]
        trace_id = article["trace_id"]
        data = {
            "item_id": article_id,  # 行为类型         article_id
            "trace_info": trace_info,  # 回传trace_info
            "trace_id": trace_id,  # 回传 trace_id
            "item_type": "article",  # 内容类型
            "bhv_type": "click",  # 行为类型 如点击，曝光
            "bhv_value": "1",  # 行为详情 如点击次数详情  int (树枝)   停留随机20-30， 点击/曝光都是1
            "scene_id": sys_config["test_scene"]  # 用户所在场景
        }
        req = requests.post(url=click_url, data=data,
                            headers=self.header, verify=False)
        print("点击完成", req.json())

    def stay_operation(self, article):
        '''
        对给定的文章进行停留操作
        input: article
        output: 
        '''
        path = os.getcwd()
        with open(path+"/config/sys_config.json", "r") as r:
            sys_config = json.load(r)
        stay_url = sys_config["api_url"] + \
            sys_config["user_act_path"] + "{stay}"

        random_num = random.randint(20, 30)
        article_id = article["article_id"]
        trace_info = article["trace_info"]
        trace_id = article["trace_id"]
        data = {
            "item_id": article_id,  # 行为类型         article_id
            "trace_info": trace_info,  # 回传trace_info
            "trace_id": trace_id,  # 回传 trace_id
            "item_type": "article",  # 内容类型
            "bhv_type": "stay",  # 行为类型 如点击，曝光
            # 行为详情 如点击次数详情  int (树枝)   停留随机20-30， 点击/曝光都是1
            "bhv_value": random_num,
            "scene_id": sys_config["test_scene"]  # 频道ID
        }
        req = requests.post(url=stay_url, data=data,
                            headers=self.header, verify=False)
        print("停留完成%s " % req.json())

    def click_stay_operation(self, article):
        '''
        对给定的文章进行点击+停留操作
        input: article
        output: 
        '''
        self.click_operation(article)
        self.stay_operation(article)
    
    def proportion_sum(self,items):
        '''
        得到items中各项目的数量
        input:list<string> ['1','2','1',2','1']
        output:{'1':3,'2':2}
        '''
        item_set = set(items)
        proportion_sum = {}
        for item in item_set:
            proportion_sum[item] = items.count(item)
        return proportion_sum

    def break_recommend(self,recommend_queue):
        '''
        获取recommend_queue的推荐channel队列
        '''
        queue = []
        for article in recommend_queue:
            queue.append(article["scene_id"])
        return queue

    def read_operation(self,recommend_queue,scene_id,amount):
        '''
        模拟进行读文章操作（点击&停留）
        '''
        for rec in recommend_queue:
            if amount == 0:
                break
            else:
                if rec["scene_id"] == scene_id:
                    self.click_stay_operation(rec)
                    amount -= 1
            


    def user_read(self):
        '''
        进行用户读文章操作
        '''
        # 得到用户期望阅读的列表
        path = os.getcwd()
        with open(path+"/config/user_config.json", "r") as r:
            user_config = json.load(r)
        browse_amount = user_config["user_read_amount"]
        user_read = self.get_user_read()
        browse_prop = self.proportion_sum(user_read)
        browse = copy.deepcopy(browse_prop)

        # 模拟阅读
        recommend_prop = {}
        recommend_amount = 0
        while browse:
            recommend_queue = self.get_recommend()
            queue = self.break_recommend(recommend_queue)
            recommend = self.proportion_sum(queue)
            # 获得新推荐后的recommend_prop
            for key,value in recommend.items():
                if key in recommend_prop:
                    recommend_prop[key] = recommend_prop[key] + value
                else:
                    recommend_prop[key] = value
            # 对文章进行曝光操作
            self.expose_operation(recommend_queue)
            
            # 将用户期望阅读与推荐文章进行比对相减
            pop_keys = []
            for key,value in browse.items():
                if key in recommend:
                    if browse[key] <= recommend[key]:
                        self.read_operation(recommend_queue,key,browse[key])
                        pop_keys.append(key)
                    else:
                        self.read_operation(recommend_queue,key,recommend[key])
                        browse[key] = browse[key] - recommend[key]
            for key in pop_keys:
                browse.pop(key)
            recommend_amount += len(recommend_queue)

        # 将此次的数据存入 user_behavior
        client = pymongo.MongoClient(host="localhost",port=27017)
        db = client.NCDS
        collection = db["user_behavior"]
        behavior = {"user_id":self.user_id,"mode_id":self.mode_id,"recommend":{"amount":recommend_amount,"proportion":recommend_prop},"browse":{"amount":browse_amount,"proportion":browse_prop},"time":int(time.time)}
        collection.insert_one(behavior)


if __name__ == "__main__":
    pass
