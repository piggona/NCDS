# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Yan,haohao


import os
import json
import time
import random

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
    def __init__(self,user_id):
        '''
        1. 判断是否为导入用户。 
        2. 新建用户？ 根据config中mode的概率获取user_mode，使用user_mode得到user_profile存储到mongodb中 : 通过user_id恢复用户对象
        '''
        client = pymongo.MongoClient(host="localhost",port=27017)
        db = client.NCDS
        mode_collection = db["user_acting_mode"]
        user_collection = db["user_profile"]

        is_user = user_collection.find({"user_id":user_id}).count()

        if not is_user:
            path = os.getcwd()
            with open(path+"/config/user_config.json") as r:
                user_config = json.load(r)
            user_prob_mode = user_config["user_prob_mode"]
            user_mode_id = random_index(user_prob_mode)
            print(user_mode_id)
            user_modes = mode_collection.find({"mode_id":user_mode_id})

            for user_mode in user_modes:
                device_prob = user_mode["acting_mode"]["device"]
                self.device = device_prob["device_type"][random_index(device_prob["prob"])]
                user_conf = new_user()["data"]
                self.token = user_conf["token"]
                self.header = ra_header(self.device,user_conf["token"])
                user_profile = {"user_id":user_conf["uid"],"mode_id":user_mode_id,"header":self.header}
                user_collection.insert_one(user_profile)

                self.read_preference = user_mode["acting_mode"]["read_preference"]
                self.user_id = user_conf["uid"]
                self.mode_id = user_mode_id
        else:
            users = user_collection.find({"user_id":user_id})
            for user in users:
                self.device = user["header"]["os"]
                self.header = user["header"]
                self.token = user["header"]["x-token"]
                user_mode_id = user["mode_id"]
                modes = mode_collection.find({"mode_id":user_mode_id})
                for mode in modes:
                    print(mode)
                    preference = {"channel":mode["acting_mode"]["read_preference"]["channel"],"prob":mode["acting_mode"]["read_preference"]["prob"]}
                    self.read_preference = preference
                    self.user_id = user_id
                    self.mode_id = user_mode_id
    

    def get_recommend(self):
        path = os.getcwd()
        with open(path+"/config/sys_config.json") as r:
            sys_config = json.load(r)
        rec_url = sys_config["api_url"] + sys_config["news_path"]
        data = {
            "type": "1",
            "refresh": "1",
            "tab": sys_config["test_scene"]
        }
        
        response = requests.get(url=rec_url,params=data,headers=self.header,verify=False)
        data = response.json()
        return response

if __name__ == "__main__":
    pass
        
