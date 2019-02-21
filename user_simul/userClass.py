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
        client = pymongo.MongoClient(host="localhost",port=27017)
        db = client.mifeng_user
        mode_collection = db["user_acting_mode"]
        user_collection = db["user_profile"]

        is_user = user_collection.find({"user_id":user_id}).count()

        if not is_user:
            path = os.getcwd()
            with open(path+"/config/user_config.json") as r:
                user_config = json.load(r)
            user_prob_mode = user_config["user_prob_mode"]
            user_mode_id = random_index(user_prob_mode)
            user_mode = mode_collection.find_one({"mode_id":user_mode_id})

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
            user = user_collection.find_one({"user_id":user_id})
            self.device = user["header"]["os"]
            self.header = user["header"]
            self.token = user["header"]["x-token"]
            user_mode_id = user["mode_id"]
            self.read_preference = mode_collection.find_one({"mode_id":user_mode_id})["acting_mode"]["read_preference"]
            self.user_id = user_id
            self.mode_id = user_mode_id
    

    def get_recommend(self):
        path = os.getcwd()
        with open(path+"/config/sys_config.json") as r:
            sys_config = json.load(r)
        url = sys_config["api_url"] + sys_config["news_path"]
         
