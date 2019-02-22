# date:2019/2/22
# -*- coding: utf-8 -*-
# auth：haohao

import os
import json
import random
import multiprocessing

from user_simul.userClass import *


def user_operate(user_id):
    '''
    模拟用户行为启动
    input: 模拟用户的id(user_id) <integer>
    output:
    '''
    user = userClass(user_id,0)
    user.user_read()


def user_simulator(amount):
    '''
    抽取amount个用户进行行为模拟
    input: 抽取用户的数量(amount) <integer>
    output:
    '''
    client = pymongo.MongoClient(host="localhost",port=27017)
    db = client.NCDS
    collection = db["user_profile"]
    path = os.getcwd()
    with open(path+"/config/user_config.json","r") as r:
        user_config = json.load(r)
    user_amount = user_config["user_amount"]

    random_list = range(1,user_amount+1)
    chosen_user_list = random.sample(random_list,amount)

    for chosen_user in chosen_user_list:
        users = collection.find({"random_id":chosen_user})
        for user in users:
            user_operate(user["user_id"])
        print("用户%d行为模拟完成",user["user_id"])
