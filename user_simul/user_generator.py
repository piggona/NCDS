# date:2019/2/22
# -*- coding: utf-8 -*-
# auth：haohao


import os
import json
import multiprocessing

import pymongo

from user_simul.userClass import *

def get_user(amount,offset):
    count = amount
    while count > 0:
        userClass(0,offset+count)
        count -= 1
    print("成功创建%d个用户",amount)

def user_generator(threads):
    path = os.getcwd()
    with open(path+"/config/user_config.json","r") as r:
        user_config = json.load(r)
    user_amount = user_config["user_amount"]
    amount = user_amount // threads
    pool = multiprocessing.Pool(processes= threads)
    for i in range(0,threads):
        pool.apply_async(get_user,(amount,i*amount,))
        print("正在创建%d个用户",amount)
    pool.close()
    pool.join()
    print("进程结束")

if __name__ == "__main__":
    pass
    


