# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time

import pymongo
import pymysql

def fetch_new_user():
    conn = pymysql.connect(host='127.0.0.1',port=3306,user="jinyuanhao",db="infomation",passwd="Sjk0213%$")
    cursor = conn.cursor()
    user_query = "SELECT user_id from aliyun_user_info Where register_time >"+str(int(time.time()) - 86400)
    cursor.execute(user_query)
    users = cursor.fetchall()
    user_list = []
    for user in users:
        user_list.append(user[0])
    conn.commit()
    cursor.close()
    conn.close()
    return user_list

def get_start_ctr():
    '''
    得到每个user_list用户的ctr信息,存储到mongodb中
    '''
    user_ids = fetch_new_user()
    conn = pymysql.connect(host='127.0.0.1',port=3306,user="jinyuanhao",db="infomation",passwd="Sjk0213%$")
    cursor = conn.cursor()
    for user_id in user_ids:
        query = "SELECT item_id,bhv_time,user_id,bhv_type FROM aliyun_behavior_info WHERE user_id = {} ORDER BY bhv_time ASC".format(user_id)
        cursor.execute(query)
        user_items = cursor.fetchmany(100)
        print(user_items)
    conn.commit()
    cursor.close()
    conn.close()



if __name__ == "__main__":
    user_list = fetch_new_user()
    print(len(user_list))

