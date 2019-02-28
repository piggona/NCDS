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


if __name__ == "__main__":
    user_list = fetch_new_user()
    print(user_list)

