# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time

import pymongo
import pymysql

def fetch_new_user():
    conn = pymysql.connect(host='127.0.0.1',port=3306,user="jinyuanhao",passwd="Sjk0213%$",charset='utf-8')
    cursor = conn.cursor()
    cursor.execute("SELECT user_id from aliyun_user_info Where register_time > 1551241093")
    row = cursor.fetchall()
    print(row)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    fetch_new_user()

