import os
import time
import pymysql
import gc
from elasticsearch import Elasticsearch

def get_sql_generator():
    conn = pymysql.connect(host='rm-2zeg7277v9fkmj3bi.mysql.rds.aliyuncs.com',
                           port=3306, user="information", db="infomation", passwd="Infor0110")
    cursor = conn.cursor()

    first_query = "SELECT a.id as article_id, a.category as category, a.pub_time as create_time,a.expire_time as expire_time,a.site_id as site_id,JSON_EXTRACT(a.extend, '$.source') as source,JSON_EXTRACT(a.extend, '$.summary') as summary,a.title as title,a.url as url FROM article_resource as a WHERE a.expire_time > NOW() LIMIT 100"
    cursor.execute(first_query)
    user_items = cursor.fetchall()
    for user_item in user_items:
        data = {"article_id":user_item[0],"category":user_item[1],"create_time":user_item[2],"expire_time":user_item[3],"site_id":user_item[4],"source":user_item[5],"summary":user_item[6],"title":user_item[7],"url":user_item[8]}
        yield data
        del data
        gc.collect(data)
    conn.commit()
    cursor.close()
    conn.close()

def to_elasticsearch():
    es = Elasticsearch([{'host':'localhost','port':5200}])
    frame("获取es对象")
    for data in get_sql_generator():
        frame("得到数据")
        es.index(index="mifeng_article",doc_type="mifeng",body=data)

def frame(content):
    print("-----------------")
    print("|---{}---|".format(content))
    print("-----------------")
    print("  ")
    print("  ")

if __name__ == "__name__":
    to_elasticsearch()