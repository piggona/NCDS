# date:2019/3/25
# -*- coding: utf-8 -*-
# auth：Haohao

import requests
import json
import pymysql
import os

from BA_service.config.config import BASIC_QUERY

class mysql_search:
    def __init__(self, basic_query):
        self.basic_query =basic_query
        self.conn = pymysql.connect(
                user="fuyu",
                passwd="Sjfy0114!!",
                host="127.0.0.1",  # 此处必须是 127.0.0.1
                db='infomation',
                port=3306)
        self.cursor =self.conn.cursor()

    def build_search_query(self, word, from_page, page_size):
        self.basic_query["from"] = int(from_page) * 10
        self.basic_query["size"] = page_size
        self.basic_query["query"]["query_string"]["query"] = word
        self.search_for_all()
     
    def next_query(self):
        self.basic_query["from"] += 1

    def search_for_all(self):
        res = {"hits":{"hits":[]}}

        self.cursor.execute(self.basic_query)
        results = self.cursor.fetchall()
        for result in results:
            print(result[0])
            print(result[4])
            print(result[11])
            print(result[16])
            print(result[5])
            inner = {"_source":{},"_id":""}
            inner["_source"]["title"] = result[5]
            inner["_source"]["extend"] = result[16]
            inner["_source"]["tags"] = result[11]
            inner["_source"]["url"] = result[4]
            inner["_id"] = result[0]
            res["hits"]["hits"].append(inner)
        self.cursor.close()
        self.conn.close()
        return res