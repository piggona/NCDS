# date:2019/3/25
# -*- coding: utf-8 -*-
# auth：Haohao

import requests
import json
import elasticsearch
import os

from BA_service.config.config import BASIC_QUERY

res = requests.get('http://localhost:5200/mifeng_article/mifeng/_search')
print(res.content)

class es_search:
    def __init__(self, basic_query):
        self.basic_query = basic_query
    
    @classmethod
    def get_basic_search(cls):
        return cls(
            basic_query= BASIC_QUERY,
        )

    def build_search_query(self, word, from_page, page_size):
        self.basic_query["from"] = from_page
        self.basic_query["size"] = page_size
        self.basic_query["query"]["query_string"]["query"] = word
        self.search_for_all()
    
    def next_query(self):
        self.basic_query["from"] += 1

    def search_for_all(self):
        es = elasticsearch.Elasticsearch([{"host": 'localhost','port': 5200}])
        try:
            res = es.search(index='infomation',body = self.basic_query)
            RES = json.dumps(res,indent=2)
            return RES
        except elasticsearch.exceptions.NotFoundError:
            return "not found %".format(self.basic_query)