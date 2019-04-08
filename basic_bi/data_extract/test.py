# date:2019/3/31
# -*- coding: utf-8 -*-
# auth：Haohao,fuyu

import pymysql
import pandas as pd
import numpy as np
import datetime
import time

# from basic_bi.config.basic import *

def table_is_exists(table_name):
    def _table_is_exists(func):
        def __table_is_exists(self,*args,**kwargs):
            if not eval("self._is_"+table_name):
                eval("self.get_"+table_name+"()")
            func(self,*args,**kwargs)
        return __table_is_exists
    return _table_is_exists 


class extract_data:
    def __init__(self):
        self._is_tmp_tables = False
        self._is_new_accu_user_action = False
        self._is_date_user_info = False
        self._is_tmp_accu_user_info = False
    
    def get_tmp_tables(self):
        print("get_tmp_tables")
    
    @table_is_exists("tmp_tables")
    def ho(self):
        print("运行")

if __name__ == "__main__":
    dat = extract_data()
    dat.ho()