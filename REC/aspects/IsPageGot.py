# date:2019/4/23
# -*- coding: utf-8 -*-
# auth：Haohao

from REC.config.basic import *

def isPageGot():
    def _isConn(func):
        def __isConn(self,*args,**kwargs):
            if self.is_conn == False:
                self.connect_sql(CONNECTION_MODE)
            func(self,*args,**kwargs)
        return __table_is_exists
    return _table_is_exists 