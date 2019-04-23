# date:2019/4/23
# -*- coding: utf-8 -*-
# auth：Haohao

from REC.config.basic import *

def isConn():
    def _isConn(func):
        def __isConn(self,*args,**kwargs):
            if self.is_conn == False:
                self.connect_sql(CONNECTION_MODE)
            return func(self,*args,**kwargs)
        return __isConn
    return _isConn

def isConn_no():
    def _isConn_no(func):
        def __isConn_no(self,*args,**kwargs):
            if self.is_conn == False:
                self.connect_sql(CONNECTION_MODE)
            func(self,*args,**kwargs)
        return __isConn_no
    return _isConn_no