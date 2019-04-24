# date:2019/4/24
# -*- coding: utf-8 -*-
# auth：Haohao

from REC.config.basic import *

def isVecTrained():
    def _isVecTrained(func):
        def __isVecTrained(self,*args,**kwargs):
            if self.sp_vec_path == "":
                self.sp_vec_path = '/Users/haohao/Documents/NCDS/REC/static/specialVec.dat'
            return func(self,*args,**kwargs)
        return __isVecTrained
    return _isVecTrained