# date:2019/4/24
# -*- coding: utf-8 -*-
# auth：Haohao

from REC.config.basic import *

def isStrategy():
    def _isStrategy(func):
        def __isStrategy(self,*args,**kwargs):
            if self.Strategy == "":
                self.init_strategy()
            func(self,*args,**kwargs)
        return __isStrategy
    return _isStrategy