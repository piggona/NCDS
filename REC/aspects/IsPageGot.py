# date:2019/4/23
# -*- coding: utf-8 -*-
# auth：Haohao

from REC.config.basic import *

def isPageGot():
    def _isPageGot(func):
        def __isPageGot(self,*args,**kwargs):
            if self.isPageGot == False:
                self.get_page()
            func(self,*args,**kwargs)
        return __isPageGot
    return _isPageGot