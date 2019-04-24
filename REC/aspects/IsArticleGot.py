# date:2019/4/24
# -*- coding: utf-8 -*-
# auth：Haohao

from REC.config.basic import *

def isArticleGot():
    def _isArticleGot(func):
        def __isArticleGot(self,*args,**kwargs):
            if self.is_article == False:
                self.get_article()
            func(self,*args,**kwargs)
        return __isArticleGot
    return _isArticleGot