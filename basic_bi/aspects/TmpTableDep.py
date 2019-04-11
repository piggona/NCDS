'''
判断临时表是否完成.
仍需制作AOP：
1. 判断是否是生成报表的函数（前）自动解析其DataFrame生成csv保存并显示
2. 跨类的中间表依赖，在Scheduler中加以限制
'''
def table_is_exists(table_name_list):
    def _table_is_exists(func):
        def __table_is_exists(self,*args,**kwargs):
            for table_name in table_name_list:
                if not eval("self._is_"+table_name):
                    eval("self.get_"+table_name+"()")
            func(self,*args,**kwargs)
        return __table_is_exists
    return _table_is_exists 