# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：zhuxiongxian,Haohao

import random

def random_index(rate):
    '''
    返回概率事件的下标索引
    input: rate(list<int>,百分数概率)
    output: index(int)
    '''
    start = 0
    index = 0
    randnum = random.randint(1, sum(rate))

    for index, scope in enumerate(rate):
        start += scope
        if randnum <= start:
            break
    return index