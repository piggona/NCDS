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

if __name__ == "__main__":
	rate = [45,30,25]
	count = 100
	while count > 0:
		result = random_index(rate)
		if result == 0:
			print("get 0")
			break
		count -= 1
	count = 100
	while count > 0:
		result = random_index(rate)
		if result == 1:
			print("get 1")
			break
		count -= 1
	while count > 0:
		result = random_index(rate)
		if result == 2:
			print("get 2")
			break
		count -= 1