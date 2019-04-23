# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time
from sklearn.datasets.base import Bunch

# from user_simul.userClass import *
# from user_simul.user_generator import *
# from user_simul.user_simulator import *
# from ctr_analysis.new_users.new_users import *
# from Spider.baidu_article_spider.api_spider.spider import *
# from basic_bi.ctr.article import *
# from tools.mysql_es.sql_to_es import *
from REC.Scheduler import *
from REC.logs.logger import *


# def sql_es_run():
#     logger.info("sql_es导入系统启动")
#     to_elasticsearch()

# def basic_bi_run():
#     article = Article()
#     result = article.display_total_ctr()
#     print(result)
#     article.stop_conn()

# def baidu_spider_run():
#     print(int(time.time()))
#     # spider_generator()
#     spider_op()

# def analysis_run():
#     print("分析时间点：")
#     print(int(time.time()))
#     print("--------------------------------")
#     print("-----Welcome-----")
#     print("--------------------------------")
#     start_time = int(time.time()) - 172800
#     end_time = int(time.time())
#     data_flow_analysis(start_time,end_time)
#     ctr_run()

def rec_run():
    print("rec信息源清洗工具启动")
    Sc = Scheduler()
    # print("进行计算")
    # Sc.get_special_vec()

    # print("输出结果:")
    # print("正样本：")
    # Sc_bunch = Sc.SpecialVec
    # Sc_pos = Sc_bunch.source_pos
    # Sc_neg = Sc_bunch.source_neg
    # print("作者")
    # print(Sc_pos)
    # print("作者-频道")
    # print(Sc_bunch.source_channel_pos)
    # print("频道")
    # print(Sc_bunch.channel_pos)
    # print("负样本：")
    # print("作者")
    # print(Sc_neg)
    # print("作者-频道")
    # print(Sc_bunch.source_channel_neg)
    # print("频道")
    # print(Sc_bunch.channel_neg)
    Sc.online_output()
    # Sc.kill_conn()

def rec_generator(self):
    print("rec信息源清洗工具启动")
    Sc = Scheduler()

    pool = multiprocessing.Pool(processes=10)
    
    pool.apply_async(Sc.get_special_vec)
    pool.apply_async(Sc.online_output)
    pool.close()
    pool.join()
    print("进程结束")
    
if __name__ == "__main__":
    rec_run()