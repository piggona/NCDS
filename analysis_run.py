# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time

from ctr_analysis.new_users.new_users import *

def Run():
    print(int(time.time()))
    start_time = int(time.time()) - 172800
    end_time = int(time.time())
    data_flow_analysis(start_time,end_time)
    # ctr_run()
    # article_ctr_analysis()
    # get_article_distribution()

if __name__ == "__main__":
    Run()