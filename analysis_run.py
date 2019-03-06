# date:2019/2/19
# -*- coding: utf-8 -*-
# auth：Haohao

import os
import time

from ctr_analysis.new_users.new_users import *

def Run():
    print(int(time.time()))
    ctr_run()
    article_ctr_analysis()

if __name__ == "__main__":
    Run()