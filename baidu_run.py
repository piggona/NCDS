import os
import time

from baidu_article_spider.api_spider.spider import *

def Run():
    print(int(time.time()))
    spider_generator()

if __name__ == "__main__":
    Run()