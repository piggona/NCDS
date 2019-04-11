import os
from basic_bi.ctr.article import *

def Run():
    article = Article()
    result = article.display_total_ctr()
    print(result)
    article.stop_conn()

if __name__ == "__main__":
    Run()
