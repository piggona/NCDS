from tools.mysql_es.sql_to_es import *

def frame(content):
    print("-----------------")
    print("|---{}---|".format(content))
    print("-----------------")
    print("  ")
    print("  ")

def Run():
    frame("sql_es导入系统启动")
    to_elasticsearch()

if __name__ == "__main__":
    Run()