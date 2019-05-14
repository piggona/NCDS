import os
import time
import datetime

oneday = datetime.timedelta(days=1)
DEFAULT_TIME_RANGE = 172800

# 连接模式配置
CONNECTION_MODE = "local"

'''
离线库配置
'''
TMP_DB = "dp_bi"
TMP_CTR_DB = "database_ctr"
ANALYZE_DATE = datetime.date.today()
TIME_INTERVAL = oneday
TMP_DB_HOST = 'localhost'
TMP_DB_PORT = 3306
TMP_DB_USER = "fuyu"
TMP_DB_PSWD = "Sjfy0114!!"

'''
在线库配置
'''
ONLINE_DB = 'infomation'
ONLINE_DB_HOST = 'rm-2zeg7277v9fkmj3bi.mysql.rds.aliyuncs.com'
ONLINE_DB_PORT = 3306
ONLINE_DB_USER = 'information'
ONLINE_DB_PSWD = 'Infor0110'

'''
测试SSH环境配置
'''
SSH_USER_NAME = 'root'
SSH_PEM_PATH = '/Users/haohao/.ssh/ceshi.pem'
SSH_IP = '123.56.223.206'
SSH_PORT = 22

'''
训练向量路径配置
'''
SP_VEC = os.getcwd()+"/REC/static/specialVec.dat"
AR_VEC = os.getcwd()+""
CM_VEC = os.getcwd()+""
SIMPLE_MODEL_PATH = {"sp_vec":os.getcwd()+"/REC/static/SpecialVec.dat","ar_vec":"","cm_vec":""}

'''
运行间隔配置
'''
TRAIN_SLEEP = 86400
PROCESS_SLEEP = 43200
TOP_PUSH_SLEEP = 7200