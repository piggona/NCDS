import time
import datetime

oneday = datetime.timedelta(days=1)
DEFAULT_TIME_RANGE = 172800

TMP_DB = "dp_bi"
TMP_CTR_DB = "database_ctr"
ANALYZE_DATE = datetime.date.today()
TIME_INTERVAL = oneday
TMP_DB_HOST = 'localhost'
TMP_DB_PORT = 3306
TMP_DB_USER = "fuyu"
TMP_DB_PSWD = "Sjfy0114!!"

ONLINE_DB = 'dp_bi'
ONLINE_DB_HOST = 'rm-2zeg7277v9fkmj3bi.mysql.rds.aliyuncs.com'
ONLINE_DB_PORT = 3306
ONLINE_DB_USER = 'information'
ONLINE_DB_PSWD = 'Infor0110'