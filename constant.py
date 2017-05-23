# -*- coding: UTF-8 -*-
# 数据库信息
DB_USER_NAME = 'root'
DB_PASSWORD = ''
DB_HOST = 'localhost'
DB_PORT = '3306'
DATABASE = 'wlws_187'
ZHUAN_DATABASE = 'iot_jtwl'
ANALYSIS_DATABASE = 'data_analysis'
DATA_TOTAL = 'data_total'

# 现网数据库引擎
# ENGINE_STR = "mysql+mysqlconnector://root:''@localhost:3306/data_analysis?charset=utf8"
ENGINE_STR = 'mysql+mysqlconnector://{}:{}@{}:{}/{}?charset=utf8'.\
    format(DB_USER_NAME, DB_PASSWORD, DB_HOST, DB_PORT, DATABASE)

# 专网数据库引擎
ZHUAN_ENGINE_STR = 'mysql+mysqlconnector://{}:{}@{}:{}/{}?charset=utf8'.\
    format(DB_USER_NAME, DB_PASSWORD, DB_HOST, DB_PORT, ZHUAN_DATABASE)

# data_analysis数据库引擎
ANALYSIS_ENGINE_STR = 'mysql+mysqlconnector://{}:{}@{}:{}/{}?charset=utf8'.\
    format(DB_USER_NAME, DB_PASSWORD, DB_HOST, DB_PORT, ANALYSIS_DATABASE)

# data_total数据库引擎
TOTAL_ENGINE_STR = 'mysql+mysqlconnector://{}:{}@{}:{}/{}?charset=utf8'.\
    format(DB_USER_NAME, DB_PASSWORD, DB_HOST, DB_PORT, DATA_TOTAL)

# 信令处理数据与移动的文件夹
TEST_FOLDER = '..\PythonData\sig_sum15\data_test'  # 测试
SOURCE_FOLDER = '..\PythonData\sig_sum15\untreated_data'     # 源文件
TREAT_FOLDER = '..\PythonData\sig_sum15\data_treated'    # 已经处理

# 日志文件名
SIG_LOG_FILE = 'sig.log'    # 信令日志


# 信令15,1h,1d
FOLDER_15 = '..\PythonData\sig\sig_15'
FOLDER_1H = '..\PythonData\sig\sig_1h'
FOLDER_1D = '..\PythonData\sig\sig_1d'
FOLDER_OK = '..\PythonData\sig\sig_ok'
FOLDER_ERROR = '..\PythonData\sig\error'

# 流量15,1h,1d
USR_FOLDER_15 = '..\PythonData\usr_sum15\usr_15'
USR_FOLDER_1H = '..\PythonData\usr_sum15\usr_h1'
USR_FOLDER_1D = '..\PythonData\usr_sum15\usr_d1'
USR_FOLDER_OK = '..\PythonData\usr_sum15\usr_ok'
USR_FOLDER_ERROR = '..\PythonData\usr_sum15\error'

# 信令定时任务sqlite的位置
SQLITE_DB = 'sqlite_db.db'

# 请求成功的结果码（现网,专网）
XIAN_SUCCESS_CAUSE = ['16', '128']
ZHUAN_SUCCESS_CAUSE = ['0']

# 临时生成文件
TMP_FOLDER = r'E:\PythonData\tmp_file'
