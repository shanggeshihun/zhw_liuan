# _*_coding:utf-8 _*_
# @Time     :2023/11/22 16:58
# @Author   : anliu
# @File     :test.py
# @Theme    :PyCharm


import time, datetime, configparser, warnings, math, platform
import sys,itertools

import numpy as np
import pandas as pd
import pymysql, presto
from sqlalchemy import create_engine

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)


plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/Onerway/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

warnings.filterwarnings("ignore")
# ------------------------数据库配置读取----------------------------
cf = configparser.ConfigParser()
if cf.read("E:/Onerway/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("E:/Onerway/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

doris_host = cf.get("prod_doris", "host")
doris_user = cf.get("prod_doris", "user")
doris_password = cf.get("prod_doris", "password")
doris_db = cf.get("prod_doris", "DB")
doris_port = cf.get("prod_doris", "port")

doris_con = create_engine(
    "mysql+pymysql://" + doris_user + ":" + doris_password + "@" + doris_host + ":" + doris_port + "/" + doris_db,
    echo=False
    )

merch_v1_sql = '''
    select cast(MERNO as varchar(10)) as merchant_no, MERNAME as merchant_name, MEREMAIL as merchant_email,
    LINKMANEMAIL as linkman_email
    from ods.ods_v1pacypay_INTERNATIONAL_MERCHANT_ri
'''
df_v1 = pd.read_sql(merch_v1_sql, con=doris_con)
df_v1.to_excel('./df_v1.xlsx')

merch_v3_sql = '''
    select merchant_no,merchant_name,merchant_email,login_email
    from ods.ods_v3db_spt_t_spt_user_merchant_ri
'''
df_v3 = pd.read_sql(merch_v3_sql, con=doris_con)
df_v3.to_excel('./df_v3.xlsx')