# _*_coding:utf-8 _*_

# @Time      : 2023/7/25  10:46
# @Author    : An
# @File      : robot_defend_gamebox_ban_daily_warning.py
# @Software  : PyCharm

import time, datetime, configparser, warnings, math, platform , psycopg2
import sys
import pandas as pd
import pymysql, presto
from pyhive import presto
from sqlalchemy import create_engine
import json, requests

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件\在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysql import OperateMysql
from OperatePresto import OperatePresto
from SchedualToMysql import SchedualInfo
from OperateHologresNew import OperateHologresNew

from QqexmailSmtpAttach import QqExmailSmtp

warnings.filterwarnings("ignore")
# ------------------------数据库配置读取----------------------------
cf = configparser.ConfigParser()
if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

# hologres数据库
holo_host = cf.get("Hologres_defend_r", "host")
holo_port = cf.get("Hologres_defend_r", "port")
holo_database = cf.get("Hologres_defend_r", "DB")
holo_user = cf.get("Hologres_defend_r", "user")
holo_password = cf.get("Hologres_defend_r", "password")
holo_cnx = create_engine("postgresql+psycopg2://"+holo_user+":"+holo_password+"@"+holo_host+":"+holo_port+"/" + holo_database )

# hologres数据库
holo_host_w = cf.get("Hologres_defend_w", "host")
holo_port_w = cf.get("Hologres_defend_w", "port")
holo_database_w = cf.get("Hologres_defend_w", "DB")
holo_user_w = cf.get("Hologres_defend_w", "user")
holo_password_w = cf.get("Hologres_defend_w", "password")
holo_cnx_w = create_engine("postgresql+psycopg2://"+holo_user_w+":"+holo_password_w+"@"+holo_host_w+":"+holo_port_w+"/" + holo_database_w)
holo_cnx2 = psycopg2.connect(host=holo_host_w, port=holo_port_w, user=holo_user_w, password=holo_password_w, database=holo_database_w)

# hologres-gamebox数据库
holo_gbox_host = cf.get("Holo_dfbox_defend_r", "host")
holo_gbox_port = cf.get("Holo_dfbox_defend_r", "port")
holo_gbox_database = cf.get("Holo_dfbox_defend_r", "DB")
holo_gbox_user = cf.get("Holo_dfbox_defend_r", "user")
holo_gbox_password = cf.get("Holo_dfbox_defend_r", "password")
holo_gbox_cnx = create_engine("postgresql+psycopg2://"+holo_gbox_user+":"+holo_gbox_password+"@"+holo_gbox_host+":"+holo_gbox_port+"/" + holo_gbox_database )

# presto数据库
host = cf.get("hive_presto_hive", "host")
username = cf.get("hive_presto_hive", "username")
port = cf.get("hive_presto_hive", "port")
schema = cf.get("hive_presto_hive", "schema")
catalog = cf.get("hive_presto_hive", "catalog")
presto_db = presto.connect(host=host, port=port, username=username, schema=schema, catalog=catalog)



def dfbox_hitname_login_info(start_day, end_day):
    '''
    :param start_day:
    :param end_day:
    :return:安全防御检测的刀锋盒子的外挂及其投诉用量
    '''
    #
    final_report = pd.DataFrame()

    # 安防外挂检测的盒子上号token
    presto_sql = """    	
        select part_day,jsm
        from kudu.safe_center.safe_log_v5  
        where true
        and part_day between '{0}' and '{1}'
        and typ = 'BOX_HITNAME'
        and jsm not like 'xxxxx%%'
        group by 1,2
    """.format(start_day, end_day)
    presto_result = pd.read_sql(presto_sql, con=presto_db)
    print(presto_result)
    # 无结果返回则直接退出
    if not len(presto_result):
        return
    # 有结果返回则token串连
    jsm_concat = ','.join(["'" + str(a) + "'" for a in presto_result.jsm])

    # 盒子查询给定token的上号记录
    gamebox_sql = """    	
        -- 近6小时写入数据
        select to_char(a.create_time,'yyyy-mm-dd') as part_day ,
        a.create_time, a.game_name, a.game_account, a.login_token, 
        a.first_login_time, a.account_login_time, 
        a.ip_address, a.mac_address, b.username, b.nickname, b.email
        from ods.account_login_record a
        left join ods.member b 
        on a.member_id = b.id 
        where a.login_token in ({2})
        and a.create_time between cast('{0} 00:00:00' as timestamp) and cast('{1} 23:59:59' as timestamp)
    """.format(start_day, end_day, jsm_concat)
    gamebox_result = pd.read_sql(gamebox_sql, con=holo_gbox_cnx)
    print(gamebox_result)
    sys.exit()
    # 无结果返回则直接退出
    if not len(gamebox_result):
        return
    # 有结果返回则写入到holo public
    cursor = holo_cnx2.cursor()
    delete_sql = "delete from public.dfbox_hitname_login_info where part_day between '{0}' and '{1}'".format(start_day, end_day)
    cursor.execute(delete_sql)

    insert_sql = "insert into public.dfbox_hitname_login_info values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    cursor.executemany(insert_sql, gamebox_result.values.tolist())
    holo_cnx2.commit()

if __name__ == '__main__':
    for i in range(1):
        end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
        start_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
        start_day = '2023-07-05'
        dfbox_hitname_login_info(start_day, start_day)
    holo_cnx2.close()