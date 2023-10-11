# _*_coding:utf-8 _*_

# @Time      : 2023/7/6  15:34
# @Author    : An
# @File      : pdd_fenghao_orders_group.py
# @Software  : PDD自动发货拉黑功能

import time, datetime
import pandas as pd
import requests
import redis
import json
from pyhive import presto
import psycopg2
from sqlalchemy import create_engine
import configparser
from dateutil.relativedelta import relativedelta
from collections import defaultdict

# ------------------------参数变量区----------------------------
# 配置运营数据库地址
cf = configparser.ConfigParser()
if cf.read("D:/code/python/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/usr/model/zhw_product/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("D:/code/python/config.ini", encoding='utf-8')

bigdata_redis_host = cf.get("bigdata_redis", "host")
bigdata_redis_port = cf.get("bigdata_redis", "port")
bigdata_redis_password = cf.get("bigdata_redis", "password")
bigdata_redis_db = cf.get("bigdata_redis", "user_db")

host = cf.get("hive_presto", "host")
username = cf.get("hive_presto", "username")
port = cf.get("hive_presto", "port")
schema = cf.get("hive_presto", "schema")
catalog = cf.get("hive_presto", "catalog")
presto_db = presto.connect(host=host, port=port, username=username, schema=schema, catalog=catalog)

holo_host = cf.get("Hologres_defend_r", "host")
holo_user = cf.get("Hologres_defend_r", "user")
holo_password = cf.get("Hologres_defend_r", "password")
holo_DB = cf.get("Hologres_defend_r", "db")
holo_port = cf.get("Hologres_defend_r", "port")
holo_cnx = create_engine(
    "postgresql+psycopg2://" + holo_user + ":" + holo_password + "@" + holo_host + ":" + holo_port + "/" + holo_DB)

wj_host = cf.get("Mysql-sjwj", "host")
wj_user = cf.get("Mysql-sjwj", "user")
wj_password = cf.get("Mysql-sjwj", "password")
wj_DB = cf.get("Mysql-sjwj", "DB")
wj_port = cf.get("Mysql-sjwj", "port")
cnx = create_engine("mysql+pymysql://" + wj_user + ":" + wj_password + "@" + wj_host + ":" + wj_port + "/" + wj_DB,
                    echo=False)

# ------------------------参数配置----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
day_last_2 = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')  # t-2
day_last_3 = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')  # t-3
day_last_7 = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')  # t-7
day_last_15 = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')  # t-30
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')  # t-30
day_last_365 = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')  # t-30
month_last_3 = (datetime.date.today() - relativedelta(months=+3)).strftime('%Y%m%d')

day_now_H = (datetime.datetime.now()).strftime('%Y%m%d%H')  # h
day_now_H_Last = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h-1
day_now_H_Last_2 = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime('%Y%m%d%H')  # h-2
day_now_H_Last_4 = (datetime.datetime.now() - datetime.timedelta(hours=4)).strftime('%Y%m%d%H')  # h-4
day_now_H_Last_13 = (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime('%Y%m%d%H')  # h-12
day_now_H_Last_24 = (datetime.datetime.now() - datetime.timedelta(hours=24)).strftime('%Y%m%d%H')  # h-24

month = (datetime.date.today()).strftime('%Y-%m')  # t-1 #当月日期
last_month = (datetime.date.today() - relativedelta(months=+1)).strftime('%Y-%m')  # t-1 #上月日期
last_month_1 = (datetime.date.today() - relativedelta(months=+2)).strftime('%Y-%m')  # t-2
day_Week = datetime.datetime.now().weekday()
pre_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).hour

def pdd_fenghao_orders_group(start_day, end_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:
    '''
    sql = """    	
        select 
            distinct 
            {2} as fk_target_id,
            1 as fk_module_type,
            e.id as fk_uid,
            a.pusername as userid,
            c.ip as userip,
            c.gameid as gameid,
            c.id as did,
            0 as user_mark,
            0 as push_no,
            cast (now() as timestamp)  as push_time,
            0 as is_inc,
            0 as deal_flag 
        from ods_zhw.zhw_zhuanzu_dingdan a 
        join ods_zhw.zhw_dingdan c 
        on a.order_id = c.id and c.part_day between to_char(date('{0}') - 3,'yyyy-mm-dd') and '{1}'
        join ods_zhw.zhw_hao_lock_details b 
        on a.order_id = b.order_id and b.order_id>0
        left join ods_zhw.safe_center_lock_target_activity_user_info d 
        on a.order_id = d.did and d.fk_target_id = {2}
        left join ods_zhw.zhw_user e  
        on a.pusername = e.jkx_userid
        where true 
        and a.third_type = 1 -- 拼多多
        and a.create_time between cast('{0} 00:00:00' as timestamp) - interval '3 days' and cast('{1} 23:59:59' as timestamp)
        and b.start_time between cast('{0} 00:00:00' as timestamp) and cast('{1} 23:59:59' as timestamp) -- 按封号时间取推送数据
        and d.did is null 
    """.format(start_day, end_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)
    print(report)

    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                      'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']

    report.to_sql(name='safe_center_lock_target_activity_user_info', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """2023-07-06，
    group_name PDD自动发货拉黑功能 55
    """
    group_id = 55
    group_id = 999

    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    start_day = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')  # t-1
    # 推送近 4 日数据
    try:
        pdd_fenghao_orders_group(start_day, end_day, group_id)
    except Exception as e:
        print('pdd_fenghao_orders_group 异常信息:\n', e)
    pass