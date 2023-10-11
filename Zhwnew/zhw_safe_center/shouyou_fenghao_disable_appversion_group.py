# _*_coding:utf-8 _*_

# @Time      : 2023/7/21  15:34
# @Author    : An
# @File      : shouyou_fenghao_disable_appversion_group.py
# @Software  : 安卓停用版本上号风险用户


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


def shouyou_fenghao_disable_appversion_group(start_day, end_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:
    '''
    sql = """    	
        -- 2023/7/10 11:12 对安卓部分历史(高封号低单量)版本停用
        -- 安卓主版本400100000 版本'6291', '6150', '6300', '6250', '6180', '6310', '6120', '6260', '6270', '6190','6140'
        select 
            {2} as fk_target_id,
            1 as fk_module_type,
            f.id as fk_uid,
            c.userid,
            c.ip as userip,
            c.gameid,
            a.order_id as did,
            0 as user_mark,
            0 as push_no,
            cast (now() as timestamp)  as push_time,
            0 as is_inc,
            0 as deal_flag 
        from ods_zhw.zhw_quick_zhw_quick_queue a 
        left join ods_zhw.zhw_dict_item b 
        on a.attr_ext::json->>'app_id' = b.item_value and b.dict_id = 124
        join ods_zhw.zhw_dingdan c 
        on a.order_id = c.id and c.part_day between '{0}' and '{1}'
        left join 
        (
            select userid
            from ads.zhw_shanghu_type_all
            where true 
            and save_date = to_char(current_date - 1,'yyyy-mm-dd')
            group by 1 
        ) d 
        on c.userid = d.userid 
        left join ods_zhw.zhw_fx_sublet_kf e 
        on c.userid = e.userid and e.status = 1
        left join ods_zhw.zhw_user f 
        on c.userid = f.jkx_userid 
        left join ods_zhw.safe_center_lock_target_activity_user h 
        on a.order_id = h.did and h.fk_target_id = {2}
        where a.part_day between '{0}' and '{1}'
        and a.attr_ext::json->>'app_version' in ('6291', '6150', '6300', '6250', '6180', '6310', '6120', '6260', '6270', '6190','6140')
        and a.attr_ext::json->>'app_id_real' in  ('400100000') -- 安卓-普通版
        and to_char(a.create_time,'yyyy-mm-dd hh24:mi:ss')>'2023-07-10 11:12:00'
        and a.type in (3,4)
        and d.userid is null and e.userid is null -- 普通用户
        and h.did is null
    """.format(start_day, end_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)

    if len(report)>50:
        return
    print(report)

    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                      'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']

    report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """2023-07-21，
    group_name 安卓停用版本上号风险用户 63
    7/10 11:12 已对部分安卓版本停用，但该版本后续仍有上号订单，该部分订单无数美记录。高度疑似篡改设备信息。
    """
    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    start_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
    group_id = 63

    try:
        shouyou_fenghao_disable_appversion_group(start_day, end_day, group_id)
    except Exception as e:
        print('shouyou_fenghao_disable_appversion_group 异常信息:\n', e)
    pass