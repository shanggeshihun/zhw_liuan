# _*_coding:utf-8 _*_

# @Time      : 2023/4/21  9:40
# @Author    : An
# @File      : shouyou_fengjin_match_group_tmp.py
# @Software  : PyCharm


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

holo_host = cf.get("Hologres", "host")
holo_user = cf.get("Hologres", "user")
holo_password = cf.get("Hologres", "password")
holo_DB = cf.get("Hologres", "db")
holo_port = cf.get("Hologres", "port")
holo_cnx = create_engine(
    "postgresql+psycopg2://" + holo_user + ":" + holo_password + "@" + holo_host + ":" + holo_port + "/" + holo_DB)

wj_host = cf.get("Mysql-sjwj", "host")
wj_user = cf.get("Mysql-sjwj", "user")
wj_password = cf.get("Mysql-sjwj", "password")
wj_DB = cf.get("Mysql-sjwj", "DB")
wj_port = cf.get("Mysql-sjwj", "port")
cnx = create_engine("mysql+pymysql://" + wj_user + ":" + wj_password + "@" + wj_host + ":" + wj_port + "/" + wj_DB,
                    echo=False)

wj_host = cf.get("Mysql-Database-test", "host")
wj_user = cf.get("Mysql-Database-test", "user")
wj_password = cf.get("Mysql-Database-test", "password")
wj_DB = cf.get("Mysql-Database-test", "DB")
# wj_port = cf.get("Mysql-Database-test","port")
test_cnx = create_engine("mysql+pymysql://" + wj_user + ":" + wj_password + "@" + wj_host + ":" + wj_port + "/" + wj_DB,
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


# ----------------------配置信息-------------------------
# mysql -h rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com -u datawj -P 3306 -p
# 密码：oKSq77hSJKMX825GFL

# insert into safe_center_lock_target_activity_manage (place_type_more,place_type,group_code,group_name,group_statue_code,abtest_scale)values('1',1,'pc_game_cancel_order_user_group_3','pc游戏撤单率高导致封号用户群（关闭3天）',1,0);


def shouyou_waigua_xuniphone_group(start_day, end_day, group_id):
    sql = """
        select
            {2} as fk_target_id,
            1 as fk_module_type,
            t2.id as fk_uid,
            t1.userid,
            ' ' as userip,
            11 as gameid,
            0 as did,
            0 as user_mark,
            0 as push_no,
            cast (now() as timestamp)  as push_time,
            0 as is_inc,
            0 as deal_flag 
            from
        (
            select a.jkx_userid as userid
            from ods_zhw.zhw_app_hao_log a 
            left join ods_zhw.zhw_user b on a.jkx_userid =b.jkx_userid 
            where a.part_day between '{0}' and '{1}'
            and (jkx_userphone like '1700%%' or jkx_userphone like '1701%%' or jkx_userphone like '1702%%' or jkx_userphone like '162%%' or jkx_userphone like '1703%%' or jkx_userphone like '1705%%' or jkx_userphone like '1706%%' or jkx_userphone like '165%%' or jkx_userphone like '1704%%' or jkx_userphone like '1707%%' or jkx_userphone like '1708%%' or jkx_userphone like '1709%%' or jkx_userphone like '171%%' or jkx_userphone like '167%%' or jkx_userphone like '1349%%' or jkx_userphone like '174%%' or jkx_userphone like '140%%' or jkx_userphone like '141%%' or jkx_userphone like '144%%' or jkx_userphone like '146%%' or jkx_userphone like '148%%')
            group by 1
        ) t1
        left join
        (
            select id ,jkx_userid from ods_zhw.zhw_user
        ) t2
        on t1.userid=t2.jkx_userid
        left join
        (
            select userid from public.zhw_shanghu_type_all where save_date = to_char(current_date - interval '1 day','yyyy-mm-dd')
        ) t3
        on t1.userid=t3.userid
        left join
        (
            select * from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2}
        ) t4
        on t1.userid=t4.userid
        left join
        (
            select  userid from ods_zhw.zhw_fx_sublet_kf where STATUS=1
        ) t5
        on t1.userid=t5.userid
        where t3.userid is null
        and t4.userid is null
        and t5.userid is null
    """.format(start_day, end_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)
    print('结果数量:',len(report))
    print('返回结果如下:\n',report)
    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did', 'user_mark',
                      'push_no', 'push_time', 'is_inc', 'deal_flag']
    report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)


if __name__ == '__main__':
    """
    手游上报外挂的虚拟号段用户（封禁永久） 29
    """
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')
    start_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    """2022-08-09，手游上报外挂的虚拟号段用户（封禁永久） 29"""
    try:
        shouyou_waigua_xuniphone_group(start_day, end_day, 29)
    except Exception as e:
        print(e)
        pass