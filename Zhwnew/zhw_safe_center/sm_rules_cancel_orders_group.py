# _*_coding:utf-8 _*_

# @Time      : 2023/3/24  17:30
# @Author    : An
# @File      : shouyou_fenghao_match_for_sh_group.py
# @Software  : PyCharm

# 20230406 1836 按照检测时间推送

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


# ------------------------参数配置----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1


def sm_rules_cancel_orders_group(day_last_1, now_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:针对手游查询到的封号匹配出对应封号订单，针对商户订单 封禁上号OAID
    '''
    sql = """    	
            -- root设备|是否hook设备|PC模拟器|远程操控工具|手机模拟器|篡改地理位置(20230407)
          
            select                         
            	{2} as fk_target_id,
            	1 as fk_module_type,
            	t2.id as fk_uid,
            	t1.userid,
            	'' as userip,
            	t1.gameid as gameid,
            	t1.did as did,
            	0 as user_mark,
            	0 as push_no,
            	cast (now() as timestamp)  as push_time,
            	0 as is_inc,
            	0 as deal_flag 
            from 
            (
            	select userid,did,gameid
            	from ods_zhw.zhw_ts  
            	where true 
            	and part_day between to_char(date('{0}') - interval '1 days','yyyy-mm-dd') and '{1}'
            	and lx = '上号器自动投诉（风险设备）'
            	and re = '风险设备触发撤单'
                and t >= (current_timestamp - interval '25 minutes') 
            ) t1
            left join ods_zhw.zhw_user t2
            on t1.userid = t2.jkx_userid 
            left join
            (
                 select did from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2} group by 1
            ) t4
            on t1.did=t4.did
            where t4.did is null
    """.format(day_last_1, now_day, group_id)
#   测试SQL代码：print(sql)
    report = pd.read_sql(sql, con=holo_cnx)

    print(report)
    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                      'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """2023-04-12，根据数美规则（root设备|是否hook设备|PC模拟器|远程操控工具|手机模拟器|篡改地理位置）
    命中标记用户，结合针对符合规则的用户进行撤单操作，添加该类型的用户群至safe_center_lock_target_activity_user表中，并封禁该用户群的oaid
    """
    try:
        sm_rules_cancel_orders_group(day_last_1, now_day, 49)
    except Exception as e:
        print('sm_rules_cancel_orders_group 异常信息:\n', e)
    pass



