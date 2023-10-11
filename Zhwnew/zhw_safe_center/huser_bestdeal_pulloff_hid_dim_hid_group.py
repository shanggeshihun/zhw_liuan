# _*_coding:utf-8 _*_

# @Time      : 2023/7/6  15:34
# @Author    : An
# @File      : huser_bestdeal_pulloff_hid_dim_hid_group.py
# @Software  : 号主刷红包-下架货架-货架逻辑

import time, datetime
import pandas as pd
import requests,sys
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

def huser_bestdeal_pulloff_hid_dim_hid_group(start_day, end_day, group_id):
    '''
    :param start_day:
    :param end_day:
    :param group_id:
    :return:
    '''
    # 初始化满足规则的结果集
    final_report = pd.DataFrame()

    # 近3天订单量[5,10)，撤单量[0,1]，红包使用金额占订单金额比例80%:下架货架.
    holo_sql = """
        select
            a.hid,a.huserid as userid,
            c.id as uid,
            count(a.id) as orders,
            sum(a.pm) as pm,
            count(case when a.zt = 3 then a.id end) as cd_orders,
            sum(case when a.zt = 3 then a.pm end) as cd_pm,
            count(b.order_id) as hb_orders,
            sum(b.use_money) as hb_use_money
        from ods_zhw.zhw_dingdan a 
        left join ods_zhw.zhw_hongbao_order b 
        on a.id = b.order_id and b.part_day between to_char(current_date -2,'yyyy-mm-dd') and to_char(current_date,'yyyy-mm-dd')
        left join ods_zhw.zhw_user c 
        on a.userid = c.jkx_userid 
        where a.part_day between to_char(current_date -2,'yyyy-mm-dd') and to_char(current_date,'yyyy-mm-dd')
        group by 1,2,3
        having count(a.id) between 5 and 9 
        and count(case when a.zt = 3 then a.id end) between 0 and 1 
        and sum(b.use_money)*1.00/sum(a.pm)>=0.8
    """.format(start_day, end_day, group_id)
    holo_report = pd.read_sql(holo_sql, con=holo_cnx)
    # 无满足刷红包则退出
    if not len(holo_report):
        return final_report

    # 近6小时已命中
    mysql_sql = """    	
        -- 近6小时写入数据
        select 
            hid
        from datawj.safe_center_lock_target_activity_user_info
        where fk_target_id = {0}
        and substring(push_time,1,19) between date_format(date_sub(now(),interval 6 hour),'%%Y-%%m-%%d %%H:%%i:%%S') 
        and date_format(now(),'%%Y-%%m-%%d %%H:%%i:%%S')
        group by 1
    """.format(group_id)
    mysql_report = pd.read_sql(mysql_sql, con=cnx)

    # 1）近6小时已命中的刷红包 为空，则直接将新命中的作为结果数据
    if not len(mysql_report):
        final_report = holo_report.copy()
    # 2）近6小时已命中的不为空，从将新命中的结果中剔除已命中的作为结果数据
    final_report = holo_report[~holo_report.hid.isin(mysql_report.hid)]

    return final_report

def push_risk_huser(punish_type,risk_object):
    data = {
        'punish_type':punish_type,
        'uid':risk_object
    }
    url = 'http://39.98.193.35:9518/report-risk-user'
    json_data = json.dumps(data)
    r = requests.post(url = url,data = json_data)
    return r.status_code


if __name__ == '__main__':

    """2023-07-13，
    group_name 号主刷红包-下架货架-货架逻辑 60
    """
    group_id = 60
    group_id = 999

    push_time = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
    print(push_time,' 号主刷红包-下架货架-货架逻辑 数据推送情况')

    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')
    start_day = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    df_final_report = huser_bestdeal_pulloff_hid_dim_hid_group(start_day, end_day, group_id)

    if not len(df_final_report):
        print('\t无满足规则条件的结果集')
        sys.exit()
    print('\t1)满足规则条件的结果集{}条'.format(len(df_final_report)))
    hid_set = set(df_final_report.hid)
    risk_object = ','.join([str(h) for h in hid_set])
    punish_type = 'off_shelves'
    push_status = push_risk_huser(punish_type,risk_object)
    print('\t2)对满足规则条件的结果集完成推送')


    if push_status==200:
        report = df_final_report.copy()

        report.rename(columns={'uid': 'fk_uid'}, inplace=True)
        report['push_time'] = push_time
        report['fk_target_id'] = group_id
        report['fk_module_type'] = 1
        report['userip'] = '0'
        report['gameid'] = 0
        report['did'] = 0
        report['user_mark'] = 0
        report['push_no'] = 0
        report['deal_flag'] = 0
        report['is_inc'] = 0
        report['deal_flag'] = 2

        report['info'] = report.apply(
            lambda x:
            '{"orders":' + str(x['orders']) +
            ',"pm":' + str(x['pm']) +
            ',"cd_orders":' + str(x['cd_orders']) +
            ',"cd_pm":' + str(x['cd_pm']) +
            ',"hb_orders":' + str(x['hb_orders']) +
            ',"hb_use_money":' + str(x['hb_use_money']) +
            '}',
            axis=1
        )
        result_report = report[[
            'fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                              'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag', 'info', 'hid'
        ]]
        result_report.to_sql(name='safe_center_lock_target_activity_user_info', con=cnx, if_exists='append', index=False)

        print('\t3)将推送数据写入数据表')