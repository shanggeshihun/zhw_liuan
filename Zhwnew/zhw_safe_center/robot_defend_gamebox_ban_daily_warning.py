# _*_coding:utf-8 _*_

# @Time      : 2023/7/28  10:46
# @Author    : An
# @File      : robot_defend_gamebox_ban_daily_warning.py
# @Software  : PyCharm

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
# hologres-gamebox数据库
holo_dfbox_host = cf.get("Holo_dfbox_defend_r", "host")
holo_dfbox_port = cf.get("Holo_dfbox_defend_r", "port")
holo_dfbox_database = cf.get("Holo_dfbox_defend_r", "DB")
holo_dfbox_user = cf.get("Holo_dfbox_defend_r", "user")
holo_dfbox_password = cf.get("Holo_dfbox_defend_r", "password")
holo_dfbox_cnx = create_engine("postgresql+psycopg2://"+holo_dfbox_user+":"+holo_dfbox_password+"@"+holo_dfbox_host+":"+holo_dfbox_port+"/" + holo_dfbox_database )

'''
******** 基础函数定义 ********
'''
def calc_tb_hb(type='ratio',numeric=0,compare_numeric=0):
    '''
    :param type: 计算同环比时，对比数据的类型，数值or比例
    :param numeric:当前数据
    :param compare_numeric:对比期数据
    :return:同环比结果
    '''
    if type == 'value':
        if compare_numeric == 0:
            if numeric>0:
                return 1
            else:
                return 0
        else:
            return numeric/compare_numeric - 1
    else:
        if compare_numeric == 0:
            return numeric
        else:
            return numeric - compare_numeric

def float_to_percent_nosign(float_numeric):
    return [str(round(float_numeric * 100.000, 2)) if float_numeric > 0 else str(round(float_numeric * 100.000 * (-1), 2))][0] + "%"

def mapping_fontcolor(numeric):
    """
    :param numeric: 数值
    :return: 大于0 绿色，小于0 橙色，等于0 灰色
    """
    if numeric > 0:
        return "warning"
    elif numeric < 0:
        return "info"
    else:
        return "comment"

def mapping_updown(numeric):
    """
    :param values: 数值
    :return: 大于0 上升，小于0 下降，等于0 持平
    """
    if numeric > 0:
        return "上升"
    elif numeric < 0:
        return "下降"
    else:
        return "持平"

def null_series_default(pd_series):
    if len(pd_series) == 0:
        return pd.Series([0,0])
    else:
        return pd_series

def work_wxrobot(content, webhook):
    """
    :param content:
    :param webhook:
    :return:
    """
    headers = {"Content-Type": "application/json"}
    form_data = {
        "msgtype": "markdown",
        "markdown": {
            "content": content,
            "mentioned_list": ["liuan@jld1141.wecom.work"]
        }
    }

    form_data_json = json.dumps(form_data)
    work_wxrobot_result = requests.post(url=webhook, data=form_data_json, headers=headers, verify=False)
    return work_wxrobot_result

_now = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%H:%M')
_current_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y%m%d')


# 数据窗口日期
start_day = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
end_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

part_day_h = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
part_day_t = (datetime.datetime.now() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')


# 刀锋盒子封号数-日期
gamebox_ban_dim_day = '''
    select 
        d.part_day,
        d.zhs as bans,coalesce(h.zhs,0) as bans_h,coalesce(t.zhs,0) as bans_t
    from 
    (	
        select date(a.update_time) as part_day,
            count(distinct a.account) as zhs
        from ods.game_account a
        left join ods.game_shelves b on a.good_stock_id = b.good_stock_id
        left join ods.game_info c on b.game_info_id = c.id
        where un_shelve_reason in ('ACCOUNT_PERMANENT_BANNED','ACCOUNT_TEMP_BANNED') and is_deleted <> 1
        and b.game_info_id in (9,11,120,136,506,518,538,695)
        and a.update_time>=current_date - 1 and a.update_time<current_timestamp
        group by 1
    ) d 
    left join 
    (
        select date(a.update_time) as part_day,
            count(distinct a.account) as zhs
        from ods.game_account a
        left join ods.game_shelves b on a.good_stock_id = b.good_stock_id
        left join ods.game_info c on b.game_info_id = c.id
        where un_shelve_reason in ('ACCOUNT_PERMANENT_BANNED','ACCOUNT_TEMP_BANNED') and is_deleted <> 1
        and b.game_info_id in (9,11,120,136,506,518,538,695)
        and a.update_time>=current_date - 2 and a.update_time<current_timestamp
        group by 1
    ) h
    on d.part_day = h.part_day + 1
    left join 
    (
        select date(a.update_time) as part_day,
            count(distinct a.account) as zhs
        from ods.game_account a
        left join ods.game_shelves b on a.good_stock_id = b.good_stock_id
        left join ods.game_info c on b.game_info_id = c.id
        where un_shelve_reason in ('ACCOUNT_PERMANENT_BANNED','ACCOUNT_TEMP_BANNED') and is_deleted <> 1
        and b.game_info_id in (9,11,120,136,506,518,538,695)
        and a.update_time>=current_date - 8 and a.update_time<current_timestamp
        group by 1
    ) t
    on d.part_day = h.part_day + 7
    where true 
    and ((h.zhs>0 and d.zhs>h.zhs*1.5) or
        (t.zhs>0 and d.zhs>t.zhs*1.5) or 
        (h.zhs = 0 and d.zhs >=4) or 
        (t.zhs = 0 and d.zhs >=4)
    )
    and d.part_day = current_date
    order by 1 desc 
'''.format(start_day,end_day)
tmp_df = pd.read_sql(gamebox_ban_dim_day, con=holo_dfbox_cnx)
print(tmp_df)

title = '# ** {}刀锋盒子-封号数量 预警 **({})'.format(_current_day,_now)

part1_title = '# ** {}刀锋盒子-封号数量 预警 **({})'.format(_current_day,_now)

if not len(tmp_df):
    sys.exit()
else:
    for idx,item in tmp_df.iterrows():
        tmp_part_day = item['part_day']
        tmp_bans = item['bans_h']
        tmp_bans_h = item['bans_h']
        tmp_bans_t = item['bans_t']

part1_subcontent1 = ''
part1_subcontent1 ='{}\t日期 {}、当日封号数 {}、环期封号数 {}、同期封号数{}\n'.format(
    part1_subcontent1,
    tmp_part_day,
    tmp_bans,
    tmp_bans_h,
    tmp_bans_t
)

test_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=231ae38a-3d31-4635-80d2-800029963832"
# 刀锋盒子反外挂群播报
prod_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=c73a13ec-e747-463a-98c4-236374d6bb79"

result_content1 = part1_title + '\n' + '>' + part1_subcontent1
work_wxrobot(result_content1,prod_webhook)
