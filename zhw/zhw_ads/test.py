# _*_coding:utf-8 _*_

# @Time      : 2022/5/18  15:37
# @Author    : An
# @File      : zhw_msg_recall_new_v2.py
# @Software  : 在 zqy_activate_msg_new 基础上于2022/5/18改版

# 目标表 zqy_duanxin_zhaohui_new @吴辰哲 zqy_duanxin_zhaohui_new'

# noinspection PyUnresolvedReferences
import pymysql
from pyhive import presto
import pandas as pd
# noinspection PyUnresolvedReferences
import numpy as np
# noinspection PyUnresolvedReferences
from sqlalchemy import create_engine
# noinspection PyUnresolvedReferences
import time, datetime
from WorkWeixinRobot.work_weixin_robot import WWXRobot

# 1、连接hive数据库
# 配置项
host_hive = "172.16.13.47"
username_hive = "zhw"
port_hive = 8080
catalog_hive = "hive"
schema_hive = "zhwdb"
# 连接语句
connect_hive = presto.connect(host=host_hive, port=port_hive, username=username_hive, catalog=catalog_hive)

# 2、连接线上运营mysql数据库
# 配置项
host_yy = "rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com"
user_yy = "data_yunying"
password_yy = "yTMgWouzdRfVtmgj"
DB_yy = "data_yunying"
port_yy = "3306"
# 连接语句
connect_yy = create_engine(
    "mysql+pymysql://" + user_yy + ":" + password_yy + "@" + host_yy + ":" + port_yy + "/" + DB_yy, echo=False)
# 案例：cnx = create_engine("mysql+pymysql://"+user+":"+password+"@"+host+":"+port+"/"+DB, echo=False)





"""
                                    红包类型，红包id 发送短信数据统计
"""
# 红包类型，红包id，红包任务
red_packet_type_tuple = (1246, 1248, 1235, 1232, 1237, 1239, 1241, 1243, 1217, 1246, 1256, 1266, 1273, 1258, 1260, 1276)
red_packet_id_tuple = (
1908, 1909, 1910, 1911, 2013, 1916, 1917, 1936, 1937, 1938, 1939, 1940, 1949, 1950, 1855, 1856, 1857, 1858, 1859, 1877,
1878, 1879, 1880, 1896, 1897, 2002, 1598, 1599, 1900, 1659, 2033, 2034, 2035, 2036, 2085, 2086, 2081, 2083, 2139, 2140,
2097, 2098, 2089, 2091, 2143)

sql_task_record = """
    select task_id,red_packet_ids, red_packet_type
    from hive.zhwhigh.zhw_task_record
    where red_packet_ids  is not null 
    and red_packet_type in {}
    group by 1,2,3
""".format(red_packet_type_tuple)
result_task_record = pd.read_sql(sql_task_record, connect_hive)

task_id_, red_packet_type_, red_packet_ids_ = [], [], []
for idx, item in result_task_record.iterrows():
    task_id = item['task_id']
    red_packet_type = item['red_packet_type']
    red_packet_ids = [int(a) for a in item['red_packet_ids'].split(',')]

    tmp_len = len(red_packet_ids)

    task_id_.extend([task_id] * tmp_len)
    red_packet_type_.extend([red_packet_type] * tmp_len)
    red_packet_ids_.extend(red_packet_ids)

df_task_record = pd.DataFrame(
    {'task_id_': task_id_, 'red_packet_type_': red_packet_type_, 'red_packet_ids_': red_packet_ids_})
task_id_tuple = tuple(task_id_)
df_task_record.to_excel(r"./tmp.xlsx")
df_task_record_2 = df_task_record[df_task_record.red_packet_ids_.isin(red_packet_id_tuple)]

# 红包任务下的短信发送记录
sql_send_sms = """
    select format_datetime(create_time,'yyyy-MM-dd') as create_dt,task_id,count(*) as send_sms_count
    from hive.zhwhigh.zhw_log_send_sms
    where task_id in {}
    and format_datetime(create_time,'yyyy-MM')>='2022-03'
    group by 1,2
""".format(task_id_tuple)
result_send_sms = pd.read_sql(sql_send_sms, connect_hive)

df_ = pd.merge(result_send_sms, df_task_record_2, left_on=['task_id'], right_on=['task_id_'])
df_.to_sql(name='red_packet_type_id_msg', con=connect_yy, if_exists='replace', index=False)
