# _*_coding:utf-8 _*_

# @Time      : 2023/10/30  10:26
# @Author    : An
# @File      : tmp_cb_multi_dims_retention_1di.py
# @Software  : 支付成功R日拒付

import time, datetime, configparser, warnings, math, platform
import sys

import numpy as np
import pandas as pd
import pymysql, presto
# import pypresto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
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

from OperateDoris import OperateDoris

class TmpCsvToTmpTmpAllAlerts:
    def __init__(self):
        warnings.filterwarnings("ignore")
        # ------------------------数据库配置读取----------------------------
        cf = configparser.ConfigParser()
        if cf.read("E:/Onerway/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
            """服务器模式"""
            cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
        else:
            """本地模式"""
            cf.read("E:/Onerway/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

        self.final_result_log = {}

        self.doris_host = cf.get("prod_doris", "host")
        self.doris_user = cf.get("prod_doris", "user")
        self.doris_password = cf.get("prod_doris", "password")
        self.doris_db = cf.get("prod_doris", "DB")
        self.doris_port = cf.get("prod_doris", "port")

    def run(self, start_day ,end_day):
        """
        :param start_day: 起始日期参数
        :param end_day: 终止日期参数
        :return: 抽奖主题：抽奖日期+用户维度  指标数据 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_doris = OperateDoris(
            username=self.doris_user,
            password=self.doris_password,
            host_ip=self.doris_host,
            port=int(self.doris_port),
            database=self.doris_db
        )

        df = pd.read_csv(r"C:\Users\zimo1\Desktop\all_alerts.csv", encoding='utf-8')
        df2 = df.head(3)

        data = df.values
        data[pd.isna(data)] = None
        data_list=data.tolist()

        # 目标表 数据写入
        column_concat = 'origion_id, process_date, chageback_date, chargeback_code, chargeback_arn, origin_txn_id, channel_id, chargeback_reason, notify_source, merchant_no, txn_date, external_order_id, txn_amount, refund_amount, txn_currency, note1, note2, ethoca_id, card_type, email, website'
        insert_sql = "insert into tmp.tmp_tmp_all_alerts({0}) values ({1});".format(
            column_concat,
            ','.join(['%s'] * len(column_concat.split(',')))
        )
        operate_doris.insert_data(insert_sql, data_list)

        operate_doris_log = {}
        for k, v in operate_doris.operate_result.items():
            if v[0] != '无':
                operate_doris_log[k] = v

        operate_doris.close_conn()

        # 数据日志记录
        self.final_result_log['operate_doris_log'] = operate_doris_log
        self.final_result_log['operate_doris'] = operate_doris_log
        print(self.final_result_log)


if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    start_end_day_list = [
        ('2022-01-01', '2022-06-30')
    ]
    for idx, start_end_tuple in enumerate(start_end_day_list):
        start_time = time.time()

        start_day, end_day = start_end_tuple[0], start_end_tuple[1]

        print('执行时间', (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), start_end_tuple)

        start_time = time.time()
        tmp_csv_to_tmp_tmp_all_alerts = TmpCsvToTmpTmpAllAlerts()
        tmp_csv_to_tmp_tmp_all_alerts.run(start_day, end_day)
        end_time = time.time()

        time.sleep(1)
        print('运行耗时：', end_time - start_time)