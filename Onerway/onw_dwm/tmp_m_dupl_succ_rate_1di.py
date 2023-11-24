# _*_coding:utf-8 _*_

# @Time      : 2023/10/30  10:26
# @Author    : An
# @File      : tmp_m_dupl_succ_rate_1di.py
# @Software  : 支付成功率(去重)

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

class TmpMDuplSuccRate1di:
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


        # 清理目标表数据
        delete_sql = "delete from tmp.tmp_m_dupl_succ_rate_1di where part_day between '{0}' and '{1}'".format(start_day ,end_day)
        operate_doris.delete_data(delete_sql)

        # 投放效果初始表
        query_sql = """
            select 
                date_format(txn_date, '%Y-%m-%d') as part_day,
                'V3' as version,
                merchant_no,
                count(case when status = 'S' then id end) as pay_succ_orders,
                count(case when status = 'F' then id end) as pay_dupl_fail_orders
            from 
            (
                select id,txn_date,email,status,merchant_no,txn_currency,txn_amount,card_bin_country,card_type,website,payer_payment_type,
                row_number() over(partition by merchant_no, date_format(txn_date, '%Y-%m-%d'), email order by status desc, txn_date desc) rn
                from ods.ods_v3db_acq_t_acq_gw_txn_log_ri
                where txn_date>=cast('{0} 00:00:00' as datetime)
                and txn_date<=cast('{1} 23:59:59' as datetime)
                and status in ('S','F')
                and `txn_type` in ('0006','0008','0009') -- 20231031更改：限定交易类型取值范围：Sale/Auth/preAuth 
                and (source_channel <> 'shopify_plugin' or source_channel is null) -- 剔除v1
                and channel_id is not null
            ) t1
            where (status = 'S' or rn = 1)
            group by 1,2,3
        
            union all 
            
            select 
                date_format(TRADETIME, '%Y-%m-%d') as part_day,
                'V1' as version,
                MER_NO as merchant_no,
                count(case when SUBSTR(TRADESTATE, 1, 1) in ('1','4') then id end) as pay_succ_orders,
                count(case when SUBSTR(TRADESTATE, 1, 1) = '0' then id end) as pay_dupl_fail_orders
            from
            (
                SELECT 
                    T1.ID,TRADETIME,EMAIL,TRADESTATE,MER_NO,ORDERNO,CARDSCHEME,TRADEURL,C.BINCOUNTRY, T1.SHOWCURRENCY,TRADEAMOUNT,
                    ROW_NUMBER() OVER(PARTITION BY MER_NO, date_format(TRADETIME, '%Y-%m-%d'), EMAIL ORDER BY SUBSTR(TRADESTATE, 1, 1) DESC, TRADETIME DESC) RN
                FROM ods.ods_v1pacypay_INTERNATIONAL_TRADEINFO_ri T1  
                LEFT JOIN ods.ods_v1pacypay_INTERNATIONAL_CARDHOLDERSINFO_ri C
                ON T1.ID = C.TRADEID
                WHERE TRADETIME >=cast('{0} 00:00:00' as datetime)
                and TRADETIME<=cast('{1} 23:59:59' as datetime)
                AND TRADESTATE regexp '^[0|1|4]' -- TRADESTATE 以0或1或4开头
                AND (TRANSACTION_STATUS IN (3, 7, 8, 9, 10) OR TRANSACTION_STATUS IS NULL)
            ) a
            where  (TRADESTATE LIKE '1%' OR RN = 1)
            group by 1,2,3    
        
        """.format(start_day, end_day)
        doris_data_list = operate_doris.query_data(query_sql)

        df = pd.DataFrame()
        if doris_data_list:
            columns = operate_doris.query_data_index()
            init_df = pd.DataFrame(doris_data_list)
            init_df.columns = columns
        else:
            init_df = pd.DataFrame()

        # 目标表 数据写入
        column_concat = 'part_day ,version ,merchant_no ,pay_succ_orders ,pay_dupl_fail_orders'
        insert_sql = "insert into tmp.tmp_m_dupl_succ_rate_1di({0}) values ({1});".format(
            column_concat,
            ','.join(['%s'] * len(column_concat.split(',')))
        )
        operate_doris.insert_data(insert_sql, doris_data_list)

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
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('operate_doris')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    start_end_day_list = [
        ('2022-01-01', '2022-06-30'),
        ('2022-07-01', '2022-12-31'),
        ('2023-01-01', '2023-06-30'),
        ('2023-07-01', '2023-12-31')
    ]

    for idx, start_end_tuple in enumerate(start_end_day_list):
        start_time = time.time()

        start_day, end_day = start_end_tuple[0], start_end_tuple[1]

        exe_time = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

        start_time = time.time()
        tmp_m_dupl_succ_rate_1di = TmpMDuplSuccRate1di()
        tmp_m_dupl_succ_rate_1di.run(start_day, end_day)
        end_time = time.time()

        print('日期窗口:', start_end_tuple, '\t开始执行时间:', exe_time, '\t运行耗时:', end_time - start_time)