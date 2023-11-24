# _*_coding:utf-8 _*_

# @Time      : 2023/10/30  10:26
# @Author    : An
# @File      : tmp_dws_iccmm_chargeback_1di.py
# @Software  : 收单拒付宽表-拒付日期

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

class TmpDwsIccmmChargeback1di:
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
        delete_sql = "delete from tmp.tmp_dws_iccmm_chargeback_1di where chargeback_day between '{0}' and '{1}'".format(start_day, end_day)
        operate_doris.delete_data(delete_sql)

        query_sql = """
            select 
                'V3' as version,
                date_format(c.chargeback_date,'%Y-%m-%d') as chargeback_day,
                coalesce(t.institution,'other') as institution,
                coalesce(t.card_type,'other') as card_type,
                coalesce(
                    case 
                        when t.institution='SAFE_CHARGE' then 'SC' 
                        when t.institution not regexp '-' then upper(t.institution) 
                        else upper(split_part(t.institution,'-',1)) 
                    end,'other'
                ) as channel,
                coalesce(t.channel_name,'other') as mid,
                coalesce(t.merchant_no,'other') as merchant_no,
                count(distinct c.origin_txn_id) as chargeback_ordres,
                count(distinct c.id) as chargeback_times,
                sum(c.chargeback_amount * r.mid_rate) as chargeback_money,
                count(distinct case when c.chargeback_source_type = '1' then c.origin_txn_id end) as chargeback_warning_ordres,
                sum(case when c.chargeback_source_type = '1' then c.chargeback_amount * r.mid_rate end) as chargeback_warning_money,
                count(distinct case when c.chargeback_code in ('10','10.1','10.4','4540','4837','4840','4849','C42','C54','D70','C41') then c.origin_txn_id end) as fraud_ordres,
                sum(case when c.chargeback_code in ('10','10.1','10.4','4540','4837','4840','4849','C42','C54','D70','C41') then c.chargeback_amount * r.mid_rate end) fraud_money
            from ods.ods_v3db_acq_t_acq_chargeback_ri c 
            join ods.ods_v3db_acq_t_acq_gw_txn_log_ri t
            on c.origin_txn_id = t.id
            and t.txn_type in ('0006','0008','0009') -- 两方接口-直接支付,预授权,预授权
            and (t.source_channel <> 'shopify_plugin' or t.source_channel is null)-- 排除V1交易
            and t.status in ('S','F') -- 限定交易状态：S成功，F失败
            and t.txn_date>=cast(date_sub('{0}',interval 365 day) as datetime) -- 拒付日期前1年
            and t.txn_date<=cast('{1}' as datetime) 
            left join dim.dim_date_rate r on t.order_currency = r.target_currency and date_format(t.create_time, '%Y-%m-%d') = r.effect_date
            where c.chargeback_date>=date('{0}') 
            and c.chargeback_date<=date('{1}') -- 拒付日期
            group by 1,2,3,4,5,6,7
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
        column_concat = 'version, chargeback_day, institution, card_type, channel, MID, merchant_no, chargeback_ordres, chargeback_times, chargeback_money, chargeback_warning_ordres, chargeback_warning_money, fraud_ordres, fraud_money'
        insert_sql = "insert into tmp.tmp_dws_iccmm_chargeback_1di({0}) values ({1});".format(
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
        tmp_dws_iccmm_chargeback_1di = TmpDwsIccmmChargeback1di()
        tmp_dws_iccmm_chargeback_1di.run(start_day, end_day)
        end_time = time.time()

        print('日期窗口:', start_end_tuple, '\t开始执行时间:', exe_time, '\t运行耗时:', end_time - start_time)