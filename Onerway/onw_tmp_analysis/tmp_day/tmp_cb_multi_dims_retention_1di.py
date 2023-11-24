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

class TmpCbMultiDimsRetention1di:
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
        delete_sql = "delete from tmp.tmp_cb_multi_dims_retention_1di where pay_succ_day between '{0}' and '{1}'".format(start_day ,end_day)
        operate_doris.delete_data(delete_sql)

        # 投放效果初始表
        query_sql = """
            with tmp_chargeback as (
                -- V3成功支付订单的第N日拒付订单(添加支付成功金额，拒付金额)
                select 
                    merchant_no,
                    date_format(create_time,'%Y-%m-%d') as pay_succ_day,
                    payer_payment_type,card_type,channel,bin_country,mcc,website_type,docking_mode,
                    case when chargeback_date is not null then datediff(chargeback_date,create_time) else 99999999 end as gap_days,
                    count(id) as pay_succ_orders,
                    sum(pay_succ_money) as pay_succ_money,
                    count(chargeback_origin_txn_id) as chargeback_orders,
                    sum(chargeback_money) as chargeback_money
                from 
                (
                    select 
                        a.merchant_no,
                        coalesce(a.payer_payment_type,'other') as payer_payment_type,
                        coalesce(a.card_type,'other') as card_type,
                        coalesce(case 
                            when a.institution='SAFE_CHARGE' then 'SC' 
                            when a.institution not regexp '-' then upper(a.institution) 
                            else upper(split_part(a.institution,'-',1)) 
                        end,'other') as channel,
                        coalesce(case when a.card_bin_country is null then 'other' else a.card_bin_country end,'other') as  bin_country,
                        coalesce(dict.description,'other') as mcc,
                        coalesce(dict2.description,'other') as website_type,
                        coalesce(dict3.description,'other') as docking_mode,
                        a.create_time,
                        a.id,
                        b.chargeback_date,
                        max(a.order_amount * r.mid_rate) as pay_succ_money,
                        max(b.origin_txn_id) as chargeback_origin_txn_id,
                        max(b.chargeback_amount * r2.mid_rate) as chargeback_money
                    from ods.ods_v3db_acq_t_acq_gw_txn_log_ri a
                    left join dim.dim_date_rate r 
                    on a.txn_currency = r.target_currency and DATE_FORMAT(a.create_time, '%Y-%m-%d') = r.effect_date
                    left join ods.ods_v3db_acq_t_acq_chargeback_ri b 
                    on a.id = b.origin_txn_id 
                    and b.chargeback_status <> 'DELETE'
                    and b.chargeback_date>=date('{0}')
                    and b.chargeback_date<=date_add('{1} 23:59:59', interval 365 day) -- 考虑180日以内的拒付
                    left join dim.dim_date_rate r2 
                    on b.chargeback_currency = r2.target_currency and DATE_FORMAT(a.create_time, '%Y-%m-%d') = r2.effect_date
                    left join ods.ods_v3db_spt_t_spt_user_app_ri app 
                    on a.app_id = app.id 
                    left join ods.ods_v3db_spt_t_spt_base_dict_ri dict 
                    on app.mcc = dict.value and dict.code = 'MCC'
                    left join ods.ods_v3db_spt_t_spt_base_dict_ri dict2 
                    on app.website_type = dict2.value and dict2.code = 'WEB_TYPE'
                    left join ods.ods_v3db_spt_t_spt_base_dict_ri dict3
                    on app.docking_mode = dict3.value and dict3.code = 'DOCKIGN_MODE'
                    where a.txn_type in ('0006','0008','0009') -- 两方接口-直接支付,预授权,预授权
                    and a.status in ('S') -- 成功
                    and a.create_time>=cast('{0} 00:00:00' as datetime)
                    and a.create_time<=cast('{1} 23:59:59' as datetime)
                    -- and (a.institution not in ('Alipay','WeChat') or a.institution is null)-- 排除聚合交易，非收单业务
                    and (a.source_channel <> 'shopify_plugin' or a.source_channel is null) -- 排除V1交易
                    -- and a.merchant_no in (601246 ,600788 ,601296 ,601203 ,601434 ,601411 ,601478 ,601341 ,601062 ,800069 ,800111 ,600860 ,300293 ,601248 ,601455 ,800060 ,600422 ,601428 ,601352 ,600859 ,800087 ,800104 ,800068 ,800013 ,600842 ,600736 ,600817 ,601118)
                    group by 1,2,3,4,5,6,7,8,9,10,11
                ) a 
                group by 1,2,3,4,5,6,7,8,9,10
            ) 
            select merchant_no ,pay_succ_day ,payer_payment_type ,card_type ,channel ,bin_country ,mcc,website_type ,docking_mode , gap_days ,pay_succ_orders ,pay_succ_money ,chargeback_orders ,chargeback_money
            from tmp_chargeback
        """.format(start_day ,end_day)
        doris_data_list = operate_doris.query_data(query_sql)

        df = pd.DataFrame()
        if doris_data_list:
            columns = operate_doris.query_data_index()
            init_df = pd.DataFrame(doris_data_list)
            init_df.columns = columns
        else:
            init_df = pd.DataFrame()

        # 目标表 数据写入
        column_concat = 'merchant_no ,pay_succ_day ,payer_payment_type ,card_type ,channel ,bin_country ,mcc ,website_type, docking_mode, gap_days ,pay_succ_orders ,pay_succ_money ,chargeback_orders ,chargeback_money'
        insert_sql = "insert into tmp.tmp_cb_multi_dims_retention_1di({0}) values ({1});".format(
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

        print('执行时间', (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), start_end_tuple)

        start_time = time.time()
        tmp_cb_multi_dims_retention_1di = TmpCbMultiDimsRetention1di()
        tmp_cb_multi_dims_retention_1di.run(start_day, end_day)
        end_time = time.time()

        time.sleep(1)
        print('运行耗时：', end_time - start_time)