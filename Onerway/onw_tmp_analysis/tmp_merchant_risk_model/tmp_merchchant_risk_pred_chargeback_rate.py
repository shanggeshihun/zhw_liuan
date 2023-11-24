# _*_coding:utf-8 _*_

# @Time      : 2023/10/30  10:26
# @Author    : An
# @File      : .py
# @Software  : 商户风险暴露值-新商户初始化-拒付

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

warnings.filterwarnings("ignore")
# ------------------------数据库配置读取----------------------------
cf = configparser.ConfigParser()
if cf.read("E:/Onerway/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("E:/Onerway/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

doris_host = cf.get("prod_doris", "host")
doris_user = cf.get("prod_doris", "user")
doris_password = cf.get("prod_doris", "password")
doris_db = cf.get("prod_doris", "DB")
doris_port = cf.get("prod_doris", "port")

doris_con = create_engine(
    "mysql+pymysql://" + doris_user + ":" + doris_password + "@" + doris_host + ":" + doris_port + "/" + doris_db,
    echo=False
    )

merch_sql = '''
    select a.* 
    from tmp.tmp_tmp_i_new_merchant_cases a 
    left join 
    (
        select merchant_no 
        from tmp.tmp_m_dupl_succ_rate_1di
        where version = 'V1'
        group by 1
    ) b  
    on a.merchant_no = b.merchant_no
    where b.merchant_no is null
    -- and a.merchant_no = '800319'
'''
df_merch = pd.read_sql(merch_sql,con=doris_con)
df_merch['min_part_day'] = pd.to_datetime(df_merch.min_part_day)
df_merch['max_part_day'] = pd.to_datetime(df_merch.max_part_day)
df_merch['min_part_day_add60'] = df_merch.min_part_day + pd.Timedelta(days=60)
df_merch['min_part_day_add90'] = df_merch.min_part_day + pd.Timedelta(days=90)
df_merch['min_part_day_sub60'] = df_merch.min_part_day - pd.Timedelta(days=60)
df_merch['min_part_day_sub90'] = df_merch.min_part_day - pd.Timedelta(days=90)
df_merch['min_part_day_sub240'] = df_merch.min_part_day - pd.Timedelta(days=240)

merch_concat = ','.join(["'" + m + "'" for m in df_merch.merchant_no])


df_pred_chargeback_rate = pd.DataFrame()

for idx, item in df_merch.iterrows():
    tmp_merch = item['merchant_no']
    min_part_day = item['min_part_day'].strftime('%Y-%m-%d')
    min_part_day_add60 = item['min_part_day_add60'].strftime('%Y-%m-%d')
    min_part_day_sub60 = item['min_part_day_sub60'].strftime('%Y-%m-%d')
    min_part_day_sub240 = item['min_part_day_sub240'].strftime('%Y-%m-%d')
    min_part_day_sub90 = item['min_part_day_sub90'].strftime('%Y-%m-%d')

    # 获取新商户的初始化信息
    tmp_merch_sql = '''
        select
            a.merchant_no,a.card_type,a.channel,a.bin_country,a.mcc,
            sum(a.pay_succ_orders) as pay_succ_orders,
            sum(a.pay_succ_money) as pay_succ_money,
            sum(a.chargeback_orders) as chargeback_orders,
            round(sum(a.chargeback_orders)/sum(a.pay_succ_orders),2) as rate,
            round(sum(a.pay_succ_money)*1.0/sum(a.pay_succ_orders),1) as unit_price
        from tmp.tmp_cb_multi_dims_retention_1di a 
        where true
        and a.merchant_no = '{0}'
        and a.pay_succ_day>='{1}'
        and a.pay_succ_day<='{2}'
        group by 1,2,3,4,5
        having sum(a.pay_succ_orders)>10 and sum(a.chargeback_orders)*1.0/sum(a.pay_succ_orders)>0.001
        order by 1,5,6 desc
    '''.format(tmp_merch, min_part_day, min_part_day_add60)

    df_merch_init = pd.read_sql(tmp_merch_sql,con=doris_con)
    # 如果没有满足条件的商户则循环下一个商户
    if not len(df_merch_init):
        print('未获取初始化信息：', tmp_merch, min_part_day, min_part_day_add60)
        continue
    df_merch_init['attr_concat'] = df_merch_init.apply(
        lambda row: "{}-{}-{}-{}".format(row['card_type'], row['channel'], row['bin_country'], row['mcc']), axis=1
    )
    tmp_merch_attr_concat = ','.join(['"' + m + '"' for m in df_merch_init.attr_concat])
    unit_price_weigth = df_merch_init.pay_succ_money.sum(axis=0) / df_merch_init.pay_succ_orders.sum(axis=0)
    unit_price_lower, unit_price_upper = unit_price_weigth*(1-0.5), unit_price_weigth*(1+0.5)


    # 通过老商户计算新商户的预测拒付率
    tmp_old_merch_sql = '''
        select *,
        round(all_succ_money/all_succ_orders,2) as unit_price,round(accum_chargeback_orders/all_succ_orders,3) as accum_chargeback_rate
        from 
        (
            -- 老商户 下钻维度的R日累积退款率与R日累积拒付率
            select merchant_no,gap_days,pay_succ_orders,pay_succ_money,chargeback_orders,
            sum(pay_succ_orders)over(partition by merchant_no) as all_succ_orders,
            sum(pay_succ_money)over(partition by merchant_no) as all_succ_money,
            sum(chargeback_orders)over(partition by merchant_no order by gap_days asc) as accum_chargeback_orders
            from 
            (
                select merchant_no,gap_days,
                sum(pay_succ_orders) as pay_succ_orders,
                sum(pay_succ_money) as pay_succ_money,
                sum(chargeback_orders) as chargeback_orders
                from tmp.tmp_cb_multi_dims_retention_1di 
                where true
                and merchant_no not in ({0})
                and pay_succ_day between '{1}' and '{2}' -- 保证老商户截至0701有3个月的拒付窗口
                and concat(card_type,'-',channel,'-',bin_country,'-',mcc) in (
                    {3}
                )
                group by 1,2
            ) a 
        ) a
        where true 
        -- and a.gap_days<=180 
        and a.all_succ_orders>=50 and a.all_succ_money/all_succ_orders between {4} and {5}
        order by 1,2
    '''.format(merch_concat, min_part_day_sub240, min_part_day_sub90, tmp_merch_attr_concat, unit_price_lower, unit_price_upper)
    df_pred_chargeback_rate_tmp = pd.read_sql(tmp_old_merch_sql,con=doris_con)
    if not len(df_pred_chargeback_rate_tmp):
        print('获取初始化信息, 无法预测退款率', tmp_merch)
        print('\t初始化信息如下：\n\t{}\n\t{}\n\t{}\n\t{}\n\t{}\n\t{}'.format(merch_concat, min_part_day_sub240, min_part_day_sub60, tmp_merch_attr_concat, unit_price_lower, unit_price_upper))
        continue
    df_pred_chargeback_rate_tmp['new_merchant_no'] = tmp_merch
    df_pred_chargeback_rate = df_pred_chargeback_rate.append(df_pred_chargeback_rate_tmp)

    time.sleep(2)

df_pred_chargeback_rate.reset_index(inplace=True, drop=True)
df_pred_chargeback_rate.to_excel('./df_pred_chargeback_rate.xlsx')

df_gap_days = df_pred_chargeback_rate.groupby(['new_merchant_no', 'gap_days'])[['chargeback_orders']].sum()
df_gap_days.reset_index(inplace=True)

# 构造笛卡尔积 数据集
r = range(180)
m = set(df_pred_chargeback_rate.new_merchant_no)
import itertools
r_m = list(itertools.product(r, m))
df_r_m = pd.DataFrame(r_m)
df_r_m.columns = ['r','m']

df_gap_days2 = pd.merge(df_r_m, df_gap_days, how='left', left_on=['r', 'm'], right_on=['gap_days', 'new_merchant_no'])
df_gap_days2['chargeback_orders'] = df_gap_days2.chargeback_orders.fillna(0)
df_gap_days2.sort_values(by=['m', 'r'], ascending=[True, True], inplace=True)

df_gap_days2['cumsum_chargeback_orders'] = df_gap_days2.groupby(['m'])['chargeback_orders'].cumsum()
df_gap_days2 = df_gap_days2[['m', 'r', 'chargeback_orders', 'cumsum_chargeback_orders']]

df_succ_orders = df_pred_chargeback_rate.groupby(['new_merchant_no'])[['pay_succ_orders']].sum()
df_succ_orders.reset_index(inplace=True)

df_pred_r_rate = pd.merge(df_gap_days2, df_succ_orders, how='left', left_on='m', right_on='new_merchant_no')
df_pred_r_rate['risk_type'] = 'chargeback'
df_pred_r_rate['accum_rate'] = df_pred_r_rate.cumsum_chargeback_orders/df_pred_r_rate.pay_succ_orders
df_pred_r_rate = df_pred_r_rate[['risk_type', 'm', 'r', 'accum_rate']]
df_pred_r_rate.rename(columns = {'risk_type':'risk_type', 'm': 'new_merchant_no', 'r': 'r', 'accum_rate':'accum_rate'}, inplace=True)
df_pred_r_rate.to_excel('./df_pred_r_rate_dim2.xlsx')

df_pred_r_rate.to_sql(
    name='tmp_ads_new_merchant_pred_r_rate',
    con=doris_con, if_exists='append', index=False, schema='tmp'
)
