# _*_coding:utf-8 _*_
# @Time     :2023/11/22 17:03
# @Author   : anliu
# @File     :process_merchant_basis_v1v3_step2.py
# @Theme    :商户大账号

import time, datetime, configparser, warnings, math, platform
import sys, itertools, re

import numpy as np
import pandas as pd
import pymysql, presto
from sqlalchemy import create_engine


df_v3_step1 = pd.read_excel(r'./df_v3_step1.xlsx', dtype={'merchant_no': str, 'sub_merchant_no': str})
df_v3_step1.rename(columns={'login_email': 'linkman_email'}, inplace=True)
df_v3_step1 = df_v3_step1.astype(str)

df_v1_step1 = pd.read_excel(r'./df_v1_step1.xlsx', dtype={'merchant_no': str, 'sub_merchant_no': str})
df_v1_step1 = df_v1_step1.astype(str)

df = pd.concat([df_v1_step1, df_v3_step1], ignore_index=True)

df.reset_index(drop=True, inplace=True)

df = df.applymap(lambda x: x.lower())

df.replace('nan', np.nan, inplace=True)



df['same_merchant_name'] = ''

for idx, item in df.iterrows():
    print(idx)
    # 使用merchant_name作为大客户名称的标记
    same_merchant_name = item['main_merchant_name']

    # 找到所有与当前商户相关的行
    condition = (df['merchant_email'] == item['merchant_email']) | (df['linkman_email'] == item['linkman_email']) | (
                df['main_merchant_name'] == item['main_merchant_name']) | (df['sub_merchant_no'] == item['merchant_no'])

    same_merchant_name_series = df.loc[condition, 'same_merchant_name']
    if any(same_merchant_name_series != ''):
        same_merchant_name = same_merchant_name_series[same_merchant_name_series != ''].iloc[0]

    # 更新大客户名称列
    df.loc[condition, 'same_merchant_name'] = same_merchant_name

df.to_excel('./df_v1v3_step2.xlsx')