# _*_coding:utf-8 _*_
# @Time     :2023/11/22 17:03
# @Author   : anliu
# @File     :process_merchant_basis_v1.py
# @Theme    :PyCharm

import time, datetime, configparser, warnings, math, platform
import sys, itertools, re

import numpy as np
import pandas as pd
import pymysql, presto
from sqlalchemy import create_engine


df = pd.read_excel(r'./df_v3_step1.xlsx')

df_v3_step2 = pd.DataFrame()

df['same_merchant_name'] = ''

while True:
    if len(df) == 0:
        break
    for idx, item in df.iterrows():
        # 判断条件：merchant_email 相同 或者 linkman_email 相同 或者 main_merchant_name 相同 或者 sub_merchant_no 与 merchant_no 相同
        condition = (df['merchant_email'] == item['merchant_email']) | (df['linkman_email'] == item['linkman_email']) | (
                    df['main_merchant_name'] == item['main_merchant_name']) | (df['sub_merchant_no'] == item['merchant_no'])
        # print(list(df.loc[condition, :].index))
        # 根据条件筛选出符合条件的行，并获取其 main_merchant_name
        same_merchant_name = df.loc[condition, 'main_merchant_name'].unique()

        if len(same_merchant_name) > 0:
            idx_list = list(df.loc[condition, :].index)
            tmp_same_merchant_name = same_merchant_name[0]  # 如果有相同 main_merchant_name，则返回第一个
        else:
            idx_list=[idx]
            tmp_same_merchant_name = item['main_merchant_name']  # 如果没有相同 main_merchant_name，则保持原值

        df.loc[idx_list, 'same_merchant_name'] = tmp_same_merchant_name

        tmp_df = df.loc[idx_list, :]
        df_v3_step2 = df_v3_step2.append(tmp_df)

        df.drop(idx_list, inplace=True)
        break

    print('df长度:', len(df), 'df_v3_step2长度:', len(df_v3_step2))
df_v3_step2.to_excel('./df_v3_step2.xlsx')