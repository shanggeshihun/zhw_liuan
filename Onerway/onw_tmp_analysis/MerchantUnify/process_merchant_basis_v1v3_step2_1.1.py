# _*_coding:utf-8 _*_
# @Time     :2023/11/22 17:03
# @Author   : anliu
# @File     :process_merchant_basis_v1v3_step2_1.1.py
# @Theme    :
'''
process_merchant_basis_v1v3_step2.py buy:
    第i次循环后会将关联到的删除，即使第j次循环与第i的结果有关联也无法合并
process_merchant_basis_v1v3_step2_1.1.py :
    第i次循环后结果与第j次循环的结果有重合则合并
'''

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
df = df.applymap(lambda x: x.lower())

print('替换前 np.nan 数量', '\n', df.isna().sum())
df.replace('nan', np.nan, inplace=True)
print('替换后 np.nan 数量', '\n', df.isna().sum())
df.reset_index(drop=True, inplace=True)

# 当 idx = 355 可以关联 idx 2623、6829、6830
condition = (df['merchant_email'] == 'xingchenjyuan@163.com') | (df['linkman_email'] == 'xingchenjyuan@163.com') | (
                    df['main_merchant_name'] == 'star era ltd-601502') | (df['sub_merchant_no'] == np.nan)
print(df.loc[condition, :])

# idx 355、2623、6829、6830 已经删除，剩余 5993
condition = (df['merchant_email'] == np.nan) | (df['linkman_email'] == np.nan) | (
                    df['main_merchant_name'] == '厦门星辰纪元网络科技有限公司') | (df['merchant_no'] == np.nan)
print(df.loc[condition, :])


df_v1v3_step2 = pd.DataFrame()

i = 0
for idx_, item_ in df.iterrows():
    print(idx_, len(df))
    i += 1
    print(i)
    # print()
    # print(item_)
    try:
        condition_ = (df['merchant_email'] == item_['merchant_email']) | (df['linkman_email'] == item_['linkman_email']) | (
            df['main_merchant_name'] == item_['main_merchant_name']) | (df['sub_merchant_no'] == item_['merchant_no'])
    except Exception as e:
        print(item_['merchant_no'], e)
    else:
        tmp_idx_ = list(df.loc[condition_].index)
        print(tmp_idx_)
        tmp_same_merchant_name = df.loc[condition_].loc[tmp_idx_[0], 'main_merchant_name']

    for idx, item in df.iterrows():
        if idx <= idx_:
            continue
        condition = (df['merchant_email'] == item['merchant_email']) | (df['linkman_email'] == item['linkman_email']) | (
                    df['main_merchant_name'] == item['main_merchant_name']) | (df['merchant_no'] == item['sub_merchant_no'])
        tmp_idx = list(df.loc[condition].index)

        if not set(tmp_idx_) & set(tmp_idx):
            continue
        else:
            tmp_idx_ = list(set(tmp_idx_ + tmp_idx))


    tmp_df_ = df.loc[tmp_idx_, :]
    tmp_df_['same_merchant_name'] = tmp_same_merchant_name
    df_v1v3_step2 = df_v1v3_step2.append(tmp_df_)

    df.drop(tmp_idx_, inplace=True)

    print('df长度:', len(df), 'df_v1v3_step2长度:', len(df_v1v3_step2))

df_v1v3_step2.to_excel('./df_v1v3_step2.xlsx')