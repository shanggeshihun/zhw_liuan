# _*_coding:utf-8 _*_
# @Time     :2023/11/22 17:03
# @Author   : anliu
# @File     :process_merchant_basis.py
# @Theme    :PyCharm

import time, datetime, configparser, warnings, math, platform
import sys, itertools, re

import numpy as np
import pandas as pd
import pymysql, presto
from sqlalchemy import create_engine

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

# df_v1 = pd.read_excel('./df_v1.xlsx')
df_v1= pd.read_excel('./df_v1_test.xlsx')
df_v1 = df_v1.astype(str)
df_v1 = df_v1.applymap(lambda x: x.lower())

merchant_no_list, merchant_name_list, merchant_email_list, linkman_email_list = [], [], [], []
main_merchant_name_list, sub_merchant_no_list = [], []
for idx, item in df_v1.iterrows():

    merchant_no = item['merchant_no']
    merchant_name = item['merchant_name']
    merchant_email = item['merchant_email']
    linkman_email = item['linkman_email']

    sub_merchant_no = np.nan
    main_merchant = np.nan

    if 'limited' in item['merchant_name'] or '公司' in item['merchant_name']:
        tmp_list = item['merchant_name'].split('limited') if 'limited' in item['merchant_name'] else item['merchant_name'].split('公司')
        main_merchant_name = tmp_list[0]
        if len(tmp_list) == 1:
            merchant_no_list.append(merchant_no)
            merchant_name_list.append(merchant_name)
            merchant_email_list.append(merchant_email)
            linkman_email_list.append(linkman_email)
            main_merchant_name_list.append(main_merchant)
            sub_merchant_no_list.append(sub_merchant_no)
        else:
            extra_info = tmp_list[1]
            if re.match(r'(\d+)', extra_info):
                sub_merchant_no_list = re.findall(r'(\d+)', extra_info)

                repeat_time = len(sub_merchant_no_list)

                merchant_no_list.extend(repeat_time*[merchant_no])
                merchant_name_list.extend(repeat_time*[merchant_name])
                merchant_email_list.extend(repeat_time*[merchant_email])
                linkman_email_list.extend(lrepeat_time*[linkman_email])
                main_merchant_name_list.extend(repeat_time*[main_merchant])
                sub_merchant_no_list.extend(sub_merchant_no_list)
            else:
                merchant_no_list.append(merchant_no)
                merchant_name_list.append(merchant_name)
                merchant_email_list.append(merchant_email)
                linkman_email_list.append(linkman_email)
                main_merchant_name_list.append(main_merchant)
                sub_merchant_no_list.append(sub_merchant_no)
    else:
        merchant_no_list.append(merchant_no)
        merchant_name_list.append(merchant_name)
        merchant_email_list.append(merchant_email)
        linkman_email_list.append(linkman_email)
        main_merchant_name_list.append(main_merchant)
        sub_merchant_no_list.append(sub_merchant_no)

df_v1_2 = pd.DataFrame(
    {
        'merchant_no': merchant_no_list,
        'merchant_name': merchant_name_list,
        'merchant_email': merchant_email_list,
        'linkman_email': linkman_email_list,
        'main_merchant_name': main_merchant_name_list,
        'sub_merchant': sub_merchant_no_list
    }
)
print(df_v1_2)
