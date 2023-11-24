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

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

df_v3 = pd.read_excel(r'./df_v3.xlsx',)
df_v3 = df_v3.astype(str)
df_v3 = df_v3.applymap(lambda x: x.lower())

df_v3.rename(columns={'login_email': 'linkman_email'}, inplace=True)

merchant_no_list, merchant_name_list, merchant_email_list, linkman_email_list = [], [], [], []
main_merchant_name_list, sub_merchant_no_list = [], []
for idx, item in df_v3.iterrows():

    merchant_no = item['merchant_no']
    merchant_name = item['merchant_name']
    merchant_email = item['merchant_email']
    linkman_email = item['linkman_email']

    main_merchant_name = merchant_name
    sub_merchant_no = np.nan
    if 'limited' not in merchant_name and '公司' not in merchant_name:
        merchant_no_list.append(merchant_no)
        merchant_name_list.append(merchant_name)
        merchant_email_list.append(merchant_email)
        linkman_email_list.append(linkman_email)
        main_merchant_name_list.append(main_merchant_name)
        sub_merchant_no_list.append(sub_merchant_no)
    else:
        main_merchant_name = re.findall('.*?limited|.*?公司', merchant_name)[0]
        extra_info = merchant_name.replace(main_merchant_name, '')
        if not extra_info:
            merchant_no_list.append(merchant_no)
            merchant_name_list.append(merchant_name)
            merchant_email_list.append(merchant_email)
            linkman_email_list.append(linkman_email)
            main_merchant_name_list.append(main_merchant_name)
            sub_merchant_no_list.append(sub_merchant_no)
        else:
            if re.search(r'\d{6}', extra_info):
                tmp_sub_merchant_no_list = re.findall(r'(\d{6})', extra_info)

                repeat_time = len(tmp_sub_merchant_no_list)

                merchant_no_list.extend(repeat_time*[merchant_no])
                merchant_name_list.extend(repeat_time*[merchant_name])
                merchant_email_list.extend(repeat_time*[merchant_email])
                linkman_email_list.extend(repeat_time*[linkman_email])
                main_merchant_name_list.extend(repeat_time*[main_merchant_name])
                sub_merchant_no_list.extend(tmp_sub_merchant_no_list)
            else:
                merchant_no_list.append(merchant_no)
                merchant_name_list.append(merchant_name)
                merchant_email_list.append(merchant_email)
                linkman_email_list.append(linkman_email)
                main_merchant_name_list.append(main_merchant_name)
                sub_merchant_no_list.append(sub_merchant_no)
df_v3_step1 = pd.DataFrame(
    {
        'merchant_no': merchant_no_list,
        'merchant_name': merchant_name_list,
        'merchant_email': merchant_email_list,
        'linkman_email': linkman_email_list,
        'main_merchant_name': main_merchant_name_list,
        'sub_merchant_no': sub_merchant_no_list
    }
)

df_v3_step1.to_excel('./df_v3_step1.xlsx')
