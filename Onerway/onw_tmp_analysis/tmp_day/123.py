# _*_coding:utf-8 _*_
# @Time     :2023/10/30 11:09
# @Author   : anliu
# @File     :123.py
# @Theme    :PyCharm


import pandas as pd
df = pd.read_csv(r"C:\Users\zimo1\Desktop\all_alerts.csv", encoding='utf-8')
df2 = df.head(3)
data_list = df2.values
print(data_list)