# _*_coding:utf-8 _*_
# @Time     :2023/10/30 11:09
# @Author   : anliu
# @File     :123.py
# @Theme    :PyCharm
import sys

import pandas as pd
import random

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

# 创建用户列表
users = ["User" + str(i) for i in range(11)]

# 创建性别列表
genders = [random.choice([0, 1]) for _ in range(11)]

# 创建职业列表
occupations = [random.choice(["Engineer", "Doctor", "Teacher", "Artist", "Student"]) for _ in range(11)]

# 创建籍贯列表
hometowns = [random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Miami"]) for _ in range(11)]

# 创建收入列表（假设以千为单位）
incomes = [random.randint(30, 100) * 1000 for _ in range(11)]

# 创建DataFrame
data = {
    "用户": users,
    "性别": genders,
    "职业": occupations,
    "籍贯": hometowns,
    "收入": incomes,
    "收入2": incomes
}

df = pd.DataFrame(data)
df2 = pd.DataFrame(data)

df_ = pd.DataFrame()
df_ = df_.append(df)
df_ = df_.append(df2)
df_.reset_index(inplace=True,drop=True)
print(df_.head(3))



# df2 = df.groupby('性别')[['收入','收入2']].max()
# df2.reset_index(inplace=True)
# print(df2)