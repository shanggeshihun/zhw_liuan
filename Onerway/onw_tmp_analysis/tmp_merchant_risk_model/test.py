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

import pandas as pd
import itertools

set1 = (1, 2, 3)
set2 = {'A', 'B', 'C'}

cartesian_product = list(itertools.product(set1, set2))

print(pd.DataFrame(cartesian_product))