# _*_coding:utf-8 _*_

# @Time      : 2022/6/17  17:01
# @Author    : An
# @File      : gmv_dim_month_game_renttype_zqinterval.py
# @Software  : PyCharm

import time, datetime, configparser, warnings, math, platform
import sys
import pandas as pd
import pymysql, presto
import pyhive.presto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine

import json, requests

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysql import OperateMysql
from OperatePresto import OperatePresto
from SchedualToMysql import SchedualInfo
from OperateHologresNew import OperateHologresNew

from QqexmailSmtpAttach import QqExmailSmtp


class ZhwAnalysis:
    def __init__(self):
        warnings.filterwarnings("ignore")
        # ------------------------数据库配置读取----------------------------
        cf = configparser.ConfigParser()
        if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
            """服务器模式"""
            cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
        else:
            """本地模式"""
            cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

        # 运营数据库
        self.mysql_host = cf.get("Mysql-data_yunying", "host")
        self.mysql_user = cf.get("Mysql-data_yunying", "user")
        self.mysql_password = cf.get("Mysql-data_yunying", "password")
        self.mysql_db = cf.get("Mysql-data_yunying", "DB")
        self.mysql_port = cf.get("Mysql-data_yunying", "port")

        # hive数据库
        self.presto_host = cf.get("hive_presto", "host")
        self.presto_username = cf.get("hive_presto", "username")
        self.presto_port = cf.get("hive_presto", "port")
        self.presto_schema = cf.get("hive_presto", "schema")
        self.presto_catalog = cf.get("hive_presto", "catalog")

        # 百度商业账号
        self.bdtj_username = cf.get("bdtj_account_info", "username")
        self.bdtj_password = cf.get("bdtj_account_info", "password")
        self.bdtj_token = cf.get("bdtj_account_info", "token")

        # hologres数据库
        self.holo_host = cf.get("hologres-dofun", "host")
        self.holo_port = cf.get("hologres-dofun", "port")
        self.holo_database = cf.get("hologres-dofun", "DB")
        self.holo_user = cf.get("hologres-dofun", "user")
        self.holo_password = cf.get("hologres-dofun", "password")

    def zhw_tmp_query(self,sql_file_name, start_day, end_day, start_hour, end_hour):
        """
        :param part_day: 当前月份参数
        :return: 月份维度
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )
        cnx_mysql = create_engine(
            "mysql+pymysql://" + self.mysql_user + ":" + self.mysql_password + "@" + self.mysql_host + ":" + self.mysql_port + "/" + self.mysql_db,
            echo=False)

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )
        con_presto = pypresto.connect(host=self.presto_host, port=self.presto_port, username=self.presto_username,
                                      schema=self.presto_schema, catalog=self.presto_catalog)

        # 实例化Hologres，查询待更新的数据信息
        operate_hologres = OperateHologresNew(
            username=self.holo_user,
            password=self.holo_password,
            host_ip=self.holo_host,
            port=int(self.holo_port),
            database=self.holo_database
        )

        file = open(r"{}.sql".format(sql_file_name), mode='r', encoding='utf-8')
        holo_sql = file.read()
        holo_sql = holo_sql.format(start_day, end_day, start_hour, end_hour)
        file.close()

        holo_data_list = operate_hologres.query_data(holo_sql)

        if not holo_data_list:
            return
        holo_data_columns = operate_hologres.query_data_index()
        operate_hologres.close_conn()

        df_result = pd.DataFrame(holo_data_list)
        df_result.columns = holo_data_columns

        return df_result


if __name__ == '__main__':
    df = pd.DataFrame()
    sql_file_name = 'tmp2'
    start_time = time.time()

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h


    sql_file_name = 'strategy_withdraw_8_90pp_minute'
    df = pd.DataFrame()

    start,end = '2023-06-14','2023-06-16'
    date_hour_range = pd.date_range(start=start, end=end, freq='30min')
    a = [d.strftime('%Y-%m-%d') for d in date_hour_range.tolist()][:-4]
    b = [d.strftime('%Y-%m-%d') for d in date_hour_range.tolist()][4:]
    c = [d.strftime('%Y-%m-%d %H:%M:00') for d in date_hour_range.tolist()][:-4]
    d = [d.strftime('%Y-%m-%d %H:%M:00') for d in date_hour_range.tolist()][4:]
    date_hour_list = list(zip(a,b,c,d))

    for idx, start_end_tuple in enumerate(date_hour_list):
        start_time = time.time()
        start_day, end_day, start_hour, end_hour = start_end_tuple[0], start_end_tuple[1], start_end_tuple[2], start_end_tuple[3]
        zhw_analysis = ZhwAnalysis()
        df_result = zhw_analysis.zhw_tmp_query(sql_file_name, start_day, end_day, start_hour, end_hour)
        df = df.append(df_result)
        end_time = time.time()
        print(start_end_tuple, sql_file_name, '，运行耗时：%s s' % (end_time - start_time))
        time.sleep(2)

    current_time = (datetime.datetime.now()).strftime('%Y-%m-%d %H%M%S')

    df.to_excel(r"./tmp_day_minute_data/{0}_{1}_{2}.xlsx".format(sql_file_name,start,end))