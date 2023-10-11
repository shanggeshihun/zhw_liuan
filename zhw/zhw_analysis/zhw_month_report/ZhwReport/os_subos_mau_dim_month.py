# _*_coding:utf-8 _*_
# @Time　　 : 2022/6/11 1:29
# @Author　 : liuan
# @File　 　: os_subos_mau_dim_month.py
# @Theme : PyCharm


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

    def os_subos_mau_dim_month(self, part_month):
        """
        :param part_day: 月份参数
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
        presto_sql = """
            select 
                part_month,
                count(distinct case when lxfl in(1,7,8,9) then userid end ) as "aa-PC-MAU",
                count(distinct case when lxfl in(2,3,4,5,6) then userid end ) as "aa-移动-MAU",
                count(distinct case when lxfl=10 then userid end ) as "a-其他-MAU",
                count(distinct case when lxfl=1 then userid end ) as "a-官网-MAU",
                count(distinct case when lxfl in(7,8,9) then userid end ) as "b-客户端-MAU",
                count(distinct case when lxfl=7 then userid end ) as "a-官网客户端-MAU",
                count(distinct case when lxfl=9 then userid end ) as "a-网吧客户端-MAU",
                count(distinct case when lxfl=8 then userid end ) as "a-市场客户端-MAU",
                count(distinct case when lxfl in(3,4,5,6) then userid end ) as "b-app-MAU",
                count(distinct case when lxfl=2 then userid end ) as "a-M站-MAU",
                count(distinct case when lxfl=3 then userid end ) as "a-app安卓-MAU",
                count(distinct case when lxfl=6 then userid end ) as "a-app苹果-MAU",
                count(distinct case when lxfl=4 then userid end ) as "a-applite-MAU",
                count(distinct case when lxfl=5 then userid end ) as "a-apppro-MAU"
            from 
            (   
                select lxfl,userid,part_month
                from hive.zhwdb.zhw_user_login_log_extend 
                where part_month = '{0}' 
                group by 1,2,3
            ) t
            group by 1
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.iloc[:, 1:].astype('float')
        df_result.to_excel(r"./zhw_rpt_data/os_subos_mau_dim_month_{0}.xlsx".format(part_month), index=False)


if __name__ == '__main__':
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    part_month_list = ['2022-05', '2022-04', '2022-03', '2021-05', '2021-04', '2021-03']
    part_month_list = ['2022-11', '2021-11']
    for part_month in part_month_list:
        start_time = time.time()
        zhw_analysis = ZhwAnalysis()
        zhw_analysis.os_subos_mau_dim_month(part_month)
        end_time = time.time()
        print('os_subos_mau_dim_month，运行耗时：%s s' % (end_time - start_time))
