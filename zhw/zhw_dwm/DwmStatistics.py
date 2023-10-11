# _*_coding:utf-8 _*_

# @Time      : 2022/7/19  17:17
# @Author    : An
# @File      : DwmStatistics.py
# @Software  :


import time, datetime, configparser, warnings, math, platform,calendar
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


class Dwm:
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

    def zhw_dwm_dau_day(self, part_day):
        """
        :param part_day: 日期参数
        :return: 租号玩每日的日活，安卓日活，ios日活
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
        # -- 1 重点关注指标
        # 清空昨日数据
        delete_sql = "delete from zhw_dwm_dau_day where part_day = '{}'".format(
            part_day)

        operate_mysql.update_data(delete_sql)

        presto_sql = """
            select part_day,
                coalesce(count(distinct userid),0) as dau,
                coalesce(count(distinct case when lxfl = 3 then userid end),0) as android_common_dau,
                coalesce(count(distinct case when lxfl = 4 then userid end),0) as android_lite_dau,
                coalesce(count(distinct case when lxfl = 5 then userid end),0) as android_pro_dau,
                coalesce(count(distinct case when lxfl in (3,4,5) then userid end),0) as android_dau,
                coalesce(count(distinct case when lxfl = 6 then userid end),0) as ios_dau
            from zhwdb.zhw_user_login_log_extend 
            where part_day = '{}' 
            group by 1
        """.format(part_day)
        presto_data_list = operate_presto.query_data(presto_sql)

        # presto_data_list 是以list为元素的list(批量插入)
        if presto_data_list:
            # 每次写入50条数据
            step = 50
            length_data = len(presto_data_list)
            r = math.ceil(length_data / step)
            for i in range(r):
                tmp_list = presto_data_list[i * step:(i + 1) * step]
                batch_sql = ','.join([str(tuple(a)) for a in tmp_list])
                insert_sql = "insert into zhw_dwm_dau_day(part_day,dau,android_common_dau,android_lite_dau,android_pro_dau,android_dau,ios_dau) values {0};".format(
                    batch_sql)
                operate_mysql.insert_data(insert_sql)
        operate_presto.close_conn()

if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    for i in range(3,4):
        start_time = time.time()

        day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
        print('开始跑{}的数据'.format(day_last_1))

        dwm  = Dwm()
        dwm.zhw_dwm_dau_day(day_last_1)

        end_time = time.time()

        print(i, '运行耗时：%s s' % (end_time - start_time))
