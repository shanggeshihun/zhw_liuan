# _*_coding:utf-8 _*_

# @Time      : 2022/11/15  17:26
# @Author    : An
# @File      : wow_dwm_shop_measures_by_createday.py
# @Software  : PyCharm


import time, datetime, configparser, warnings, math, platform
import sys
import pymysql, presto
# import pypresto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件\在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysql import OperateMysql
from OperateMysqlNew import OperateMysqlNew
from OperatePresto import OperatePresto
from SchedualToMysql import SchedualInfo


class WowDwsUsersByDay:
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

        self.final_result_log = {}

        # 运营数据库
        self.mysql_host = cf.get("Mysql-data_yunying", "host")
        self.mysql_user = cf.get("Mysql-data_yunying", "user")
        self.mysql_password = cf.get("Mysql-data_yunying", "password")
        self.mysql_db = cf.get("Mysql-data_yunying", "DB")
        self.mysql_port = cf.get("Mysql-data_yunying", "port")

        # 本地MySQL8.0
        self.mysql_local8_host = cf.get("MySQL8.0-localhost", "host")
        self.mysql_local8_user = cf.get("MySQL8.0-localhost", "user")
        self.mysql_local8_password = cf.get("MySQL8.0-localhost", "password")
        self.mysql_local8_db = cf.get("MySQL8.0-localhost", "DB")
        self.mysql_local8_port = cf.get("MySQL8.0-localhost", "port")

        # hive数据库
        self.presto_host = cf.get("hive_presto", "host")
        self.presto_username = cf.get("hive_presto", "username")
        self.presto_port = cf.get("hive_presto", "port")
        self.presto_schema = cf.get("hive_presto", "schema")
        self.presto_catalog = cf.get("hive_presto", "catalog")

    def run(self, part_day):
        """
        :param part_day: 日期参数
        :return: 抽奖主题：抽奖日期+用户维度  指标数据 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysqlNew(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # 实例化mysql 本地 8.0
        operate_mysql_local8 = OperateMysqlNew(
            username=self.mysql_local8_user,
            password=self.mysql_local8_password,
            host_ip=self.mysql_local8_host,
            port=int(self.mysql_local8_port),
            database=self.mysql_local8_db
        )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # 清理目标表数据
        mysql_local_sql = "delete from zhw_luck_shop_dws_m_day_1012 where action_day = '{0}'".format(part_day)
        operate_mysql_local8.delete_data(mysql_local_sql)

        # 原始表 数据处理
        mysql_sql = """
            select id,uid,action_name,type_name,action_luck,action_time,action_day,rest_give_luck,real_check_luck,action_time_rk
            from zhw_luck_shop_dws_m_day_1012
            where true
            and action_day = '{0}'
        """.format(part_day)
        mysql_data_list = operate_mysql.query_data(mysql_sql)

        operate_mysql_log = {}
        for k,v in operate_mysql.operate_result.items():
            if v[0] != '无':
                operate_mysql_log[k] = v

        operate_mysql.close_conn()

        # 目标表 数据写入
        columns = 10
        insert_local_sql = "insert into zhw_luck_shop_dws_m_day_1012(id,uid,action_name,type_name,action_luck,action_time,action_day,rest_give_luck,real_check_luck,action_time_rk) values ({0});".format(
            ','.join(['%s'] * columns))
        operate_mysql_local8.insert_data(insert_local_sql, mysql_data_list)

        operate_mysql_local8_log = {}
        for k,v in operate_mysql_local8.operate_result.items():
            if v[0] != '无':
                operate_mysql_local8_log[k] = v

        operate_mysql_local8.close_conn()

        # 数据日志记录
        self.final_result_log['operate_mysql_log']=  operate_mysql_log
        self.final_result_log['operate_mysql_local8']=  operate_mysql_local8_log

if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    for i in range(48,82):
        start_time = time.time()
        day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1

        wow_dws_users_by_day = WowDwsUsersByDay()
        wow_dws_users_by_day.run(day_last_1)
        end_time = time.time()
        print(day_last_1, '运行耗时：', end_time - start_time)
