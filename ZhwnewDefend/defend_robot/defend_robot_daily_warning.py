# _*_coding:utf-8 _*_

# @Time      : 2023/2/19  18:26
# @Author    : An
# @File      : defend_dwd_hao_lock_with_check_info.py
# @Software  : 封号查询明细写入 public.zhw_hao_lock_with_check_info


import time, datetime, configparser, warnings, math, platform
import sys
import pymysql, presto
# import pypresto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysqlNew import OperateMysqlNew
from OperatePresto import OperatePresto
from OperateHologresNew import OperateHologresNew



class DefendCommen:
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
        # self.mysql_local8_host = cf.get("MySQL8.0-localhost", "host")
        # self.mysql_local8_user = cf.get("MySQL8.0-localhost", "user")
        # self.mysql_local8_password = cf.get("MySQL8.0-localhost", "password")
        # self.mysql_local8_db = cf.get("MySQL8.0-localhost", "DB")
        # self.mysql_local8_port = cf.get("MySQL8.0-localhost", "port")

        # hive数据库
        self.presto_host = cf.get("hive_presto", "host")
        self.presto_username = cf.get("hive_presto", "username")
        self.presto_port = cf.get("hive_presto", "port")
        self.presto_schema = cf.get("hive_presto", "schema")
        self.presto_catalog = cf.get("hive_presto", "catalog")

        # hologres数据库
        self.holo_host = cf.get("hologres-dofun", "host")
        self.holo_port = cf.get("hologres-dofun", "port")
        self.holo_database = cf.get("hologres-dofun", "DB")
        self.holo_user = cf.get("hologres-dofun", "user")
        self.holo_password = cf.get("hologres-dofun", "password")

    def run(self, sql_file_name, start_day ,end_day):
        """
        :param start_day: 起始日期参数
        :param end_day: 终止日期参数
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
        # operate_mysql_local8 = OperateMysqlNew(
        #     username=self.mysql_local8_user,
        #     password=self.mysql_local8_password,
        #     host_ip=self.mysql_local8_host,
        #     port=int(self.mysql_local8_port),
        #     database=self.mysql_local8_db
        # )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # 实例化Hologres，查询待更新的数据信息
        operate_hologres = OperateHologresNew(
            username=self.holo_user,
            password=self.holo_password,
            host_ip=self.holo_host,
            port=int(self.holo_port),
            database=self.holo_database
        )

        # 清理目标表数据
        # holo_public_sql = "delete from public.zhw_hao_lock_with_check_info where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'".format(start_day ,end_day)
        # operate_hologres.update_data(holo_public_sql)
        # print('清理目标表数据')

        # 安防查询封号-steam
        file = open(r"{}.sql".format(sql_file_name), mode='r', encoding='utf-8')
        holo_sql = file.read()
        holo_sql = holo_sql.format(start_day,end_day)
        file.close()

        operate_hologres.update_data(holo_sql)
        operate_hologres.close_conn()

if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    today_last_days_ = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # d

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    start_time = time.time()
    start_end_list = [
        ('2023-01-01', '2023-01-16'),
        ('2023-01-17', '2023-02-01'),
        ('2023-02-02', '2023-02-17'),
        ('2023-02-18', '2023-03-05'),
        ('2023-03-06', '2023-03-21'),
        ('2023-03-22', '2023-04-06'),
        ('2023-04-07', '2023-04-22'),
        ('2023-04-23', '2023-05-08'),
        ('2023-05-09', '2023-05-24'),
        ('2023-05-25', '2023-06-09'),
        ('2023-06-10', '2023-06-25'),
        ('2023-06-26', '2023-07-11')
    ]
    start_end_list = [
        ('2023-07-05', '2023-07-18')
    ]
    sql_file_name_list=  [
        'same_idcard_multi_user_get_hb',
        'same_phone_multi_user_get_hb',
        'same_paynum_multi_user_get_hb',
        'same_hour_multi_user_get_hb',
        'same_ip_multi_user_get_hb',
        'user_get_hb'
    ]
    for sql_file_name in sql_file_name_list:
        for idx,tup in enumerate(start_end_list):
            start_day, last_day = tup[0], tup[1]
            defend_commen = DefendCommen()
            defend_commen.run(sql_file_name, start_day ,last_day)
            time.sleep(1)
        end_time = time.time()

        print('运行耗时：', end_time - start_time)