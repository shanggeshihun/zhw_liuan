# _*_coding:utf-8 _*_

#@Time      : 2022/1/24  18:46
#@Author    : An
#@File      : ReadSql.py
#@Software  : PyCharm

import time, datetime
from WorkWeixinRobot.work_weixin_robot import WWXRobot
import numpy as np
import pandas as pd
import configparser
import warnings
from sqlalchemy import create_engine
from pyhive import presto
import pymysql

# 从mysql查询数据
class DatabaseResult():
    def __init__(self,database_type_name):
        """
        :param database_type_name: 数据库类型 hive,mysql
        :param table_name: 查询的数据表名
        :param is_valid_of_date_param: 是否引用日期条件 0,1
        :param start_date: 起始日期
        :param end_date: 结束日期
        """
        self.database_type_name = database_type_name
    # ----------------------------------数据库（presto+redis）配置读取---------------------------------------------------
    def get_connect_string(self):
        """
        :param database_type_name: 数据库类型名称，hive，mysql
        :return: 数据库连接字符串
        """
        warnings.filterwarnings("ignore")
        # ------------------------数据库配置读取----------------------------
        cf = configparser.ConfigParser()
        if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini",encoding='utf-8') == []:
            """服务器模式"""
            cf.read("/home/zhwom/config/config.ini",encoding='utf-8')
        else:
            """本地模式"""
            cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini",encoding='utf-8')

        if self.database_type_name == 'hive':
            ##hive数据库
            host = cf.get("hive_presto", "host")
            username = cf.get("hive_presto", "username")
            port = cf.get("hive_presto", "port")
            schema = cf.get("hive_presto", "schema")
            catalog = cf.get("hive_presto", "catalog")
            hive_con_string = presto.connect(host=host, port=port, username=username, schema=schema,catalog=catalog)
            return hive_con_string

        if self.database_type_name == 'hive_kudu':
            ##hive数据库
            host = cf.get("hive_presto_kudu", "host")
            username = cf.get("hive_presto_kudu", "username")
            port = cf.get("hive_presto_kudu", "port")
            schema = cf.get("hive_presto_kudu", "schema")
            catalog = cf.get("hive_presto_kudu", "catalog")
            hive_kudu_con_string = presto.connect(host=host, port=port, username=username, schema=schema,catalog=catalog)
            return hive_kudu_con_string

        elif self.database_type_name == 'mysql':

            # 运营数据库
            host = cf.get("Mysql-data_yunying", "host")
            user = cf.get("Mysql-data_yunying", "user")
            password = cf.get("Mysql-data_yunying", "password")
            DB = cf.get("Mysql-data_yunying", "DB")
            port = cf.get("Mysql-data_yunying", "port")
            mysql_con_string = create_no_chiine(
                "mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + DB, echo=False)
            return mysql_con_string

        else:
            error_print = '不支持当前数据库连接'
            print(error_print)
            return 'database_type_name is error'

    def get_query_result(self,table_name,is_valid_of_date_param,start_date = '1970-01-01',end_date= '1970-01-01'):
        self.table_name = table_name
        self.is_valid_of_date_param = is_valid_of_date_param
        self.start_date = start_date
        self.end_date = end_date

        if self.is_valid_of_date_param == 0 :
            query_sql = "select * from {0}".format(self.table_name)
        elif self.is_valid_of_date_param == 1:
            query_sql = "select * from {0} where part_day between '{1}' and '{2}'".format(self.table_name,self.start_date,self.end_date)
        else:
            pass
        result = pd.read_sql(query_sql,self.get_connect_string())
        return result