# _*_coding:utf-8 _*_
# @Time　　 : 2022/9/9   10:04
# @Author　 : zimo
# @File　   :OperateMysql.py
# @Software :PyCharm
# @Theme    :不再捕获报错信息（当引用该类时，便于判断数据是否报错）

import pyhive
import presto
import time
import os
import configparser
import warnings

import sys
sys.setrecursionlimit( 2000 )

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class OperatePresto():
    def __init__(self, username, host_ip, port, catalog, schema):
        '''
        :param username:
        :param host_ip:
        :param port:
        :param catalog:
        :param schema:
        '''
        self.username = username
        self.host_ip = host_ip
        self.port = port
        self.catalog = catalog
        self.schema = schema

        self.conn = presto.dbapi.connect(
            host=self.host_ip,
            port=int(self.port),
            user=self.username,
            catalog=self.catalog,
            schema= self.schema
        )
        self.cursor = self.conn.cursor()

    def close_conn(self):
        self.conn.close()
        self.cursor.close()

    def query_data(self, sql):
        '''
        :param sql:
        :return: list作为元素的list
        '''
        self.cursor.execute(sql)  # 执行SQL语句
        data = self.cursor.fetchall()  # 取出数据库所有数据，该操作慢
        return data  # 返回数据

    def query_data_index(self):
        columns_list = []
        index = self.cursor.description
        for tup in index:
            columns_list.append(tup[0])
        return columns_list  # 返回数据

    def update_data(self, sql):
        self.cursor.execute(sql)  # 执行SQL语句
        self.cursor.execute("commit")


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    # ------------------------数据库配置读取----------------------------
    cf = configparser.ConfigParser()
    if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini",encoding='utf-8') == []:
        """服务器模式"""
        cf.read("/home/zhwom/config/config.ini",encoding='utf-8')
    else:
        """本地模式"""
        cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini",encoding='utf-8')

    # Presto 连接配置
    ##hive数据库
    presto_host = cf.get("hive_presto", "host")
    presto_username = cf.get("hive_presto", "username")
    presto_port = cf.get("hive_presto", "port")
    presto_schema = cf.get("hive_presto", "schema")
    presto_catalog = cf.get("hive_presto", "catalog")

    # 实例化presto
    operate_presto = OperatePresto(
        username=presto_username,
        host_ip=presto_host,
        port=int(presto_port),
        catalog=presto_catalog,
        schema=presto_schema
    )

    # presto sql操作语句
    presto_sql = """
        select *
		from hive.zhwdb.zhw_luck_shop_prize_detail 
		where true 
		and part_day = '2022-01-30'
		limit 2
    """
    try:
        result_data = operate_presto.query_data(presto_sql)
        result_data_columns = operate_presto.query_data_index()
    except Exception as e:
        print(e)
        operate_presto.close_conn()
        print(111)
    else:
        operate_presto.close_conn()
        print(result_data)
        print(result_data_columns)