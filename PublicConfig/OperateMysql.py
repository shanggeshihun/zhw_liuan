# _*_coding:utf-8 _*_
# @Time　　 : 2022/9/9   10:04
# @Author　 : zimo
# @File　   :OperateMysql.py
# @Software :PyCharm
# @Theme    :不再捕获报错信息（当引用该类时，便于判断数据是否报错）

import pymysql, time
import os
import configparser
import warnings

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class OperateMysql():
    def __init__(self, username, password, host_ip, port, database):
        '''
        :param username:    数据库用户名
        :param password:    数据库密码
        :param host_ip:     数据库主机IP地址
        :param port:        数据库主机端口
        :param database:   数据库名称
        :param sql:         数据库执行SQL语句
        '''
        self.username = username
        self.password = password
        self.host_ip = host_ip
        self.port = port
        self.database = database

        self.conn = pymysql.connect(
            host=self.host_ip,
            port=int(self.port),
            user=self.username,
            passwd=self.password,
            db=self.database,
            charset= 'utf8'
        )
        self.cursor = self.conn.cursor()

    def close_conn(self):
        self.conn.close()
        self.cursor.close()

    def query_data(self, sql):
        self.cursor.execute(sql)  # 执行SQL语句
        data = self.cursor.fetchall()  # 取出数据库所有数据，该操作慢
        self.index = self.cursor.description
        return data  # 返回数据

    def query_data_index(self):
        columns_list = []
        index = self.cursor.description
        for tup in index:
            columns_list.append(tup[0])
        return columns_list  # 返回数据

    def update_data(self, sql):
        try:
            self.cursor.execute(sql)  # 执行SQL语句
            self.cursor.execute("commit")
        except Exception as e:
            self.conn.rollback()

    def insert_data(self, sql):
        self.cursor.execute(sql)  # 执行SQL语句
        self.cursor.execute("commit")


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    # ------------------------数据库配置读取----------------------------
    cf = configparser.ConfigParser()
    if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini",'utf-8') == []:
        """服务器模式"""
        cf.read("/home/zhwom/config/config.ini",'utf-8')
    else:
        """本地模式"""
        cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini",'utf-8')

    # MySQL连接配置
    mysql_host = cf.get("Mysql-data_yunying", "host")
    mysql_user = cf.get("Mysql-data_yunying", "user")
    mysql_password = cf.get("Mysql-data_yunying", "password")
    mysql_database = cf.get("Mysql-data_yunying", "DB")
    mysql_port = cf.get("Mysql-data_yunying", "port")

    # 实例化mysql
    operate_mysql = OperateMysql(
        username=mysql_user,
        password=mysql_password,
        host_ip=mysql_host,
        port=int(mysql_port),
        database=mysql_database
    )

    # mysql sql操作语句
    mysql_sql = """
       select packet_type,packet_id
            from red_packet_trigger_way
            where way = 'push'
            limit 5
    """
    try:
        result_data = operate_mysql.query_data(mysql_sql)
        result_data_columns = operate_mysql.query_data_index()
    except Exception as e:
        operate_mysql.close_conn()
        print(e)
    else:
        print(result_data)
        print(result_data_columns)
        operate_mysql.close_conn()