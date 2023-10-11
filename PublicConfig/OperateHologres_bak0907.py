# _*_coding:utf-8 _*_
# @Time　　 : 2020/6/12   10:04
# @Author　 : zimo
# @File　   :OperateHologres.py
# @Software :PyCharm
# @Theme    :

import psycopg2, time
import os
import configparser
import warnings

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class OperateHologres():
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

        self.conn = psycopg2.connect(
            host=self.host_ip,
            port=int(self.port),
            user=self.username,
            password=self.password,
            dbname=self.database
        )
        self.cursor = self.conn.cursor()

    def close_conn(self):
        self.conn.close()
        self.cursor.close()

    def query_data(self, sql):
        try:
            self.cursor.execute(sql)  # 执行SQL语句
            data = self.cursor.fetchall()  # 取出数据库所有数据，该操作慢
        except Exception as e:
            print('\033[1;35m 执行失败：{} \033[0m!'.format(e))
        else:
            return data  # 返回数据

    def query_data_index(self):
        columns_list = []
        try:
            index = self.cursor.description
            for tup in index:
                columns_list.append(tup[0])
        except Exception as e:
            print('\033[1;35m 执行失败：{} \033[0m!'.format(e))
        else:
            return columns_list  # 返回数据

    def update_data(self, sql):
        try:
            self.cursor.execute(sql)  # 执行SQL语句
            self.cursor.execute("commit")
        except Exception as e:
            print('\033[1;35m 执行失败：{} \033[0m!'.format(e))

    def insert_data(self, sql):
        try:
            self.cursor.execute(sql)  # 执行SQL语句
            self.cursor.execute("commit")
        except Exception as e:
            print('\033[1;35m 执行失败：{} \033[0m!'.format(e))


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

    # hologres数据库
    host = cf.get("hologres-dofun", "host")
    port = cf.get("hologres-dofun", "port")
    database = cf.get("hologres-dofun", "DB")
    user = cf.get("hologres-dofun", "user")
    password = cf.get("hologres-dofun", "password")

    # 实例化Hologres，查询待更新的数据信息
    username, password, host_ip, port, database = user, password, host, int(port), database
    operator_Hologres = OperateHologres(username, password, host_ip, port, database)

    sql = """
    select * 
    from maihaowan.mhw_order
    where part_day = '2022-01-27' limit 2
    
    """
    select_data = operator_Hologres.query_data(sql)
    select_data_index = operator_Hologres.query_data_index()
    operator_Hologres.close_conn()
    print(select_data,'\n',select_data_index)
