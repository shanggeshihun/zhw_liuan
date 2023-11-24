# _*_coding:utf-8 _*_
# @Time     :2023/10/30 11:09
# @Author   : anliu
# @File     :123.py
# @Theme    :捕获报错信息并统一收集到operate_result，数据插入通过executemany

import pymysql, time
import os
import configparser
import warnings

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class OperateDoris():
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
        self.operate_result = {'INSERT':['无','无'],
                               'DELETE': ['无', '无'],
                               'UPDATE': ['无', '无'],
                               'SELECT': ['无', '无'],
                               }

    def close_conn(self):
        self.conn.close()
        self.cursor.close()

    def query_data(self, sql):
        try:
            self.cursor.execute(sql)  # 执行SQL语句
            data = self.cursor.fetchall()  # 取出数据库所有数据，该操作慢
            self.index = self.cursor.description
        except Exception as e:
            self.operate_result['SELECT'] = ['失败', e]
        else:
            self.operate_result['SELECT'] = ['成功','成功']
            return data  # 返回数据

    def query_data_index(self):
        columns_list = []
        index = self.cursor.description
        for tup in index:
            columns_list.append(tup[0])
        return columns_list  # 返回数据

    def update_data(self, sql ,data_list):
        """
        :param data_list:以列表为元素的列表,或者以元组为元素的列表
        :return:
        """
        try:
            self.cursor.executemany(sql,data_list)
            self.cursor.execute('commit')
        except Exception as e:
            self.operate_result['UPDATE'] = ['失败', str(e).replace('\n', '\t')]
            self.conn.rollback()
        else:
            self.operate_result['UPDATE'] = ['成功', '成功']

    def insert_data(self, sql, data_list):
        try:
            self.cursor.executemany(sql,data_list)  # 执行SQL语句
            self.cursor.execute("commit")
        except Exception as e:
            print(e)
            self.operate_result['INSERT'] = ['失败', e]
            self.conn.rollback()
        else:
            self.operate_result['INSERT'] = ['成功', '成功']

    def delete_data(self, sql):
        try:
            self.cursor.execute(sql)  # 执行SQL语句
            self.cursor.execute("commit")
        except Exception as e:
            self.operate_result['DELETE'] =['失败', e]
            self.conn.rollback()
        else:
            self.operate_result['DELETE'] = ['成功', '成功']

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    # ------------------------数据库配置读取----------------------------
    cf = configparser.ConfigParser()
    if cf.read("E:/Onerway/Python/zhw_liuan/PublicConfig/config.ini",'utf-8') == []:
        """服务器模式"""
        cf.read("/home/zhwom/config/config.ini",'utf-8')
    else:
        """本地模式"""
        cf.read("E:/Onerway/Python/zhw_liuan/PublicConfig/config.ini",'utf-8')

    # MySQL连接配置
    doris_host = cf.get("prod_doris", "host")
    doris_user = cf.get("prod_doris", "user")
    doris_password = cf.get("prod_doris", "password")
    doris_database = cf.get("prod_doris", "DB")
    doris_port = cf.get("prod_doris", "port")

    # 实例化mysql
    operate_doris = OperateDoris(
        username=doris_user,
        password=doris_password,
        host_ip=doris_host,
        port=int(doris_port),
        database=doris_database
    )

    doris_sql = """
        select * from dim.dim_time limit 2
    """
    try:
        result_data = operate_doris.query_data(doris_sql)
        result_data_columns = operate_doris.query_data_index()
    except Exception as e:
        operate_doris.close_conn()
        print(e)
    else:
        print(result_data)
        print(result_data_columns)
        operate_doris.close_conn()
    print(operate_doris.operate_result)