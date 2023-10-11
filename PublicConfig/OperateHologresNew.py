# _*_coding:utf-8 _*_
# @Time　　 : 2022/11/22   10:04
# @Author　 : zimo
# @File　   :OperateHologresNew.py
# @Software :PyCharm
# @Theme    :
import pandas as pd
import psycopg2, time
import os
import configparser
import warnings

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class OperateHologresNew():
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
        self.operate_result = {'INSERT': ['无', '无'],
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
            self.operate_result['SELECT'] = ['失败', str(e).replace('\n', '\t')]
        else:
            self.operate_result['SELECT'] = ['成功', '成功']
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

    def insert_data(self, sql, data_list):
        try:
            self.cursor.executemany(sql, data_list)  # 执行SQL语句
            self.cursor.execute("commit")
        except Exception as e:
            self.operate_result['INSERT'] = ['失败', e]
            self.conn.rollback()
        else:
            self.operate_result['INSERT'] = ['成功', '成功']


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    # ------------------------数据库配置读取----------------------------
    cf = configparser.ConfigParser()
    if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
        """服务器模式"""
        cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
    else:
        """本地模式"""
        cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

    # hologres数据库
    host = cf.get("hologres-dofun", "host")
    port = cf.get("hologres-dofun", "port")
    database = cf.get("hologres-dofun", "DB")
    user = cf.get("hologres-dofun", "user")
    password = cf.get("hologres-dofun", "password")

    # 实例化Hologres，查询待更新的数据信息
    username, password, host_ip, port, database = user, password, host, int(port), database
    operator_Hologres = OperateHologresNew(username, password, host_ip, port, database)

    sql = """
    select * 
    from maihaowan.mhw_order
    where part_day = '2022-01-27' limit 2
    
    """
    select_data = operator_Hologres.query_data(sql)
    select_data_index = operator_Hologres.query_data_index()
    operator_Hologres.close_conn()
    df = pd.DataFrame(select_data)
    df.columns = select_data_index
    print(select_data, '\n', select_data_index)
    print(df)
