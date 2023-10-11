# _*_coding:utf-8 _*_

#@Time      : 2022/2/24  16:10
#@Author    : An
#@File      : SchedualToMysql.py
#@Software  : 任务调度信息写入MySQL

import configparser,pymysql
class SchedualInfo:
    def __init__(self):
        '''
        :param username:    数据库用户名
        :param password:    数据库密码
        :param host_ip:     数据库主机IP地址
        :param port:        数据库主机端口
        :param database:   数据库名称
        :param sql:         数据库执行SQL语句
        '''
        # ------------------------数据库配置读取----------------------------
        cf = configparser.ConfigParser()
        if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", 'utf-8') == []:
            """服务器模式"""
            cf.read("/home/zhwom/config/config.ini", 'utf-8')
        else:
            """本地模式"""
            cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", 'utf-8')

        # MySQL连接配置
        self.host_ip = cf.get("Mysql-data_yunying", "host")
        self.username = cf.get("Mysql-data_yunying", "user")
        self.password = cf.get("Mysql-data_yunying", "password")
        self.database = cf.get("Mysql-data_yunying", "DB")
        self.port = cf.get("Mysql-data_yunying", "port")

        self.conn = pymysql.connect(
            host=self.host_ip,
            port=int(self.port),
            user=self.username,
            passwd=self.password,
            db=self.database,
            charset='utf8'
        )
        self.cursor = self.conn.cursor()

    def close_conn(self):
        self.conn.close()
        self.cursor.close()

    def schedual_to_mysql(self,part_day,target_table_name,start_time,end_time,running_seconds,status,info):
        delete_sql =  """
            delete from zhw_luck_shop_schedual_info
            where part_day = '{0}' and target_table_name = '{1}'
        """.format(part_day,target_table_name)
        self.cursor.execute(delete_sql)
        self.cursor.execute("commit")


        sql = """
            insert into zhw_luck_shop_schedual_info (part_day,target_table_name,start_time,end_time,running_seconds,status,info)
            values ('{0}','{1}','{2}','{3}','{4}','{5}','{6}')
        """.format(part_day,target_table_name,start_time,end_time,running_seconds,status,info)

        self.cursor.execute(sql)
        self.cursor.execute("commit")

        self.close_conn()

