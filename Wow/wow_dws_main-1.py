# _*_coding:utf-8 _*_

# @Time      : 2022/11/15  19:45
# @Author    : An
# @File      : wow_dws_main-1.py
# @Software  : PyCharm


import sys, os, time, datetime, platform
import configparser, pymysql, time

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/Wow/wow_dws")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
    sys.path.append("/work/project/zhw_product/liuan/Wow/wow_dws")
else:
    sys.exit()

from OperateMysqlNew import OperateMysqlNew

class TaskSchedual:
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

        # 运营数据库
        self.mysql_host = cf.get("Mysql-data_yunying", "host")
        self.mysql_user = cf.get("Mysql-data_yunying", "user")
        self.mysql_password = cf.get("Mysql-data_yunying", "password")
        self.mysql_db = cf.get("Mysql-data_yunying", "DB")
        self.mysql_port = cf.get("Mysql-data_yunying", "port")

    def run_task(self, class_instance, class_method, part_day):
        """
        :param class_instance: 实例化的名称
        :param class_method: 示例类的方法名称
        :param part_day: 参数
        :return: 执行结果写入MySQL
        """
        self.class_instance = class_instance
        self.class_method = class_method
        self.part_day = part_day

        self.start_time = time.time()
        exec("{0}.{1}('{2}')".format(class_instance, class_method, part_day))
        self.final_result_log = eval("{0}.final_result_log".format(class_instance))
        self.end_time = time.time()
        self.running_seconds = self.end_time - self.start_time
        self.result_todb()

    def result_todb(self):
        if 'real' in self.class_method:
            task_type_ = '近实时'
            delete_sql = """
                delete from diy_task_schedual_info
                where part_day = '{0}' and task_type = '近实时' and target_table_name = '{1}'
            """.format(self.part_day, self.target_table_name)
            self.cursor.execute(delete_sql)
            self.cursor.execute("commit")
        else:
            task_type_ = '离线'

        part_day_ = self.part_day
        task_name_ = self.class_instance
        target_table_name_ = self.class_instance.replace("_real", "")
        start_time_ = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.start_time))
        end_time_ = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.end_time))
        running_seconds_ = self.running_seconds
        status_ = '成功'
        info_ = self.final_result_log

        for k, v in info_.items():
            for k1,v1 in v.items():
                if v1[0] == '失败':
                    status = '失败'

        data_list =[[part_day_, task_name_, task_type_, target_table_name_, start_time_, end_time_, running_seconds_, status_,str(info_)]]

        sql = """
            insert into diy_task_schedual_info (part_day,task_name,task_type,target_table_name,start_time,end_time,running_seconds,status,info)
            values ({0});
        """.format(','.join(['%s'] * 9))

        # 实例化mysql
        operate_mysql = OperateMysqlNew(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        operate_mysql.insert_data(sql, data_list)
        operate_mysql.close_conn()

from wow_dws_users_by_day import WowDwsUsersByDay

ts = TaskSchedual()

wow_dws_users_by_day = WowDwsUsersByDay()
class_instance, class_method, part_day = 'wow_dws_users_by_day', 'run', '2022-11-11'
ts.run_task(class_instance, class_method, part_day)
