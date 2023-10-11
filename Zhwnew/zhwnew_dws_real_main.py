# _*_coding:utf-8 _*_

# @Time      : 2022/11/22  19:00
# @Author    : An
# @File      : zhwnew_dws_main.py
# @Software  : PyCharm


import sys, os, time, datetime, platform
import configparser, pymysql, time

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/Wow/zhwnew_dws")
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/Wow/zhwnew_dwm")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
    sys.path.append("/work/project/zhw_product/liuan/Wow/zhwnew_dws")
    sys.path.append("/work/project/zhw_product/liuan/Wow/zhwnew_dwm")
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

    def run_task(self, class_instance, class_method, start_day, end_day):
        """
        :param class_instance: 实例化的名称
        :param class_method: 示例类的方法名称
        :param start_day: 参数
        :param end_day: 参数
        :return: 执行结果写入MySQL
        """
        self.class_instance = class_instance
        self.class_method = class_method
        self.start_day = start_day
        self.end_day = end_day

        self.start_time = time.time()
        exec("{0}.{1}('{2}','{3}')".format(self.class_instance, self.class_method, self.start_day, self.end_day))
        self.final_result_log = eval("{0}.final_result_log".format(self.class_instance))
        self.end_time = time.time()
        self.running_seconds = self.end_time - self.start_time
        self.result_todb()

    def result_todb(self):
        today = (datetime.datetime.now()).strftime('%Y-%m-%d')
        if 'real' in self.class_method:
            task_type_ = '近实时'
            delete_sql = """
                delete from diy_task_schedual_info
                where run_day = '{0}' and task_type = '近实时' and target_table_name = '{1}'
            """.format(today, self.target_table_name)
            self.cursor.execute(delete_sql)
            self.cursor.execute("commit")
        else:
            task_type_ = '离线'
        run_day_ = today
        part_day_ = self.start_day
        task_name_ = self.class_instance
        target_table_name_ = self.class_instance.replace("_real", "")
        start_time_ = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.start_time))
        end_time_ = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.end_time))
        running_seconds_ = self.running_seconds
        status_ = '成功'
        info_ = self.final_result_log

        for k, v in info_.items():
            for k1, v1 in v.items():
                if v1[0] == '失败':
                    status_ = '失败'

        data_list = [
            [run_day_, part_day_, task_name_, task_type_, target_table_name_, start_time_, end_time_, running_seconds_,
             status_, str(info_)]]
        columns = 10
        sql = """
            insert into diy_task_schedual_info (run_day,part_day,task_name,task_type,target_table_name,start_time,end_time,running_seconds,status,info)
            values ({0});
        """.format(','.join(['%s'] * columns))

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


from wow_dws_rechage_after_reg_byaddday_gapdays import WowDwsRechargeAfterRegByAdddayGapdays

from wow_dws_login_after_reg_byloginday_gapdays import WowDwsLoginAfterRegByALogindayGapdays

from wow_dws_cost_by_sendday_isreg import WowDwsCostBySenddayIsreg

from wow_dws_cost_by_applysendday_isreg import WowDwsCostByApplysenddayIsreg

from wow_dws_rechage_combination_by_payday import WowDwsRechargeCominationByPayday

from wow_dws_advertising_by_day_adid import WowDwsAdvertisingByDayAdid

from wow_dws_measures_by_regdate_gapdays import WowDwsMeasuresByRegdateGapdays


current_time = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
last_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1

print('脚本运行时间：{}'.format(current_time))
print('\t重跑今日和昨日数据')

lst = [0]
for i in lst:
    today_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
    last_day = (datetime.datetime.now() - datetime.timedelta(days=i+1)).strftime('%Y-%m-%d')
    last_8day = (datetime.datetime.now() - datetime.timedelta(days=i + 8)).strftime('%Y-%m-%d')
    last_15day = (datetime.datetime.now() - datetime.timedelta(days=i + 15)).strftime('%Y-%m-%d')

    print('\t正在更新T+0 {}的数据'.format(today_day))

    j = 1

    ts = TaskSchedual()
    wow_dws_rechage_after_reg_byaddday_gapdays = WowDwsRechargeAfterRegByAdddayGapdays()
    class_instance, class_method, start_day ,end_day = 'wow_dws_rechage_after_reg_byaddday_gapdays', 'run', last_day ,today_day
    ts.run_task(class_instance, class_method, start_day ,end_day)
    time.sleep(1)
    print('\t',j, '\t wow_dws_rechage_after_reg_byaddday_gapdays')

    j += 1
    ts = TaskSchedual()
    wow_dws_login_after_reg_byloginday_gapdays = WowDwsLoginAfterRegByALogindayGapdays()
    class_instance, class_method, start_day ,end_day = 'wow_dws_login_after_reg_byloginday_gapdays', 'run', last_day ,today_day
    ts.run_task(class_instance, class_method, start_day ,end_day)
    time.sleep(1)
    print('\t',j, '\t wow_dws_login_after_reg_byloginday_gapdays')

    j += 1
    ts = TaskSchedual()
    wow_dws_cost_by_sendday_isreg = WowDwsCostBySenddayIsreg()
    class_instance, class_method, start_day ,end_day = 'wow_dws_cost_by_sendday_isreg', 'run', last_day ,today_day
    ts.run_task(class_instance, class_method, start_day ,end_day)
    time.sleep(1)
    print('\t',j, '\t wow_dws_cost_by_sendday_isreg')

    j += 1
    ts = TaskSchedual()
    wow_dws_cost_by_applysendday_isreg = WowDwsCostByApplysenddayIsreg()
    class_instance, class_method, start_day ,end_day = 'wow_dws_cost_by_applysendday_isreg', 'run', last_day ,today_day
    ts.run_task(class_instance, class_method, start_day ,end_day)
    time.sleep(1)
    print('\t',j, '\t wow_dws_cost_by_applysendday_isreg')

    j += 1
    ts = TaskSchedual()
    wow_dws_rechage_combination_by_payday = WowDwsRechargeCominationByPayday()
    class_instance, class_method, start_day ,end_day = 'wow_dws_rechage_combination_by_payday', 'run', last_day ,today_day
    ts.run_task(class_instance, class_method, start_day ,end_day)
    print('\t',j, '\t wow_dws_rechage_combination_by_payday')

    j += 1
    ts = TaskSchedual()
    wow_dws_advertising_by_day_adid = WowDwsAdvertisingByDayAdid()
    class_instance, class_method, start_day ,end_day = 'wow_dws_advertising_by_day_adid', 'run', last_day ,today_day
    ts.run_task(class_instance, class_method, start_day ,end_day)
    print('\t',j, '\t wow_dws_advertising_by_day_adid')

    j += 1
    ts = TaskSchedual()
    wow_dws_measures_by_regdate_gapdays = WowDwsMeasuresByRegdateGapdays()
    class_instance, class_method, start_day ,end_day = 'wow_dws_measures_by_regdate_gapdays', 'run', last_day ,today_day
    ts.run_task(class_instance, class_method, start_day ,end_day)
    print('\t',j, '\t wow_dws_measures_by_regdate_gapdays')