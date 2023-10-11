# _*_coding:utf-8 _*_

#@Time      : 2022/1/19  9:19
#@Author    : An
#@File      : zhw_luck_shop_main.py
#@Software  : PyCharm

import sys,os,time,datetime,platform,configparser,warnings

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from WarningEmail import warning_email
from OperateMysql import OperateMysql

warnings.filterwarnings("ignore")
# ------------------------数据库配置读取----------------------------
cf = configparser.ConfigParser()
if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

# 运营数据库
mysql_host = cf.get("Mysql-data_yunying", "host")
mysql_user = cf.get("Mysql-data_yunying", "user")
mysql_password = cf.get("Mysql-data_yunying", "password")
mysql_db = cf.get("Mysql-data_yunying", "DB")
mysql_port = cf.get("Mysql-data_yunying", "port")


today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1

running_flag = "select * from zhw_luck_shop_prizer_type_statistics where part_day = '{}'".format(day_last_1)

i = 0
while i <5:
    # 实例化mysql
    operate_mysql = OperateMysql(
        username=mysql_user,
        password=mysql_password,
        host_ip=mysql_host,
        port=int(mysql_port),
        database=mysql_db
    )
    re = operate_mysql.query_data(running_flag)
    operate_mysql.close_conn()

    plat = platform.system().lower()
    if plat == 'windows':
        execute_script = "python E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/LuckShop/zhw_luck_shop_wxrobot/zhw_luck_shop_daily_report_robot.py>>E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/LuckShop/zhw_luck_shop_wxrobot/zhw_luck_shop_daily_report_robot.log"
    elif plat == 'linux':
        execute_script = "/opt/anaconda3/bin/python3 /work/project/zhw_product/liuan/LuckShop/zhw_luck_shop_wxrobot/zhw_luck_shop_daily_report_robot.py>>/work/project/zhw_product/liuan/LuckShop/zhw_luck_shop_wxrobot/zhw_luck_shop_daily_report_robot.log"
    else:
        sys.exit()

    if re:
        """
        幸运购 数据播报 机器人
        """
        try:
            os.system(execute_script)
        except Exception as e:
            warning_email('zhw_luck_shop_daily_report_robot' + e)
        else:
            warning_email('zhw_luck_shop_daily_report_robot' + '机器人正常播报')
            break
    else:
        warning_email('zhw_luck_shop_daily_report_robot' + '当日数据未写入')
        time.sleep(1200)
    i += 1
if i == 5 :
    warning_email('尝试5次,' + '当日数据未写入')
    sys.exit()