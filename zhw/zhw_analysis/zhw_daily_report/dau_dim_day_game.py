# _*_coding:utf-8 _*_
# @Time　　 : 2022/6/11 16:01
# @Author　 : liuan
# @File　 　: hotgame_gmv_dim_month.py
# @Theme : PyCharm

import time, datetime, configparser, warnings, math, platform
import sys
import pandas as pd
import pymysql, presto
import pyhive.presto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine

import json, requests

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysql import OperateMysql
from OperatePresto import OperatePresto
from SchedualToMysql import SchedualInfo


class ZhwAnalysis:
    def __init__(self):
        warnings.filterwarnings("ignore")
        # ------------------------数据库配置读取----------------------------
        cf = configparser.ConfigParser()
        if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
            """服务器模式"""
            cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
            os.chdir("/work/project/zhw_product/liuan/zhw/zhw_wxrobot/zhw_quick_hao_robot")
        else:
            """本地模式"""
            cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

        # 运营数据库
        self.mysql_host = cf.get("Mysql-data_yunying", "host")
        self.mysql_user = cf.get("Mysql-data_yunying", "user")
        self.mysql_password = cf.get("Mysql-data_yunying", "password")
        self.mysql_db = cf.get("Mysql-data_yunying", "DB")
        self.mysql_port = cf.get("Mysql-data_yunying", "port")

        # hive数据库
        self.presto_host = cf.get("hive_presto", "host")
        self.presto_username = cf.get("hive_presto", "username")
        self.presto_port = cf.get("hive_presto", "port")
        self.presto_schema = cf.get("hive_presto", "schema")
        self.presto_catalog = cf.get("hive_presto", "catalog")

        # 百度商业账号
        self.bdtj_username = cf.get("bdtj_account_info", "username")
        self.bdtj_password = cf.get("bdtj_account_info", "password")
        self.bdtj_token = cf.get("bdtj_account_info", "token")

    def dau_dim_day_game(self, start_day, end_day):
        """
        :param part_month: 当前月份参数
        :return:
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )
        cnx_mysql = create_engine(
            "mysql+pymysql://" + self.mysql_user + ":" + self.mysql_password + "@" + self.mysql_host + ":" + self.mysql_port + "/" + self.mysql_db,
            echo=False)

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )
        con_presto = pypresto.connect(host=self.presto_host, port=self.presto_port, username=self.presto_username,
                                      schema=self.presto_schema, catalog=self.presto_catalog)

        presto_sql = """
            select a.part_day,b.gid,max(c.title) as game_name,count(distinct a.jkx_userid)  as dau
            from 
            (
                select jkx_userid,hid,part_day
                from hive.behavior.zhw_user_behavior
                where part_day between '{0}' and '{1}'
                and hid is not null
                -- group by 1,2,3
            ) a  
            inner join 
            (
                select id,gid 
                from hive.zhwdb.zhw_hao
                where gid in (443 ,683 ,560 ,446 ,699 ,1088 ,698 ,1028 ,636 ,449 ,709 ,988,    11 ,17 ,581 ,1072 ,24 ,582 ,761 ,272 ,731 ,441 ,563 ,25 ,22)
            ) b 
            on a.hid=cast(b.id as varchar)
            inner join 
            (
                select id ,title
                from hive.zhwdb.zhw_game_info 
                where id in (443 ,683 ,560 ,446 ,699 ,1088 ,698 ,1028 ,636 ,449 ,709 ,988,    11 ,17 ,581 ,1072 ,24 ,582 ,761 ,272 ,731 ,441 ,563 ,25 ,22)
            ) c 
            on b.gid=c.id 	
            group by 1,2
        """.format(start_day,end_day)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.iloc[:, 3:].astype('float')
        df_result.to_excel(r"./daily_export_data/dau_dim_day_game_{0}_to_{1}.xlsx".format(start_day, end_day))


if __name__ == '__main__':
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    for i in range(16,19):
        day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1

        start_day, end_day = day_last_1, day_last_1
        start_time = time.time()
        zhw_analysis = ZhwAnalysis()
        zhw_analysis.dau_dim_day_game(start_day, end_day)
        end_time = time.time()
        print('dau_dim_day_game，运行耗时：%s s' % (end_time - start_time))
        time.sleep(2)