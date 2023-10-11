# _*_coding:utf-8 _*_
# @Time　　 : 2022/6/11 17:18
# @Author　 : liuan
# @File　 　: roi_zcroi_cdroi_dim_month_topgame.py
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

    def reg_regpay_users_dim_month_addfrom_subaddfrom(self, part_month):
        """
        :param part_day: 当前月份参数
        :return: 月份维度
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
            select 
                a.part_month,
                a.lx,a.sub_lx,
                count(a.jkx_userid) as regster_users,
                count(b.userid) as pay_users
            from 
            (
                select 
                part_month,
                case 
                    when jkx_lx=1 then '3-官网'
                    when jkx_lx=6 then '4-客户端'
                    when jkx_lx in (4,5) then '2-M站'
                    when jkx_lx in (2,3,14,15) then '1-APP'
                    else '5-其他' 
                end as lx,
                case 
                    when jkx_lx=2 then '安卓'
                    when jkx_lx=3 then 'ios'
                    when jkx_lx=14 then 'lite'
                    when jkx_lx=15 then 'pro'
                    else 'pro' 
                end as sub_lx,
                jkx_userid 
                from zhwdb.zhw_user 
                where part_month='{0}' 
            )a 
            left join 
            (
                select part_month,userid 
                from zhwdb.zhw_dingdan 
                where part_month='{0}' 
                group by 1,2
            )b 
            on a.jkx_userid=b.userid and a.part_month=b.part_month 
            group by 1,2,3
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.iloc[:, 3:].astype('float')
        df_result.to_excel(r"./zhw_rpt_data/reg_regpay_users_dim_month_addfrom_subaddfrom_{0}.xlsx".format(part_month))


if __name__ == '__main__':
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    part_month_list = ['2022-11', '2021-11']
    for part_month in part_month_list:
        start_time = time.time()
        zhw_analysis = ZhwAnalysis()
        zhw_analysis.reg_regpay_users_dim_month_addfrom_subaddfrom(part_month)
        end_time = time.time()
        print('reg_regpay_users_dim_month_addfrom_subaddfrom，运行耗时：%s s' % (end_time - start_time))
