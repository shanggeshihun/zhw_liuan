# _*_coding:utf-8 _*_

# @Time      : 2022/5/19  17:17
# @Author    : An
# @File      : AdsStatistics_bak0719.py
# @Software  : 租号玩 撤单扣款提醒数据统计-日维度


import time, datetime, configparser, warnings, math, platform,calendar
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


class Ads:
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

    def zhw_withdraw_charge_warning_popup_day_statistics(self, part_day):
        """
        :param part_day: 日期参数
        :return: 更新日期+用户维度统计数据写到MySQL
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
        # -- 1 重点关注指标
        # 清空昨日数据
        delete_sql = "delete from zhw_withdraw_charge_warning_popup_day_statistics where part_day = '{}'".format(
            part_day)

        operate_mysql.update_data(delete_sql)

        presto_sql = """
            with tmp_popup as (
                select 
                    part_day,
                    count(distinct userid) as tigger_users,
                    count(id) as trigger_times,
                    count(distinct did) as tigger_orders,
                    count(distinct case when opt_type = 1 then userid end) as cancel_complaint_users,
                    count(distinct case when opt_type = 1 then did end) as cancel_complaint_orders,
                    count(distinct case when opt_type = 1 then id end) as cancel_complaint_times
                from hive.zhwlog.zhw_order_game_relief_log 
                where part_day = '{0}' and part_day >='2022-04-29'
                group by 1 
            ) ,
            
            tmp_cancel_complain_order as (
                select a.part_day,sum(b.pm) as cancel_complain_pm 
                from 
                (
                    select part_day,did
                    from hive.zhwlog.zhw_order_game_relief_log 
                    where part_day = '{0}'
                    and opt_type = 1 
                    group by 1,2
                ) a 
                join 
                (
                    select id,pm
                    from hive.zhwdb.zhw_dingdan 
                    where part_day between cast(date_add('day',-10,cast('{0}' as date)) as varchar) and '{0}'
                ) b 
                on a.did = b.id 
                group by 1
            ) 
            
            select a.*,coalesce(b.cancel_complain_pm,0) as cancel_complain_pm
            from tmp_popup a 
            left join tmp_cancel_complain_order b 
            on a.part_day = b.part_day
        """.format(part_day)
        presto_data_list = operate_presto.query_data(presto_sql)

        # presto_data_list 是以list为元素的list(批量插入)
        if presto_data_list:
            # 每次写入50条数据
            step = 50
            length_data = len(presto_data_list)
            r = math.ceil(length_data / step)
            for i in range(r):
                tmp_list = presto_data_list[i * step:(i + 1) * step]
                batch_sql = ','.join([str(tuple(a)) for a in tmp_list])
                insert_sql = "insert into zhw_withdraw_charge_warning_popup_day_statistics(part_day,tigger_users,trigger_times,tigger_orders,cancel_complaint_users,cancel_complaint_orders,cancel_complaint_times,cancel_complain_pm) values {0};".format(
                    batch_sql)
                operate_mysql.insert_data(insert_sql)
        operate_presto.close_conn()

    def zhw_withdraw_charge_warning_popup_month_statistics(self, part_day):
        """
        :param part_day: 日期参数
        :return: 更新日期+用户维度统计数据写到MySQL
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

        the_month_first_day = part_day[:7] + '-01'

        the_year, the_month = int(part_day[:4]), int(part_day[5:7])
        the_month_first_day_date = datetime.date(the_year, the_month, 1)
        week, days_num = calendar.monthrange(the_month_first_day_date.year, the_month_first_day_date.month)
        the_month_last_day_date = datetime.date(the_year, the_month, days_num)
        the_month_last_day = the_month_last_day_date.strftime('%Y-%m-%d')
        print(the_month_first_day, the_month_last_day)



        # -- 1 重点关注指标
        # 清空昨日数据
        delete_sql = "delete from zhw_withdraw_charge_warning_popup_month_statistics where part_month = '{}'".format(
            part_day[:7])

        operate_mysql.update_data(delete_sql)

        presto_sql = """
            with tmp_popup as (
                select 
                    part_month,
                    count(distinct userid) as tigger_users,
                    count(id) as trigger_times,
                    count(distinct did) as tigger_orders,
                    count(distinct case when opt_type = 1 then userid end) as cancel_complaint_users,
                    count(distinct case when opt_type = 1 then did end) as cancel_complaint_orders,
                    count(distinct case when opt_type = 1 then id end) as cancel_complaint_times
                from hive.zhwlog.zhw_order_game_relief_log 
                where  part_day >='2022-04-29'
                and part_day <= '{0}' and part_day >= '{1}'
                group by 1 
            ) ,

            tmp_cancel_complain_order as (
                select a.part_month,sum(b.pm) as cancel_complain_pm 
                from 
                (
                    select part_month,did
                    from hive.zhwlog.zhw_order_game_relief_log 
                    where part_day >='2022-04-29'
                    and part_day <= '{0}' and part_day >= '{1}'
                    and opt_type = 1 
                    group by 1,2
                ) a 
                join 
                (
                    select id,pm
                    from hive.zhwdb.zhw_dingdan 
                    where part_day between cast(date_add('day',-10,cast('{1}' as date)) as varchar) and '{0}'
                ) b 
                on a.did = b.id 
                group by 1
            ) 

            select a.*,coalesce(b.cancel_complain_pm,0) as cancel_complain_pm
            from tmp_popup a 
            left join tmp_cancel_complain_order b 
            on a.part_month = b.part_month
        """.format(the_month_last_day, the_month_first_day)

        presto_data_list = operate_presto.query_data(presto_sql)

        # presto_data_list 是以list为元素的list(批量插入)
        if presto_data_list:
            # 每次写入50条数据
            step = 50
            length_data = len(presto_data_list)
            r = math.ceil(length_data / step)
            for i in range(r):
                tmp_list = presto_data_list[i * step:(i + 1) * step]
                batch_sql = ','.join([str(tuple(a)) for a in tmp_list])
                insert_sql = "insert into zhw_withdraw_charge_warning_popup_month_statistics(part_month,tigger_users,trigger_times,tigger_orders,cancel_complaint_users,cancel_complaint_orders,cancel_complaint_times,cancel_complain_pm) values {0};".format(
                    batch_sql)
                operate_mysql.insert_data(insert_sql)
        operate_presto.close_conn()


if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    for i in range(1, 26):
        start_time = time.time()

        day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
        print('开始跑{}的数据'.format(day_last_1))

        ads  = Ads()
        ads.zhw_withdraw_charge_warning_popup_day_statistics(day_last_1)

        ads = Ads()
        ads.zhw_withdraw_charge_warning_popup_month_statistics(day_last_1)

        end_time = time.time()

        print(i, '运行耗时：%s s' % (end_time - start_time))
