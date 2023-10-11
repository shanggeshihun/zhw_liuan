# _*_coding:utf-8 _*_
# @Time　　 : 2022/6/11 11:51
# @Author　 : liuan
# @File　 　: game_orders_dim_month_game_game.py
# @Theme : PyCharm


# _*_coding:utf-8 _*_

# @Time      : 2022/6/7  12:41
# @Author    : An
# @File      : core_dim_month_newold_order.py
# @Software  : PyCharm


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

    def channel_gmv_dim_month_channel(self, part_month):
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
                a.part_month as dt,
                a.gmv_channel,
                count(distinct a.userid) as dr,
                count(a.id) as dn,
                sum(a.pm) as dpm,-- 全网原始订单金额
                sum(a.pmoney*b.relet_give_hour) as drpm, -- 全网租送赠送订单金额
                sum(c.add_money) as dapm -- 全网二级加价金额
            from 
            (
                select part_month,userid,id,pm,gameid,pmoney,
                     case 
                         when add_from in (1,23,24,12,13,18,25,44,6,2,21,22,30,35,36,45,46,47,70,131,20,29,110,120,121,3,31,4,5) then '①自营渠道'
                         when add_from in (14,40,43,50,51,52) then '②网吧'
                         when add_from in (8,9,10,11) then '③分销'
                         when add_from in (7,15,16,17,19,26,27,28,32,33,34,38,39,41,42,48,49,61,300,301,302,303,304,305,306,307,308,309) then '④api'
                         else '其他渠道'
                     end as gmv_channel
                    --'①自营渠道' as gmv_channel
                from zhwdb.zhw_dingdan 
                where part_month = '{0}'
            )a 
            left join 
            (
                select dingdan_id,relet_give_hour 
                from zhwdb.zhw_dingdan_rent_give
                where part_month = '{0}'
            )b  
            on a.id=b.dingdan_id
            left join 
            (
                select order_id,add_money 
                from zhwdb.zhw_dingdan_channel_addmoney 
                where part_month = '{0}'
            ) c  
            on a.id=c.order_id 
            group by 1,2
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.iloc[:, 2:].astype('float')
        df_result.to_excel(r"./zhw_rpt_data/channel_gmv_dim_month_channel_{0}.xlsx".format(part_month))


if __name__ == '__main__':
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    part_month_list = ['2022-05', '2022-04', '2022-03', '2021-05', '2021-04', '2021-03']
    part_month_list = ['2022-11', '2021-11']
    for part_month in part_month_list:
        start_time = time.time()
        zhw_analysis = ZhwAnalysis()
        zhw_analysis.channel_gmv_dim_month_channel(part_month)
        end_time = time.time()
        print('channel_gmv_dim_month_channel，运行耗时：%s s' % (end_time - start_time))
