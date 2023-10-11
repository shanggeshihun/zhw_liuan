# _*_coding:utf-8 _*_

# @Time      : 2022/6/13  11:47
# @Author    : An
# @File      : roi_of_last_m_reguser_dim_month_addfrom.py
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

    def roi_of_last_m_reguser_dim_month_addfrom(self, part_month):
        """
        :param part_day: 当前月份参数
        :return: 实名制认证用户数，注册用户数，注册付费用户数
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
                f.part_month as "注册月份",
                f.jkx_lx,
                a.part_month as "付费月份",
                sum(b.add_fee) as "官方加价",
                sum(b.sys_fee) as "订单手续费",
                sum(b.channel_fee) as "渠道手续费",
                sum(c.pay_money) as "二级加价",
                sum(d.channel_money) as "渠道分成",
                sum(e.fx_money+e.fx_channel)  as "分销分成" 
            from 
            (
                select a.part_month,a.jkx_lx,a.jkx_userid 
                from 
                (
                    select part_month,
                    case when jkx_lx=1 then '4-官网'
                         when jkx_lx=2 then '3-安卓'
                         when jkx_lx=3 then '1-ios'
                         when jkx_lx in(4,5) then '2-M站'
                         when jkx_lx=6 then '5-客户端'
                         when jkx_lx=14 then '6-lite'
                         when jkx_lx=15 then '7-pro'
                        else '8-其他' 
                    end as jkx_lx,jkx_userid 
                    from zhwdb.zhw_user 
                    where part_month= to_char(date('{0}' || '-01') - interval '1' month,'yyyy-mm')
                )a
                inner join 
                (
                    select part_month,userid 
                    from zhwdb.zhw_dingdan 
                    where part_month= '{0}'
                    group by 1,2
                )b 
                on a.part_month=to_char(date(b.part_month || '-01') - interval '1' month,'yyyy-mm')  and a.jkx_userid=b.userid 
            )f 
            inner join 
            (
                select part_month,userid,id 
                from zhwdb.zhw_dingdan 
                where part_month= '{0}'
            )a 
            on f.jkx_userid=a.userid 
            inner join 
            (
                select part_month,order_id,sys_fee,channel_fee,add_fee 
                from zhwdb.zhw_dingdan_fee
                where part_month= '{0}'
            )b 
            on a.id=b.order_id and a.part_month=b.part_month
            left join 
            (
                select part_month,order_id,pay_money 
                from zhwdb.zhw_dingdan_channel_addmoney  
                where part_month= '{0}'
            )c 
            on a.id=c.order_id  and a.part_month=c.part_month
            left join 
            (
                select part_month,order_id,channel_money 
                from zhwdb.zhw_dingdan_channel  
                where part_month= '{0}'
            )d 
            on  a.id=d.order_id  and a.part_month=d.part_month
            left join 
            (
                select part_month,order_id,fx_money,fx_channel 
                from zhwdb.zhw_dingdan_fenxiao  
                where part_month= '{0}'
            )e 
            on a.id=e.order_id  and a.part_month=e.part_month
            group by 1,2,3
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.iloc[:,3:].astype('float')
        df_result.to_excel(r"./zhw_rpt_data/roi_of_last_m_reguser_dim_month_addfrom_{0}.xlsx".format(part_month))


if __name__ == '__main__':
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    part_month_list = ['2022-05', '2022-04', '2022-03', '2021-05', '2021-04', '2021-03']
    part_month_list = ['2022-07', '2021-07']
    part_month_list = ['2022-11', '2021-11']
    for part_month in part_month_list:
        start_time = time.time()
        zhw_analysis = ZhwAnalysis()
        zhw_analysis.roi_of_last_m_reguser_dim_month_addfrom(part_month)
        end_time = time.time()
        print('roi_of_last_m_reguser_dim_month_addfrom，运行耗时：%s s' % (end_time - start_time))
