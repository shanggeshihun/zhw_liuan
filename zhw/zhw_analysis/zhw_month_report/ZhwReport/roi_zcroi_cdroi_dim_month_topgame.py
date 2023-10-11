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

    def roi_zcroi_cdroi_dim_month_topgame(self, part_month):
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
            select 
                a.part_month as "月份",
                f.title as "游戏",
                -- 全部
                sum(b.add_fee) as "官方加价",
                sum(b.sys_fee) as "订单手续费",
                sum(b.channel_fee) as "渠道手续费",
                sum(c.pay_money) as "二级加价",
                sum(d.channel_money) as "渠道分成",
                sum(e.fx_money+e.fx_channel)  as "分销分成",
                -- 正常结算
                sum(case when a.zt = 2 then b.add_fee end) as "正结_官方加价",
                sum(case when a.zt = 2 then b.sys_fee end) as "正结_订单手续费",
                sum(case when a.zt = 2 then b.channel_fee end) as "正结_渠道手续费",
                sum(case when a.zt = 2 then c.pay_money end) as "正结_二级加价",
                sum(case when a.zt = 2 then d.channel_money end) as "正结_渠道分成",
                sum(case when a.zt = 2 then e.fx_money+e.fx_channel end)  as "正结_分销分成",
                -- 撤单
                sum(case when a.zt in (3,4) then b.add_fee end) as "撤单_官方加价",
                sum(case when a.zt in (3,4) then b.sys_fee end) as "撤单_订单手续费",
                sum(case when a.zt in (3,4) then b.channel_fee end) as "撤单_渠道手续费",
                sum(case when a.zt in (3,4) then c.pay_money end) as "撤单_二级加价",
                sum(case when a.zt in (3,4) then d.channel_money end) as "撤单_渠道分成",
                sum(case when a.zt in (3,4) then e.fx_money+e.fx_channel end)  as "撤单_分销分成"
            from 
            (
                select part_month,id,zt,gameid 
                from zhwdb.zhw_dingdan 
                where part_month='{0}' 
                and gameid in (11,17,581,24,443,683,560,446)
            )a 
            inner join 
            (
                select id,categoryid,title
                from zhwdb.zhw_game_info 
            ) f 
            on a.gameid=f.id 
            inner join 
            (
                select part_month,order_id,sys_fee,channel_fee,add_fee 
                from zhwdb.zhw_dingdan_fee 
                where part_month='{0}'
            )b 
            on  a.id=b.order_id 
            left join 
            (
                select part_month,order_id,pay_money 
                from zhwdb.zhw_dingdan_channel_addmoney 
                where part_month='{0}'
            )c 
            on a.id=c.order_id 
            left join 
            (
                select part_month,order_id,channel_money 
                from zhwdb.zhw_dingdan_channel  
                where part_month='{0}'
            )d 
            on  a.id=d.order_id 
            left join 
            (
                select part_month,order_id,fx_money,fx_channel 
                from zhwdb.zhw_dingdan_fenxiao  
                where part_month='{0}'
            )e 
            on a.id=e.order_id 
            group by 1,2
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.iloc[:,2:].astype('float')
        df_result.to_excel(r"./zhw_rpt_data/roi_zcroi_cdroi_dim_month_topgame_{0}.xlsx".format(part_month))


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
        zhw_analysis.roi_zcroi_cdroi_dim_month_topgame(part_month)
        end_time = time.time()
        print('roi_zcroi_cdroi_dim_month_topgame，运行耗时：%s s' % (end_time - start_time))
