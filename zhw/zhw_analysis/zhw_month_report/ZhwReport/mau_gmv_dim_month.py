# _*_coding:utf-8 _*_
# @Time　　 : 2022/6/11 0:55
# @Author　 : liuan
# @File　 　: mau_gmv_dim_month.py
# @Theme : mau,gmv,撤单gmv，结算gmv；新用户gmv，新用户撤单gmv，新用户结算gmv

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

    # 当日大R用户（GMV>=1000）用户历史抽奖毛利润
    def mau_gmv_dim_month(self, part_month):
        """
        :param part_day: 月份参数
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
                a.dt as "月份",
                a.hurs as "活跃用户数",
                b.dr as "付费人数",
                b.dn as "订单数量",
                b.dpm as "原始订单金额",
                b.drpm as "租送金额",
                b.dapm as "二级加价金额",
                e.dr as "充值人数",
                e.dn as "充值数量",
                e.drm as "充值金额",
                c.dr as "正常结算-付费人数",
                c.dn as "正常结算-订单数量",
                c.dpm as "正常结算-原始订单金额",
                c.drpm as "正常结算-租送金额",
                c.dapm as "正常结算-加价金额",
                d.dr as "撤单-付费人数",
                d.dn as "撤单-订单数量",
                d.dpm as "撤单-原始订单金额",
                d.drpm as "撤单-租送金额",
                d.dapm as "撤单-二次加价金额",
                f.zr as "新注册用户数",
                g.ndr as "新用户-付费人数",
                g.ndn as "新用户-单量",
                g.ndpm as "新用户-原始订单金额",
                g.ndrpm as "新用户-租送金额",
                g.ndapm as "新用户-二级加价金额",
                h.nr as "新用户-充值人数",
                h.ni as "新用户-充值数量",
                h.nm as "新用户-充值金额",
                i.ndr as "新用户-正常结算-付费人数",
                i.ndn as "新用户-正常结算-订单数量",
                i.ndpm as "新用户-正常结算-原始订单金额",
                i.ndrpm as "新用户-正常结算-租送金额",
                i.ndapm as "新用户-正常结算-二级加价金额",
                j.ndr as "新用户-撤单-付费人数",
                j.ndn as "新用户-撤单-订单数量",
                j.ndpm as "新用户-撤单-原始订单金额",
                j.ndrpm as "新用户-撤单-租送金额",
                j.ndapm as "新用户-撤单-二级加价金额"
            from 
            ------全网用户
            /*每个月的活跃用户数*/
            (
                select 
                part_month as dt,
                count(distinct userid) as hurs 
                from zhwdb.zhw_user_login_log_extend  
                where part_month='{0}'
                group by 1
            )a 
            inner join 
            /*每个月的订单数量/订单人数/订单金额/租送/二级加价金额-全部订单*/
            (
                select a.part_month as dt,
                count(distinct a.userid) as dr,
                count(a.id) as dn,
                sum(a.pm) as dpm,
                sum(a.pmoney*b.relet_give_hour) as drpm,
                sum(c.add_money) as dapm 
                from 
                (
                    select part_month,userid,id,pm,gameid,pmoney 
                    from zhwdb.zhw_dingdan 
                    where part_month='{0}'
                )a 
                left join 
                (
                    select dingdan_id,relet_give_hour
                    from zhwdb.zhw_dingdan_rent_give
                    where part_month='{0}'
                ) b  
                on a.id=b.dingdan_id
                left join 
                (
                    select order_id,add_money 
                    from zhwdb.zhw_dingdan_channel_addmoney 
                    where part_month='{0}' 
                )c  
                on a.id=c.order_id 
                group by 1
            )b 
            on a.dt=b.dt 
            inner join 
            /*每月的充值金额*/
            (
                select 
                part_month as dt,
                count(distinct username) dr,
                count(id) as dn,
                sum(money) as drm
                from zhwdb.zhw_recharge 
                where part_month='{0}' 
                and  status=2
                group by 1
            )e 
            on a.dt=e.dt   
            inner join 
            /*每个月的订单数量/订单人数/订单金额/租送/二级加价金额-正常结算订单*/
            (
                select a.part_month as dt,
                count(distinct a.userid) as dr,
                count(a.id) as dn,
                sum(a.pm) as dpm,
                sum(a.pmoney*b.relet_give_hour) as drpm,
                sum(c.add_money) as dapm from 
                (
                    select part_month,userid,id,pm,gameid,pmoney 
                    from zhwdb.zhw_dingdan 
                    where part_month='{0}' 
                    and zt=2
                )a 
                left join 
                (
                    select dingdan_id,relet_give_hour 
                    from zhwdb.zhw_dingdan_rent_give
                    where part_month='{0}' 
                )b  
                on a.id=b.dingdan_id
                left join 
                (
                    select order_id,add_money 
                    from zhwdb.zhw_dingdan_channel_addmoney
                    where part_month='{0}' 		
                )c  on a.id=c.order_id 
                group by 1
            )c 
            on a.dt=c.dt 
            inner join 
            /*每个月的订单数量/订单人数/订单金额/租送/二级加价金额-撤单*/
            (
                select a.part_month as dt,
                count(distinct a.userid) as dr,
                count(a.id) as dn,
                sum(a.pm) as dpm,
                sum(a.pmoney*b.relet_give_hour) as drpm,
                sum(c.add_money) as dapm from 
                (
                    select part_month,userid,id,pm,gameid,pmoney 
                    from zhwdb.zhw_dingdan 
                    where part_month='{0}' 
                    and zt in(3,4)
                ) a 
                left join 
                (
                    select dingdan_id,relet_give_hour 
                    from zhwdb.zhw_dingdan_rent_give
                    where part_month='{0}' 
                )b  
                on a.id=b.dingdan_id
                left join 
                (
                    select order_id,add_money 
                    from zhwdb.zhw_dingdan_channel_addmoney 
                    where part_month='{0}' 
                )c  on a.id=c.order_id 
                group by 1
            )d 
            on a.dt=d.dt  
            inner join 
            --------新用户
            /*新用户每个月的注册用户数*/
            (
                select 
                part_month as dt,
                count(distinct jkx_userid)  as zr
                from zhwdb.zhw_user 
                where part_month='{0}' 
                group by 1
            ) f  
            on a.dt=f.dt 
            inner join 
            /*新用户每个月的付费人数、订单数量、订单金额--全部订单*/
            (
                select 
                a.part_month as dt,
                count(distinct a.userid) as ndr,
                count(a.id) as ndn,
                sum(a.pm) as ndpm,
                sum(a.pmoney*b.relet_give_hour) as ndrpm,
                sum(c.add_money) as ndapm from 
                (
                    select part_month,jkx_userid  
                    from zhwdb.zhw_user 
                    where part_month='{0}'  
                    group by 1,2
                )d  
                inner join 
                (
                    select part_month,userid,id,pm,pmoney
                    from zhwdb.zhw_dingdan
                    where part_month='{0}' 
                )a  
                on d.jkx_userid=a.userid and d.part_month=a.part_month
                left join 
                (
                    select dingdan_id,relet_give_hour 
                    from zhwdb.zhw_dingdan_rent_give
                    where part_month='{0}' 
                )b  on a.id=b.dingdan_id
                left join 
                (
                    select order_id,add_money 
                    from zhwdb.zhw_dingdan_channel_addmoney
                    where part_month='{0}' 
                )c  on a.id=c.order_id 
                group by 1
            )g 
            on a.dt=g.dt 
            inner join 
            /*新用户充值*/
            (
                select 
                b.part_month as dt,
                count(distinct b.username) as nr,
                count(b.id) as ni,
                sum(b.money)  as nm
                from 
                (
                    select part_month,jkx_userid  
                    from zhwdb.zhw_user 
                    where part_month='{0}' 
                    group by 1,2
                )a
                inner join 
                (
                    select part_month,username,id,money 
                    from zhwdb.zhw_recharge 
                    where part_month='{0}' 
                    and status=2
                )b 
                on a.part_month=b.part_month and a.jkx_userid=b.username 
                group by 1
            )h 
            on a.dt=h.dt 
            inner join 
            /*新用户每个月的付费人数、订单数量、订单金额--正常结算*/
            (
                select 
                a.part_month as dt,
                count(distinct a.userid) as ndr,
                count(a.id) as ndn,
                sum(a.pm) as ndpm,
                sum(a.pmoney*b.relet_give_hour) as ndrpm,
                sum(c.add_money) as ndapm from 
                (
                    select part_month,jkx_userid  
                    from zhwdb.zhw_user
                    where part_month='{0}'  
                    group by 1,2
                )d  
                inner join 
                (
                    select part_month,userid,id,pm,pmoney 
                    from zhwdb.zhw_dingdan 
                    where part_month='{0}' 
                    and zt=2 
                )a  
                on d.jkx_userid=a.userid  and d.part_month=a.part_month
                left join 
                (
                    select dingdan_id,relet_give_hour 
                    from zhwdb.zhw_dingdan_rent_give
                    where part_month='{0}'
                )b 
                on a.id=b.dingdan_id
                left join 
                (
                    select order_id,add_money 
                    from zhwdb.zhw_dingdan_channel_addmoney
                    where part_month='{0}'
                )c  on a.id=c.order_id 
                group by 1
            )i 
            on a.dt=i.dt 
            inner join 
            /*新用户每个月的付费人数、订单数量、订单金额---撤单*/
            (
                select 
                a.part_month as dt,
                count(distinct a.userid) as ndr,
                count(a.id) as ndn,
                sum(a.pm) as ndpm,
                sum(a.pmoney*b.relet_give_hour) as ndrpm,
                sum(c.add_money) as ndapm from 
                (
                    select part_month,jkx_userid  
                    from zhwdb.zhw_user 
                    where part_month='{0}' group by 1,2
                )d  
                inner join 
                (
                    select part_month,userid,id,pm,pmoney 
                    from zhwdb.zhw_dingdan 
                    where part_month='{0}' and zt not in(2)
                )a  
                on d.jkx_userid=a.userid  and d.part_month=a.part_month
                left join 
                (
                    select dingdan_id,relet_give_hour 
                    from zhwdb.zhw_dingdan_rent_give
                    where part_month='{0}'
                )b  
                on a.id=b.dingdan_id
                left join 
                (
                    select order_id,add_money 
                    from zhwdb.zhw_dingdan_channel_addmoney
                    where part_month='{0}'
                )c  
                on a.id=c.order_id 
                group by 1
            )j 
            on a.dt=j.dt  
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.iloc[:, 1:].astype('float')
        df_result.to_excel(r"./zhw_rpt_data/mau_gmv_dim_month_{}.xlsx".format(part_month))


if __name__ == '__main__':
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    part_month_list= ['2021-01' ,'2021-02' ,'2021-03' ,'2021-04' ,'2021-05' ,'2021-06' ,'2021-07' ,'2021-08' ,'2021-09' ,'2021-10' ,'2021-11' ,'2021-12'
]
    part_month_list = ['2022-11', '2021-11']
    for part_month in part_month_list:

        start_time = time.time()
        zhw_analysis = ZhwAnalysis()
        zhw_analysis.mau_gmv_dim_month(part_month)
        end_time = time.time()
        print('mau_gmv_dim_month，运行耗时：%s s' % (end_time - start_time))
