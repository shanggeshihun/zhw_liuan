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

    # 当日大R用户（GMV>=1000）用户历史抽奖毛利润
    def core_dim_month_newold_order(self, part_month):
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
        df_result.to_csv(r"./export_data/core_dim_month_newold_order_{}.csv".format(part_month))

    def os_mau(self, start_month, end_month):
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
                part_month,
                count(distinct case when lxfl in(1,7,8,9) then userid end ) as "a-PC-MAU",
                count(distinct case when lxfl in(2,3,4,5,6) then userid end ) as "b-移动-MAU",
                count(distinct case when lxfl=10 then userid end ) as "其他-MAU",
                count(distinct case when lxfl=1 then userid end ) as "c-官网-MAU",
                count(distinct case when lxfl in(7,8,9) then userid end ) as "d-客户端-MAU",
                count(distinct case when lxfl=7 then userid end ) as "e-官网客户端-MAU",
                count(distinct case when lxfl=9 then userid end ) as "f-网吧客户端-MAU",
                count(distinct case when lxfl=8 then userid end ) as "g-市场客户端-MAU",
                count(distinct case when lxfl in(3,4,5,6) then userid end ) as "h-app-MAU",
                count(distinct case when lxfl=2 then userid end ) as "i-M站-MAU",
                count(distinct case when lxfl=3 then userid end ) as "j-app安卓-MAU",
                count(distinct case when lxfl=6 then userid end ) as "k-app苹果-MAU",
                count(distinct case when lxfl=4 then userid end ) as "l-applite-MAU",
                count(distinct case when lxfl=5 then userid end ) as "m-apppro-MAU"
            from hive.zhwdb.zhw_user_login_log_extend 
            where part_month between '{0}' and '{1}' 
            group by 1
        """.format(start_month, end_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns
        df_result.to_csv(r"./export_data/os_mau_{0}_to{1}.csv".format(start_month, end_month))

    def clv_users(self, save_date_list):
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
        save_date_concat = ','.join(["'" + s + "'" for s in save_date_list])
        presto_sql = """
            select save_date,life_type_id,count(*)  as users
            from hive.zhwdb_dw.label_user_life_type_dist 
            where save_date in ({0})
            group by 1,2
        """.format(save_date_concat)
        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/clv_users_{0}_to{1}.csv".format(min(save_date_list), max(save_date_list)))


    def login_user_retain(self, part_month):
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
                '{0}' as cur_login_month,
                count(b.userid) as last_and_cur_m_login_users,
                count(a.userid) as last_m_login_users
            from 
            (
                select userid
                from zhwdb.zhw_user_login_log_extend 
                where part_month = to_char(date('{0}' || '-01') - interval '1' month,'yyyy-mm')
                group by 1 
            ) a 
            left join 
            (
                select userid 
                from zhwdb.zhw_user_login_log_extend 
                where part_month='{0}'
                group by 1
            ) b 
            on a.userid=b.userid
            group by 1
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/login_user_retain_{0}.csv".format(part_month))


    def login_new_user_retain(self, part_month):
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
                '{0}' as cur_login_month,
                count(a.jkx_userid) as last_m_reg_users,
                count(b.userid) as last_reg_cur_m_login_users
            from 
            (
                select jkx_userid 
                from zhwdb.zhw_user 
                where part_month = to_char(date('{0}' || '-01') - interval '1' month,'yyyy-mm')
                group by 1 
            ) a 
            left join 
            (
                select userid 
                from zhwdb.zhw_user_login_log_extend 
                where part_month='{0}'
                group by 1
            ) b 
            on a.jkx_userid=b.userid
            group by 1
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/login_new_user_retain_{0}.csv".format(part_month))


    def login_old_user_retain(self, part_month):
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
                '{0}' as cur_login_month,
                count(aa.userid) as last_m_login_old_users,
                count(bb.userid) as last_cur_m_login_old_users
            from 
            (
                select 
                    b.userid
                from 
                (
                    select userid
                    from zhwdb.zhw_user_login_log_extend 
                    where part_month= to_char(date('{0}' || '-01') - interval '1' month,'yyyy-mm')
                    group by 1
                ) b 
                left join 
                (
                    select jkx_userid 
                    from zhwdb.zhw_user 
                    where part_month= to_char(date('{0}' || '-01') - interval '1' month,'yyyy-mm')
                    group by 1 
                ) a 
                on a.jkx_userid=b.userid 
                where a.jkx_userid is null 
            ) aa 
            left join 
            (
                select userid
                from zhwdb.zhw_user_login_log_extend 
                where part_month= '{0}'
                group by 1
            ) bb
            on aa.userid = bb.userid
            group by 1
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/login_old_user_retain_{0}.csv".format(part_month))

    def reg_user_r1_retain(self, part_month):
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
                '{0}' as reg_month,
                count(a.jkx_userid) as reg_users,
                count(b.userid) as r1_retain_users
            from 
            (
                select jkx_userid,date(jkx_timer) as t 
                from zhwdb.zhw_user 
                where part_month='{0}'
            )a 
            left join 
            (
                select userid,date(usertimer)  as t 
                from zhwdb.zhw_user_login_log_extend 
                where part_month>='{0}'  and part_month <=to_char(date('{0}' || '-01') + interval '1' month,'yyyy-mm')
                group by 1,2
            )b 
            on a.jkx_userid=b.userid 
            and b.t=a.t+interval '1' day
            group by 1
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/reg_user_r1_retain_{0}.csv".format(part_month))


    def game_orders(self, part_month_list):
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

        part_month_concat = ','.join(["'" + s + "'" for s in part_month_list])

        presto_sql = """
            select a.part_month,a.gameid,b.title as game_name,a.order_users
            from 
            (
                select part_month,gameid,count(distinct userid)  as order_users
                from zhwdb.zhw_dingdan 
                where part_month in ({0})
                and gameid in(11,17,581,24,443,446,560,683)
                group by 1,2
            ) a 
            join zhwdb.zhw_game_info b 
            on a.gameid = b.id 
        """.format(part_month_concat)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/game_orders_{0}_to_{1}.csv".format(min(part_month_list),max(part_month_list)))

    def channel_gmv(self, part_month_list):
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

        part_month_concat = ','.join(["'" + s + "'" for s in part_month_list])

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
                where part_month in ({0}) 
            )a 
            left join 
            (
                select dingdan_id,relet_give_hour 
                from zhwdb.zhw_dingdan_rent_give
                where part_month in ({0}) 
            )b  
            on a.id=b.dingdan_id
            left join 
            (
                select order_id,add_money 
                from zhwdb.zhw_dingdan_channel_addmoney 
                where part_month in ({0}) 
            ) c  
            on a.id=c.order_id 
            group by 1,2
        """.format(part_month_concat)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/channel_gmv_{0}_to_{1}.csv".format(min(part_month_list),max(part_month_list)))

    def withdraw_order_ts_by_tslb(self, part_month_list):
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

        part_month_concat = ','.join(["'" + s + "'" for s in part_month_list])

        presto_sql = """
            select a.part_month,b.lb,count(b.did) 
            from 
            (
                select part_month,id 
                from zhwdb.zhw_dingdan 
                where part_month in ({0}) and zt=3
            )a
            inner join 
            (
                select part_month,did,lb 
                from zhwdb.zhw_ts 
                where part_month in ({0})
            ) b 
            on a.id=b.did 
            group by 1,2
        """.format(part_month_concat)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/withdraw_order_ts_by_tslb_{0}_to_{1}.csv".format(min(part_month_list),max(part_month_list)))


    def withdraw_order_ts_lb_eq1_by_tslx(self, part_month_list):
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

        part_month_concat = ','.join(["'" + s + "'" for s in part_month_list])

        presto_sql = """
            select a.part_month,b.lb,count(b.did) 
            from 
            (
                select part_month,id 
                from zhwdb.zhw_dingdan 
                where part_month in ({0}) and zt=3
            )a
            inner join 
            (
                select part_month,did,lb 
                from zhwdb.zhw_ts 
                where part_month in ({0})
            ) b 
            on a.id=b.did 
            group by 1,2
        """.format(part_month_concat)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/withdraw_order_ts_lb_eq1_by_tslx_{0}_to_{1}.csv".format(min(part_month_list),max(part_month_list)))

    def game_category_ifhot(self, part_day):
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
                case 
                    when state=1 then '开启'
                    when state=0 then '关闭租赁'
                    else '其他'
                end as game_status,
                case 
                    when categoryid = 1 then '手游'
                    when categoryid = 2 then '端游'
                    when categoryid = 3 then '页游'
                    else '其他'
                end as category_name,
                case 
                    when ishotgame= 1 then '热门'
                    when ishotgame= 0 then '非热门'
                    else '其他'
                end as is_hot_game,
                count(id) as games
            from zhwdb.zhw_game_info where state=1 
            group by 1,2,3
        """

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/game_category_ifhot_{0}.csv".format(part_day))


    def withdraw_order_ts_by_tslb(self, part_month_list):
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

        part_month_concat = ','.join(["'" + s + "'" for s in part_month_list])

        presto_sql = """
            select a.part_month,b.lb,count(b.did) 
            from 
            (
                select part_month,id 
                from zhwdb.zhw_dingdan 
                where part_month in ({0}) and zt=3
            )a
            inner join 
            (
                select part_month,did,lb 
                from zhwdb.zhw_ts 
                where part_month in ({0})
            ) b 
            on a.id=b.did 
            group by 1,2
        """.format(part_month_concat)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/withdraw_order_ts_by_tslb_{0}_to_{1}.csv".format(min(part_month_list),max(part_month_list)))


    def hotgame_total_gmv(self, part_month_list):
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

        part_month_concat = ','.join(["'" + s + "'" for s in part_month_list])

        presto_sql = """
            select 
                a.part_month as dt ,
                count(distinct a.userid) as dr,
                count(a.id) as dn,
                sum(a.pm) as dpm,
                sum(a.pmoney*d.relet_give_hour) as drpm,
                sum(c.add_money)  as dapm
            from 
            (
                select userid,id,pm,pmoney,part_month,gameid 
                from zhwdb.zhw_dingdan 
                where part_month in ({0})
            )a 
            inner join 
            (
                select id 
                from zhwdb.zhw_game_info 
                where state=1 and ishotgame=1
            ) b  on a.gameid=b.id 
            left join 
            (
                select order_id,add_money 
                from zhwdb.zhw_dingdan_channel_addmoney 
                where part_month in ({0})
            ) c  on a.id=c.order_id 
            left join 
            (
                select dingdan_id,relet_give_hour 
                from zhwdb.zhw_dingdan_rent_give
                where part_month in ({0})
            ) d  
            on a.id=d.dingdan_id
            group by 1
        """.format(part_month_concat)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/hotgame_total_gmv_{0}_to_{1}.csv".format(min(part_month_list),max(part_month_list)))


    def category_order_status_gmv(self, part_month):
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
                a.part_month as dt ,
                case 
                    when b.categoryid = 1 then '手游'
                    when b.categoryid = 2 then '端游'
                    when b.categoryid = 3 then '页游'
                    else '其他'
                end as category_name,
                -- 全部
                count(distinct a.userid) as dr,
                count(a.id) as dn,
                sum(a.pm) as dpm,
                sum(a.pmoney*d.relet_give_hour) as drpm,
                sum(c.add_money)  as dapm,
                -- 正常结算
                count(distinct case when a.zt = 2 then a.userid end) as zc_dr,
                count(distinct case when a.zt = 2 then a.id end) as zc_dn,
                sum(case when a.zt = 2 then a.pm end) as zc_dpm,
                sum(case when a.zt = 2 then a.pmoney*d.relet_give_hour end) as zc_drpm,
                sum(case when a.zt = 2 then c.add_money end) as zc_dapm,
                -- 撤单
                count(distinct case when a.zt in (3,4) then a.userid end) as cd_dr,
                count(distinct case when a.zt in (3,4) then a.id end) as cd_dn,
                sum(case when a.zt in (3,4) then a.pm end) as cd_dpm,
                sum(case when a.zt in (3,4) then a.pmoney*d.relet_give_hour end) as cd_drpm,
                sum(case when a.zt in (3,4) then c.add_money end) as cd_dapm
            from 
            (
                select userid,id,pm,pmoney,part_month,gameid,zt
                from zhwdb.zhw_dingdan 
                where part_month='{0}'
            ) a 
            inner join 
            (
                select id,categoryid
                from zhwdb.zhw_game_info 
            ) b  
            on a.gameid=b.id 
            left join 
            (
                select order_id,add_money 
                from zhwdb.zhw_dingdan_channel_addmoney
                where part_month='{0}'	
            )c  
            on a.id=c.order_id 
            left join 
            (
                select dingdan_id,relet_give_hour 
                from zhwdb.zhw_dingdan_rent_give
                where part_month='{0}'
            )d  
            on a.id=d.dingdan_id
            group by 1,2
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/category_order_status_gmv_{0}.csv".format(part_month))


    def top_game_order_status_gmv(self, part_month):
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
                a.part_month as dt ,
                a.gameid,
                b.title as game_name,
                -- 全部
                count(distinct a.userid) as dr,
                count(a.id) as dn,
                sum(a.pm) as dpm,
                sum(a.pmoney*d.relet_give_hour) as drpm,
                sum(c.add_money)  as dapm,
                -- 正常结算
                count(distinct case when a.zt = 2 then a.userid end) as zc_dr,
                count(distinct case when a.zt = 2 then a.id end) as zc_dn,
                sum(case when a.zt = 2 then a.pm end) as zc_dpm,
                sum(case when a.zt = 2 then a.pmoney*d.relet_give_hour end) as zc_drpm,
                sum(case when a.zt = 2 then c.add_money end) as zc_dapm,
                -- 撤单
                count(distinct case when a.zt in (3,4) then a.userid end) as cd_dr,
                count(distinct case when a.zt in (3,4) then a.id end) as cd_dn,
                sum(case when a.zt in (3,4) then a.pm end) as cd_dpm,
                sum(case when a.zt in (3,4) then a.pmoney*d.relet_give_hour end) as cd_drpm,
                sum(case when a.zt in (3,4) then c.add_money end) as cd_dapm
                
            from 
            (
                select userid,id,pm,pmoney,part_month,gameid,zt
                from zhwdb.zhw_dingdan 
                where part_month='{0}'
                and gameid in (11,17,581,24,443,683,560,446)
            ) a 
            inner join 
            (
                select id,categoryid,title
                from zhwdb.zhw_game_info 
            ) b  
            on a.gameid=b.id 
            left join 
            (
                select order_id,add_money 
                from zhwdb.zhw_dingdan_channel_addmoney
                where part_month='{0}'	
            )c  
            on a.id=c.order_id 
            left join 
            (
                select dingdan_id,relet_give_hour 
                from zhwdb.zhw_dingdan_rent_give
                where part_month='{0}'
            )d  
            on a.id=d.dingdan_id
            group by 1,2,3
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/top_game_order_status_gmv_{0}.csv".format(part_month))


    def category_order_status_roi(self, part_month):
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
                case 
                    when f.categoryid = 1 then '手游'
                    when f.categoryid = 2 then '端游'
                    when f.categoryid = 3 then '页游'
                    else '其他'
                end as "游戏类型",
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
                sum(case when a.zt = 2 then e.fx_money+e.fx_channel end) as "正结_分销分成",
                -- 撤单
                sum(case when a.zt in (3,4) then b.add_fee end) as "撤单_官方加价",
                sum(case when a.zt in (3,4) then b.sys_fee end) as "撤单_订单手续费",
                sum(case when a.zt in (3,4) then b.channel_fee end) as "撤单_渠道手续费",
                sum(case when a.zt in (3,4) then c.pay_money end) as "撤单_二级加价",
                sum(case when a.zt in (3,4) then d.channel_money end) as "撤单_渠道分成",
                sum(case when a.zt in (3,4) then e.fx_money+e.fx_channel end) as "撤单_分销分成"
            from 
            (
                select userid,id,pm,pmoney,part_month,gameid,zt 
                from zhwdb.zhw_dingdan 
                where part_month='{0}' 
            )a  
            inner join 
            (
                select id,categoryid
                from zhwdb.zhw_game_info 
            )f 
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

        df_result.to_csv(r"./export_data/category_order_status_roi_{0}.csv".format(part_month))

    def top_game_order_status_roi(self, part_month):
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

        df_result.to_csv(r"./export_data/top_game_order_status_roi_{0}.csv".format(part_month))


    def hours_rent_order_total_zq_dim_category(self, part_month):
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
                b.categoryid,
                count(a.id) as orders,
                sum(a.zq) as total_zq
            from 
            (
                select part_month,userid,id,gameid,zq 
                from zhwdb.zhw_dingdan 
                where part_month = '{0}'
                and rent_type=1 -- 时租订单
            ) a 
            inner join 
            (
                select id,categoryid
                from zhwdb.zhw_game_info 
            ) b  
            on a.gameid=b.id
            group by 1,2
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/hours_rent_order_total_zq_dim_category_{0}.csv".format(part_month))


    def register_users_dim_addfrom(self, part_month):
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

        df_result.to_csv(r"./export_data/register_users_dim_addfrom_{0}.csv".format(part_month))


    def order_pm_dim_month_bigaddfrom(self, part_month):
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
            /*平台官网的订单情况统计  客户端，包含官方、网吧和市场客户端*/
            select 
                a.part_month,
                case 
                    when add_from= 1 then '官网' 
                    when add_from in (13,14,43,50)  then '客户端' 
                    when add_from=6 then 'M站' 
                    when add_from in (2,21,22,30,35)  then 'APP' 
                    when add_from not in (1,13,14,43,50,6,2,21,22,30,35)  then '其他' 
                end as big_addfrom,
                count(distinct a.userid) as dr,
                count(a.id) as dn,
                sum(a.pm) as dpm,
                sum(a.pmoney*b.relet_give_hour) as drpm,
                sum(c.add_money) as dapm 
            from 
            (
                select part_month,userid,id,pm,gameid,pmoney,add_from
                from zhwdb.zhw_dingdan 
                where part_month='{0}'
            ) a 
            left join 
            (
                select dingdan_id,relet_give_hour 
                from zhwdb.zhw_dingdan_rent_give
                where part_month='{0}'
            ) b  
            on 
            a.id=b.dingdan_id
            left join 
            (
                select order_id,add_money 
                from zhwdb.zhw_dingdan_channel_addmoney 
                where part_month='{0}'
            ) c  
            on a.id=c.order_id 
            group by 1,2
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/order_pm_dim_month_bigaddfrom_{0}.csv".format(part_month))


    def mau_dim_month_bigaddfrom(self, part_month):
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
            /*官网的活跃用户数*/
            select part_month,
                case 
                    when lxfl= 1 then '官网' 
                    when lxfl in (7,8,9)  then '客户端' 
                    when lxfl=2 then 'M站' 
                    when lxfl in (3,4,5,6)  then 'APP' 
                    when lxfl =10  then '其他' 
                end as big_addfrom,
            count(distinct userid) as mau
            from zhwdb.zhw_user_login_log_extend 
            where part_month='{0}'
            group by 1,2
        """.format(part_month)

        presto_data_list = operate_presto.query_data(presto_sql)
        presto_data_columns = operate_presto.query_data_index()
        operate_presto.close_conn()

        df_result = pd.DataFrame(presto_data_list)
        df_result.columns = presto_data_columns

        df_result.to_csv(r"./export_data/mau_dim_month_bigaddfrom_{0}.csv".format(part_month))


if __name__ == '__main__':
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    # for part_month in ['2022-05','2022-04','2021-04','2021-05']:
    #     start_time = time.time()
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.core_dim_month_newold_order(part_month)
    #     end_time = time.time()
    #     print('新老用户维度核心指标数据，运行耗时：%s s' % (end_time - start_time))

    # start_time = time.time()
    # start_month, end_month = '2022-04', '2022-05'
    # zhw_analysis = ZhwAnalysis()
    # zhw_analysis.os_mau(start_month, end_month)
    # end_time = time.time()
    # print('不同终端MAU，运行耗时：%s s' % (end_time - start_time))
    #
    # start_time = time.time()
    # start_month, end_month = '2021-04', '2021-05'
    # zhw_analysis = ZhwAnalysis()
    # zhw_analysis.os_mau(start_month, end_month)
    # end_time = time.time()
    # print('不同终端MAU，运行耗时：%s s' % (end_time - start_time))


    # start_time = time.time()
    # save_date_list = ['2022-04-30', '2022-05-31','2021-04-30', '2021-05-31']
    # zhw_analysis = ZhwAnalysis()
    # zhw_analysis.clv_users(save_date_list)
    # end_time = time.time()
    # print('不同日期CLV，运行耗时：%s s' % (end_time - start_time))

    # start_time = time.time()
    # part_month_list = ['2022-04', '2022-05', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.login_user_retain(part_month)
    #     end_time = time.time()
    #     print('不同月份登录留存，运行耗时：%s s' % (end_time - start_time))


    # start_time = time.time()
    # part_month_list = ['2022-04', '2022-05', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.login_new_user_retain(part_month)
    #     end_time = time.time()
    #     print('不同月份新用户登录留存，运行耗时：%s s' % (end_time - start_time))



    # start_time = time.time()
    # part_month_list = ['2022-04', '2022-05', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.login_old_user_retain(part_month)
    #     end_time = time.time()
    #     print('不同月份老用户登录留存，运行耗时：%s s' % (end_time - start_time))


    # start_time = time.time()
    # part_month_list = ['2022-04', '2022-05', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.reg_user_r1_retain(part_month)
    #     end_time = time.time()
    #     print('新注册用户次日留存，运行耗时：%s s' % (end_time - start_time))

    # start_time = time.time()
    # part_month_list = ['2022-05','2022-04','2021-04','2021-05']
    # zhw_analysis = ZhwAnalysis()
    # zhw_analysis.game_orders(part_month_list)
    # end_time = time.time()
    # print('主流游戏的每月订单量，运行耗时：%s s' % (end_time - start_time))

    # start_time = time.time()
    # part_month_list = ['2022-05','2022-04','2021-04','2021-05']
    # for part_month in part_month_list:
    #     part_month_list_1 = [part_month]
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.channel_gmv(part_month_list_1)
    #     end_time = time.time()
    #     print('渠道区分的每月gmv，运行耗时：%s s' % (end_time - start_time))


    # start_time = time.time()
    # part_month_list = ['2022-05','2022-04','2021-04','2021-05']
    # zhw_analysis = ZhwAnalysis()
    # zhw_analysis.withdraw_order_ts_by_tslb(part_month_list)
    # end_time = time.time()
    # print('撤单的订单投诉类别，运行耗时：%s s' % (end_time - start_time))


    # start_time = time.time()
    # part_month_list = ['2022-05','2022-04','2021-04','2021-05']
    # zhw_analysis = ZhwAnalysis()
    # zhw_analysis.withdraw_order_ts_lb_eq1_by_tslx(part_month_list)
    # end_time = time.time()
    # print('撤单的订单投诉类型1下的投诉类别，运行耗时：%s s' % (end_time - start_time))

    # start_time = time.time()
    # part_day = '2022-06-08'
    # zhw_analysis = ZhwAnalysis()
    # zhw_analysis.game_category_ifhot(part_day)
    # end_time = time.time()
    # print('游戏分类是否热门，运行耗时：%s s' % (end_time - start_time))


    # start_time = time.time()
    # part_month_list = ['2022-05','2022-04','2021-04','2021-05']
    # for part_month in part_month_list:
    #     part_month_list_1= [part_month]
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.hotgame_total_gmv(part_month_list_1)
    #     end_time = time.time()
    #     print('热门游戏总GMV，运行耗时：%s s' % (end_time - start_time))


    # start_time = time.time()
    # part_month_list = ['2022-05','2022-04','2021-04','2021-05']
    # for part_month in part_month_list:
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.category_order_status_gmv(part_month)
    #     end_time = time.time()
    #     print('手端页不同状态订单GMV，运行耗时：%s s' % (end_time - start_time))

    # start_time = time.time()
    # part_month_list = ['2022-05', '2022-04', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.top_game_order_status_gmv(part_month)
    #     end_time = time.time()
    #     print('TOP游戏不同状态订单GMV，运行耗时：%s s' % (end_time - start_time))


    # start_time = time.time()
    # part_month_list = ['2022-05', '2022-04', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.category_order_status_roi(part_month)
    #     end_time = time.time()
    #     print('手端页不同状态订单收支，运行耗时：%s s' % (end_time - start_time))

    # start_time = time.time()
    # part_month_list = ['2022-05', '2022-04', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.top_game_order_status_roi(part_month)
    #     end_time = time.time()
    #     print('TOP游戏不同状态订单ROI，运行耗时：%s s' % (end_time - start_time))


    # part_month_list = ['2022-05', '2022-04', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     start_time = time.time()
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.hours_rent_order_total_zq_dim_category(part_month)
    #     end_time = time.time()
    #     print('端手页时租订单的总租期，运行耗时：%s s' % (end_time - start_time))

    # part_month_list = ['2022-05', '2022-04', '2021-04', '2021-05']
    # for part_month in part_month_list:
    #     start_time = time.time()
    #     zhw_analysis = ZhwAnalysis()
    #     zhw_analysis.register_users_dim_addfrom(part_month)
    #     end_time = time.time()
    #     print('不同端口注册用户量，运行耗时：%s s' % (end_time - start_time))


    part_month_list = ['2022-05', '2022-04', '2022-03' ,'2021-05', '2021-04', '2021-03']
    for part_month in part_month_list:
        start_time = time.time()
        zhw_analysis = ZhwAnalysis()
        zhw_analysis.mau_dim_month_bigaddfrom(part_month)
        end_time = time.time()
        print('不同月份大端口月活数据，运行耗时：%s s' % (end_time - start_time))

    part_month_list = ['2022-05', '2022-04', '2022-03' ,'2021-05', '2021-04', '2021-03']
    for part_month in part_month_list:
        start_time = time.time()
        zhw_analysis = ZhwAnalysis()
        zhw_analysis.order_pm_dim_month_bigaddfrom(part_month)
        end_time = time.time()
        print('不同月份大端口订单数据，运行耗时：%s s' % (end_time - start_time))