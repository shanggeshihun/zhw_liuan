# _*_coding:utf-8 _*_

# @Time      : 2022/12/15  18:26
# @Author    : An
# @File      : wow_dws_advertising_by_day_adid.py
# @Software  : 该主题涉及注册7日内的充值等数据，每日更新前7日的数据；当无查询结果返回时，需要初始化，以免merge无结果返回
"""
delete from wow_dws_advertising_by_day_adid where part_day between date_format(date_add(str_to_date('{0}','%Y-%m-%d'), interval -7 day),'%Y-%m-%d') and '{0}'
"""

import time, datetime, configparser, warnings, math, platform
import sys

import numpy as np
import pandas as pd
import pymysql, presto
# import pypresto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)


plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件\在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysqlNew import OperateMysqlNew
from OperatePresto import OperatePresto
from OperateHologresNew import OperateHologresNew


class WowDwsAdvertisingByDayAdid:
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

        self.final_result_log = {}

        # 运营数据库
        self.mysql_host = cf.get("Mysql-data_yunying", "host")
        self.mysql_user = cf.get("Mysql-data_yunying", "user")
        self.mysql_password = cf.get("Mysql-data_yunying", "password")
        self.mysql_db = cf.get("Mysql-data_yunying", "DB")
        self.mysql_port = cf.get("Mysql-data_yunying", "port")

        # 本地MySQL8.0
        # self.mysql_local8_host = cf.get("MySQL8.0-localhost", "host")
        # self.mysql_local8_user = cf.get("MySQL8.0-localhost", "user")
        # self.mysql_local8_password = cf.get("MySQL8.0-localhost", "password")
        # self.mysql_local8_db = cf.get("MySQL8.0-localhost", "DB")
        # self.mysql_local8_port = cf.get("MySQL8.0-localhost", "port")

        # hive数据库
        self.presto_host = cf.get("hive_presto", "host")
        self.presto_username = cf.get("hive_presto", "username")
        self.presto_port = cf.get("hive_presto", "port")
        self.presto_schema = cf.get("hive_presto", "schema")
        self.presto_catalog = cf.get("hive_presto", "catalog")

        # hologres数据库
        self.holo_host = cf.get("hologres-dofun", "host")
        self.holo_port = cf.get("hologres-dofun", "port")
        self.holo_database = cf.get("hologres-dofun", "DB")
        self.holo_user = cf.get("hologres-dofun", "user")
        self.holo_password = cf.get("hologres-dofun", "password")

    def run(self, start_day ,end_day):
        """
        :param start_day: 起始日期参数
        :param end_day: 终止日期参数
        :return: 抽奖主题：抽奖日期+用户维度  指标数据 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysqlNew(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # 实例化mysql 本地 8.0
        # operate_mysql_local8 = OperateMysqlNew(
        #     username=self.mysql_local8_user,
        #     password=self.mysql_local8_password,
        #     host_ip=self.mysql_local8_host,
        #     port=int(self.mysql_local8_port),
        #     database=self.mysql_local8_db
        # )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # 实例化Hologres，查询待更新的数据信息
        operate_hologres = OperateHologresNew(
            username=self.holo_user,
            password=self.holo_password,
            host_ip=self.holo_host,
            port=int(self.holo_port),
            database=self.holo_database
        )

        # 清理目标表数据
        mysql_sql = "delete from wow_dws_advertising_by_day_adid where part_day between '{0}' and '{1}'".format(start_day ,end_day)
        operate_mysql.delete_data(mysql_sql)

        # 原始表 数据处理

        # 投放效果初始表
        holo_sql = """
            select to_char(a.summary_date ,'yyyy-mm-dd') as part_day,a.app_id,coalesce(d2.item_name,'其他') as source_name,
            a.ads_id as ad_id,a.ads_name,a.show_count,a.click_count ,a.download_count ,a.spent/100.00 as spent
            from 
            (
                select  summary_date ,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    ads_id,ads_name,show_count,click_count ,download_count ,spent
                from wow.wow_channel_pull_ads_summary 
                where true 
                and to_char(summary_date ,'yyyy-mm-dd') between '{0}' and '{1}'
                -- and id not in (143 ,144 ,145 ,146 ,86 ,98 ,77 ,71 ,68 ,64 ,55 ,60 ,107 ,45 ,12 ,106 ,44 ,42 ,105 ,138 ,137 ,97 ,85 ,75 ,119 ,62 ,116 ,111 ,57 ,109 ,47 ,13 ,100 ,132 ,120 ,76 ,54 ,59 ,46 ,49 ,141 ,92 ,73 ,110 ,117 ,128 ,84 ,78 ,72 ,65 ,61 ,58 ,53 ,80 ,74 ,95 ,127 ,125 ,81 ,103 ,91 ,102 ,90 ,131 ,87 ,126 ,82 ,101 ,133 ,95)
            ) a 
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value 

        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            init_df = pd.DataFrame(holo_data_list)
            init_df.columns = columns
        else:
            # 当 投放效果初始表 无数据返回则 不再继续数据关联
            sys.exit()
        # 新增注册用户数
        reg_holo_sql = """
            select 
                a.part_day,
                coalesce(d2.item_name,'其他') as source_name,
                coalesce(cast(d.ad_id as integer),99999999) as ad_id,
                count(a.id) as reg_users
            from 
            (
                select 
                    id,to_char(add_time,'yyyy-mm-dd') as part_day,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from wow.wow_user 
                where true 
                and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
            ) a 
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value
            left join wow.wow_channel_ad d 
            on a.channel_ad_id = d.id 
            group by 1,2,3
        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(reg_holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            reg_df = pd.DataFrame(holo_data_list)
            reg_df.columns = columns
        else:
            reg_df = pd.DataFrame(
                {'part_day': ['2100-01-01'], 'source_name': ['其他'], 'ad_id': [99999999], 'reg_users': [0]})

        init_df = pd.merge(init_df,reg_df,how = 'left',left_on = ['part_day','ad_id'],right_on = ['part_day','ad_id'])
        init_df.rename(columns={'source_name_x': 'source_name'}, inplace=True)

        # 新增充值用户数 新增充值金额 充值用户数 充值金额
        recharge_holo_sql = """
            select 
                a.part_day,
                coalesce(d2.item_name,'其他') as source_name,
                coalesce(cast(d.ad_id as integer),99999999) as ad_id,
                count(distinct a.uid) as recharge_users,
                sum(a.recharge_money) as recharge_money,
                count(distinct case when b.id is not null then a.uid end) as reg_recharge_users,
                sum(case when b.id is not null then a.recharge_money end) as reg_recharge_money
            from 
            (
                select to_char(addtime,'yyyy-mm-dd') as part_day,
                    uid,money/100.00 as recharge_money,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from wow.wow_recharge 
                where true 
                and to_char(addtime,'yyyy-mm-dd') between '{0}' and '{1}'
                and status = 2 -- 支付成功
            ) a 
            left join
            (
                select id,to_char(add_time,'yyyy-mm-dd') as part_day 
                from wow.wow_user 
                where true 
                and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}' 
            ) b 
            on a.uid = b.id and a.part_day = b.part_day
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value
            left join wow.wow_channel_ad d 
            on a.channel_ad_id = d.id 
            group by 1,2,3
        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(recharge_holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            recharge_df = pd.DataFrame(holo_data_list)
            recharge_df.columns = columns
        else:
            recharge_df = pd.DataFrame({
                'part_day': ['2100-01-01'], 'source_name': ['其他'],
                 'ad_id': [99999999], 'recharge_users': [0],'recharge_money':[0.00],
                'reg_recharge_users':[0],'reg_recharge_money':[0]
            })

        init_df = pd.merge(init_df,recharge_df,how = 'left',left_on = ['part_day','ad_id'],right_on = ['part_day','ad_id'])
        init_df.rename(columns={'source_name_x': 'source_name'}, inplace=True)


        # 充值退款用户数 充值退款金额
        refund_holo_sql = """
            select 
                a.part_day,
                coalesce(d2.item_name,'其他') as source_name,
                coalesce(cast(d.ad_id as integer),99999999) as ad_id,
                count(distinct a.uid) as recharge_refund_users,
                sum(a.recharge_money) as recharge_refund_money
            from 
            (
                select to_char(return_time,'yyyy-mm-dd') as part_day,
                    uid,money/100.00 as recharge_money,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from wow.wow_recharge 
                where true 
                and to_char(return_time,'yyyy-mm-dd') between '{0}' and '{1}'
                and status = 2 -- 支付成功
            ) a 
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value
            left join wow.wow_channel_ad d 
            on a.channel_ad_id = d.id 
            group by 1,2,3
        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(refund_holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            refund_df = pd.DataFrame(holo_data_list)
            refund_df.columns = columns
        else:
            refund_df = pd.DataFrame({
                'part_day':['2100-01-01'],'source_name':['其他'],
                'ad_id':[99999999],'recharge_refund_users':[0],
                'recharge_refund_money':[0.00]
            })
        init_df = pd.merge(init_df,refund_df,how = 'left',left_on = ['part_day','ad_id'],right_on = ['part_day','ad_id'])


        # 新增采购成本 采购成本
        cost_holo_sql = """
            select 
                a.part_day as part_day,
                coalesce(d2.item_name,'其他') as source_name,
                coalesce(cast(d.ad_id as integer),99999999) as ad_id,
                sum(a.pm_buy/100.00) as cost_money,
                sum(case when reg.id is not null then a.pm_buy/100.00 end) as reg_cost_money
            from 
            (
                select 
                    a.uid,a.part_day,b.pm_buy/100.00 as pm_buy,
                    case when a.app_id is null or a.app_id = '' then '99999999' else a.app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from
                (
                    select *,to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_war_send
                    where true 
                    and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) a 
                left join 
                (
                    select id,pm,pm_buy,is_free,uid,pm_red_packet,red_packet_id,channel_ad_id
                    from wow.wow_war
                    where true 
                ) b 
                on a.war_id = b.id 
                
                union all 
                
                select uid,to_char(success_time,'yyyy-mm-dd') as part_day,pm_buy*buy_num as pm_buy,
                case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from wow.wow_shop_order
                where true 
                and to_char(success_time,'yyyy-mm-dd') between '{0}' and '{1}'
            ) a
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value 
            left join 
            (
                select id,to_char(add_time,'yyyy-mm-dd') as part_day
                from wow.wow_user 
                where true 
                and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
            ) reg 
            on a.uid = reg.id and a.part_day = reg.part_day
            left join wow.wow_channel_ad d 
            on a.channel_ad_id = d.id 
            group by 1,2,3
        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(cost_holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            cost_df = pd.DataFrame(holo_data_list)
            cost_df.columns = columns
        else:
            cost_df = pd.DataFrame({
                'part_day':['2100-01-01'],'source_name':['其他'],
                'ad_id':[99999999],'cost_money':[0.00],
                'reg_cost_money':[0.00]
            })
        init_df = pd.merge(init_df,cost_df,how = 'left',left_on = ['part_day','ad_id'],right_on = ['part_day','ad_id'])

        # 7日内充值金额
        reg7_recharge_holo_sql = """
            select a.part_day,
                coalesce(cast(d.ad_id as integer),99999999) as ad_id,
                coalesce(d2.item_name,'其他') as source_name,
                sum(recharge_money) as reg7_recharge_money
            from 
            (
                select 	
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id,
                    id,add_time,to_char(add_time,'yyyy-mm-dd') as part_day
                from wow.wow_user 
                where true 
                and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
            ) a 
            left join 
            (
                select to_char(addtime,'yyyy-mm-dd') as part_day,addtime,
                    uid,money/100.00 as recharge_money,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from wow.wow_recharge 
                where true 
                and to_char(addtime,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '7','yyyy-mm-dd')
                and status = 2 -- 支付成功
            ) b 
            on a.id = b.uid and a.channel_ad_id = b.channel_ad_id and date(b.addtime) between date(a.add_time) and date(a.add_time) + integer '6'
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value 
            left join wow.wow_channel_ad d 
            on a.channel_ad_id = d.id 
            group by 1,2,3
        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(reg7_recharge_holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            reg7_recharge_df = pd.DataFrame(holo_data_list)
            reg7_recharge_df.columns = columns
        else:
            reg7_recharge_df = pd.DataFrame({
                'part_day': ['2100-01-01'], 'source_name': ['其他'],
                'ad_id': [99999999], 'reg7_recharge_money': [0.00]
            })

        init_df = pd.merge(init_df, reg7_recharge_df, how='left', left_on=['part_day', 'ad_id'],right_on=['part_day', 'ad_id'])

        # 7日内充值退款金额
        reg7_refund_holo_sql = """
            select a.part_day,
                coalesce(cast(d.ad_id as integer),99999999) as ad_id,
                coalesce(d2.item_name,'其他') as source_name,
                sum(refund_money) as reg7_refund_money
            from 
            (
                select 	
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id,
                    id,add_time,to_char(add_time,'yyyy-mm-dd') as part_day
                from wow.wow_user 
                where true 
                and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
            ) a 
            left join 
            (
                select to_char(return_time,'yyyy-mm-dd') as part_day,return_time,
                    uid,return_money/100.00 as refund_money,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from wow.wow_recharge 
                where true 
                and to_char(return_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '7','yyyy-mm-dd')
                and status = 2 -- 支付成功
            ) b 
            on a.id = b.uid and a.channel_ad_id = b.channel_ad_id and date(b.return_time) between date(a.add_time) and date(a.add_time) + integer '6'
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value 
            left join wow.wow_channel_ad d 
            on a.channel_ad_id = d.id 
            group by 1,2,3
        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(reg7_refund_holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            reg7_refund_df = pd.DataFrame(holo_data_list)
            reg7_refund_df.columns = columns
        else:
            reg7_refund_df = pd.DataFrame({
                'part_day':['2100-01-01'],'source_name':['其他'],
                'ad_id':[99999999],'reg7_refund_money':[0.00]
            })
        init_df = pd.merge(init_df, reg7_refund_df, how='left', left_on=['part_day', 'ad_id'],
                           right_on=['part_day', 'ad_id'])

        # 7日内成本
        reg7_cost_holo_sql = """
            select a.part_day,
                coalesce(cast(d.ad_id as integer),99999999) as ad_id,
                coalesce(d2.item_name,'其他') as source_name,
                sum(c.pm_buy) as reg7_pm_buy
            from 
            (
                select 	
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id,
                    id,add_time,to_char(add_time,'yyyy-mm-dd') as part_day
                from wow.wow_user 
                where true 
                and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
            ) a 
            left join 
            (
                select 
                    a.uid,a.part_day,b.pm_buy/100.00 as pm_buy,
                    case when a.app_id is null or a.app_id = '' then '99999999' else a.app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from
                (
                    select *,to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_war_send
                    where true 
                    and to_char(add_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '7','yyyy-mm-dd')
                ) a 
                left join 
                (
                    select id,pm,pm_buy,is_free,uid,pm_red_packet,red_packet_id,channel_ad_id
                    from wow.wow_war
                    where true 
                ) b 
                on a.war_id = b.id 
            
                union all 
            
                select uid,to_char(success_time,'yyyy-mm-dd') as part_day,pm_buy*buy_num/100.00 as pm_buy,
                case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from wow.wow_shop_order
                where true 
                and to_char(success_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '7','yyyy-mm-dd')
            ) c 
            on a.id = c.uid and a.channel_ad_id = c.channel_ad_id and date(c.part_day) between date(a.add_time) and date(a.add_time) + integer '6'
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value 
            left join wow.wow_channel_ad d 
            on a.channel_ad_id = d.id 
            group by 1,2,3
        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(reg7_cost_holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            reg7_cost_df = pd.DataFrame(holo_data_list)
            reg7_cost_df.columns = columns
        else:
            reg7_cost_df = pd.DataFrame({
                'part_day': ['2100-01-01'], 'source_name': ['其他'],
                'ad_id': [99999999], 'reg7_pm_buy': [0.00]
            })
        init_df = pd.merge(init_df, reg7_cost_df, how='left', left_on=['part_day', 'ad_id'],
                               right_on=['part_day', 'ad_id'])

        # 7日内退款单的成本
        reg7_rfcost_holo_sql = """
            select a.part_day,
                coalesce(cast(d.ad_id as integer),99999999) as ad_id,
                coalesce(d2.item_name,'其他') as source_name,
                sum(c.pm_buy) as reg7_rf_pm_buy
            from 
            (
                select 	
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id,
                    id,add_time,to_char(add_time,'yyyy-mm-dd') as part_day
                from wow.wow_user 
                where true 
                and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
            ) a 
            left join 
            (
                select 
                    a.uid,a.part_day,b.pm_buy/100.00 as pm_buy,
                    case when a.app_id is null or a.app_id = '' then '99999999' else a.app_id end as app_id,
                    case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from
                (
                    select *,to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_war_send
                    where true 
                    and to_char(add_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '7','yyyy-mm-dd')
                ) a 
                left join 
                (
                    select id,pm,pm_buy,is_free,uid,pm_red_packet,red_packet_id,channel_ad_id
                    from wow.wow_war
                    where true 
                ) b 
                on a.war_id = b.id 

                union all 

                select uid,to_char(success_time,'yyyy-mm-dd') as part_day,pm_buy*buy_num/100.00 as pm_buy,
                case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                case when channel_ad_id>0 then channel_ad_id else 99999999 end as channel_ad_id
                from wow.wow_shop_order
                where true 
                and to_char(success_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '7','yyyy-mm-dd')
            ) c 
            on a.id = c.uid and a.channel_ad_id = c.channel_ad_id and date(c.part_day) between date(a.add_time) and date(a.add_time) + integer '6'
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(a.app_id as integer)  = d2.item_value 
            left join wow.wow_channel_ad d 
            on a.channel_ad_id = d.id 
            group by 1,2,3
        """.format(start_day ,end_day)
        holo_data_list = operate_hologres.query_data(reg7_rfcost_holo_sql)
        if holo_data_list:
            columns = operate_hologres.query_data_index()
            reg7_rfcost_df = pd.DataFrame(holo_data_list)
            reg7_rfcost_df.columns = columns

        else:
            reg7_rfcost_df = pd.DataFrame({
                'part_day':['2100-01-01'],'source_name':['其他'],
                'ad_id':[99999999],'reg7_rf_pm_buy':[0.00]
            })
        init_df = pd.merge(init_df, reg7_rfcost_df, how='left', left_on=['part_day', 'ad_id'],
                               right_on=['part_day', 'ad_id'])

        init_df.rename(columns={'source_name_x': 'source_name'}, inplace=True)
        result_columns = ['part_day','app_id','source_name','ad_id','ads_name','show_count','click_count','download_count','spent','reg_users',
                          'recharge_users','recharge_money','reg_recharge_users','reg_recharge_money','recharge_refund_users','recharge_refund_money',
                          'cost_money','reg_cost_money','reg7_recharge_money','reg7_refund_money','reg7_pm_buy','reg7_rf_pm_buy'
                          ]
        df_result = init_df[result_columns]
        df_result.fillna(0,inplace=True)
        df_result = df_result.iloc[:,[0,1,2,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]]

        holo_data_list = df_result.values.tolist()
        operate_hologres_log = {}
        for k, v in operate_hologres.operate_result.items():
            if v[0] != '无':
                operate_hologres_log[k] = v

        operate_hologres.close_conn()

        # 目标表 数据写入
        columns = 22
        insert_sql = "insert into wow_dws_advertising_by_day_adid(part_day ,app_id ,source_name ,ad_id ,ads_name ,show_count ,click_count ,download_count ,spent ,reg_users ,recharge_users ,recharge_money ,reg_recharge_users ,reg_recharge_money ,recharge_refund_users ,recharge_refund_money ,cost_money ,reg_cost_money ,reg7_recharge_money ,reg7_refund_money ,reg7_pm_buy ,reg7_rf_pm_buy) values ({0});".format(','.join(['%s'] * columns))
        operate_mysql.insert_data(insert_sql, holo_data_list)

        operate_mysql_log = {}
        for k, v in operate_mysql.operate_result.items():
            if v[0] != '无':
                operate_mysql_log[k] = v

        operate_mysql.close_conn()

        # 数据日志记录
        self.final_result_log['operate_hologres_log'] = operate_hologres_log
        self.final_result_log['operate_mysql'] = operate_mysql_log


if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    lst = [4]
    lst.reverse()
    for i in lst:
        start_time = time.time()
        last_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
        last_7day = (datetime.datetime.now() - datetime.timedelta(days=i+7)).strftime('%Y-%m-%d')  # t-8

        wow_dws_advertising_by_day_adid = WowDwsAdvertisingByDayAdid()
        wow_dws_advertising_by_day_adid.run(last_7day ,last_day)
        end_time = time.time()
        print(last_day, '运行耗时：', end_time - start_time)