# _*_coding:utf-8 _*_

# @Time      : 2022/12/18  19:26
# @Author    : An
# @File      : wow_dws_measures_by_regdate_gapdays.py
# @Software  : 该主题涉及注册第N日内的充值、采购成本、退款等数据，每日更新前15日的数据
"""
delete from wow_dws_measures_by_regdate_gapdays where part_day between date_format(date_add(str_to_date('{0}','%Y-%m-%d'), interval -15 day),'%Y-%m-%d') and '{0}'
"""


import time, datetime, configparser, warnings, math, platform
import sys
import pymysql, presto
# import pypresto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine

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


class WowDwsMeasuresByRegdateGapdays:
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
        mysql_sql = "delete from wow_dws_measures_by_regdate_gapdays where part_day between '{0}' and '{1}'".format(start_day ,end_day)
        operate_mysql.delete_data(mysql_sql)


        # 原始表 数据1-处理
        holo_sql = """
            select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    b.part_day as part_day,-- 注册日期
                    '充值' as type,
                    date(a.addtime)  - date(b.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(a.money/100.00) as money,
                    count(a.out_trade_no) as times,
                    count(distinct a.uid) as users
                from 
                (
                    select *,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) b
                left join 
                (
                    select addtime,business_code,money,out_trade_no,uid,return_money,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    to_char(addtime,'yyyy-mm-dd') as part_day
                    from wow.wow_recharge
                    where true 
                    and to_char(addtime,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                    and status = 2
                ) a 
                on a.uid = b.id 
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
            
            union all 
            select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    a.part_day, -- 注册日期
                    '采购成本' as type,
                    date(c.add_time)  - date(a.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(c.pm_buy) as money,
                    count(c.uid) as times,
                    count(distinct c.uid) as users
                from 
                (
                    select id,add_time,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) a 
                left join 
                (
                    select 
                        a.uid,a.add_time,a.part_day,b.pm_buy/100.00 as pm_buy,
                        case when a.app_id is null or a.app_id = '' then '99999999' else a.app_id end as app_id
                    from
                    (
                        select *,to_char(add_time,'yyyy-mm-dd') as part_day
                        from wow.wow_war_send
                        where true 
                        and to_char(add_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                    ) a 
                    left join 
                    (
                        select id,pm,pm_buy,is_free,uid,pm_red_packet,red_packet_id,channel_ad_id
                        from wow.wow_war
                        where true 
                        and to_char(addtime,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                    ) b 
                    on a.war_id = b.id 
            
                    union all 
            
                    select 
                        uid,success_time as add_time,to_char(success_time,'yyyy-mm-dd') as part_day,pm_buy*buy_num/100.00 as pm_buy,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id
                    from wow.wow_shop_order
                    where true 
                    and to_char(success_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                ) c 
                on a.id = c.uid
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
            
            union all 
            select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    a.part_day, -- 注册日期
                    '退款成本' as type,
                    date(c.add_time)  - date(a.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(c.pm_buy) as money,
                    count(c.uid) as times,
                    count(distinct c.uid) as users
                from 
                (
                    select id,add_time,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) a 
                left join 
                (
                    select 
                        uid,update_time as add_time,to_char(update_time,'yyyy-mm-dd') as part_day,pm_buy/100.00 as pm_buy,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id
                    from wow.wow_war
                    where true 
                    and to_char(update_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                    and status = 401 -- 退款
            
                    union all 
            
                    select 
                        uid,update_time as add_time,to_char(update_time,'yyyy-mm-dd') as part_day,pm_buy*buy_num/100.00 as pm_buy,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id
                    from wow.wow_shop_order
                    where true 
                    and to_char(update_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                    and status = 6 -- 退款
                ) c 
                on a.id = c.uid
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
            
            union all 
            select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    b.part_day as part_day,-- 注册日期
                    '充值退款' as type,
                    date(a.addtime)  - date(b.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(a.return_money/100.00) as money,
                    count(a.out_trade_no) as times,
                    count(distinct a.uid) as users
                from 
                (
                    select *,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) b
                left join 
                (
                    select return_time as addtime,business_code,money,out_trade_no,uid,return_money,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    to_char(return_time,'yyyy-mm-dd') as part_day
                    from wow.wow_recharge
                    where true 
                    and to_char(return_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                    and status = 2
                ) a 
                on a.uid = b.id 
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
        """.format(start_day ,end_day)

        holo_data_list = operate_hologres.query_data(holo_sql)

        operate_hologres_log = {}
        for k, v in operate_hologres.operate_result.items():
            if v[0] != '无':
                operate_hologres_log[k] = v

         # 目标表 数据1-处理后写入
        columns = 7
        insert_sql = "insert into wow_dws_measures_by_regdate_gapdays(part_day ,type ,gap_days ,source_name ,money ,times ,users) values ({0});".format(','.join(['%s'] * columns))
        operate_mysql.insert_data(insert_sql, holo_data_list)

        # 原始表 数据2-处理
        holo_sql = """
            select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    b.part_day as part_day,-- 注册日期
                    'V1收入-商城哇宝收入' as type,
                    date(a.addtime)  - date(b.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(a.pm_wabao/10000.00) as money,
                    count(a.id) as times,
                    count(distinct a.uid) as users
                from 
                (
                    select *,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) b
                left join 
                (
                    select success_time as addtime,pm_wabao,uid,id,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    to_char(success_time,'yyyy-mm-dd') as part_day
                    from wow.wow_shop_order
                    where true 
                    and to_char(success_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                ) a 
                on a.uid = b.id 
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
			
			union all 
			select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    b.part_day as part_day,-- 注册日期
                    'V1收入-抽盒充值收入' as type,
                    date(a.addtime)  - date(b.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(a.money/100.00) as money,
                    count(a.out_trade_no) as times,
                    count(distinct a.uid) as users
                from 
                (
                    select *,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) b
                left join 
                (
                    select money,out_trade_no,uid,addtime,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(addtime,'yyyy-mm-dd') as part_day
                    from wow.wow_recharge  
                    where to_char(addtime,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
					and status = 2
					and business_code in (10000,30000)-- war 
                ) a 
                on a.uid = b.id 
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
			
			union all
			select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    b.part_day as part_day,-- 注册日期
                    'V1收入-商城充值收入' as type,
                    date(a.addtime)  - date(b.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(a.money/100.00) as money,
                    count(a.out_trade_no) as times,
                    count(distinct a.uid) as users
                from 
                (
                    select *,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) b
                left join 
                (
                    select money,out_trade_no,uid,addtime,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(addtime,'yyyy-mm-dd') as part_day
                    from wow.wow_recharge  
                    where to_char(addtime,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
					and status = 2
					and business_code in (20000,30001)-- shop 
                ) a 
                on a.uid = b.id 
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
			
			union all
			select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    b.part_day as part_day,-- 注册日期
                    'V1成本-抽盒发货成本' as type,
                    date(a.addtime)  - date(b.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(aa.pm_buy/100.00) as money,
                    count(aa.id) as times,
                    count(distinct aa.uid) as users
                from 
                (
                    select *,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) b
                left join 
                (
                    select war_id,add_time as addtime,uid,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_war_send  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                ) a 
                on a.uid = b.id
				left join 
				(
					select id,pm,pm_buy,is_free,uid,pm_red_packet,red_packet_id 
					from wow.wow_war
					where true 
					and to_char(addtime,'yyyy-mm-dd') between to_char(date '{0}' - integer '90','yyyy-mm-dd') and '{1}'
				) aa 
				on a.war_id = aa.id 
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
			
			union all
			select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    b.part_day as part_day,-- 注册日期
                    'V1成本-商城发货成本' as type,
                    date(a.addtime)  - date(b.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(a.pm_buy*a.buy_num/100.00) as money,
                    count(a.id) as times,
                    count(distinct a.uid) as users
                from 
                (
                    select *,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) b
                left join 
                (
                    select pm_wabao,pm_buy,buy_num,success_time as addtime,uid,id,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(success_time,'yyyy-mm-dd') as part_day
                    from wow.wow_shop_order  
                    where to_char(success_time,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                ) a 
                on a.uid = b.id
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
			
			union all
			select part_day ,type ,gap_days,coalesce(source_name,'-') as source_name ,
            money ,times ,users
            from 
            (
                select 
                    b.part_day as part_day,-- 注册日期
                    'V1成本-抽盒分解哇宝' as type,
                    date(a.addtime)  - date(b.add_time) as gap_days, -- 注册后第N日
                    coalesce(d2.item_name,'其他') as source_name,
                    sum(a.recovery_num/10000.00) as money,
                    count(a.id) as times,
                    count(distinct a.uid) as users
                from 
                (
                    select *,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_user  
                    where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) b
                left join 
                (
                    select recovery_num,pm_buy,addtime as addtime,uid,id,
                        case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                        to_char(addtime,'yyyy-mm-dd') as part_day
                    from wow.wow_war  
                    where to_char(addtime,'yyyy-mm-dd') between '{0}' and to_char(date('{1}') + integer '15','yyyy-mm-dd')
                ) a 
                on a.uid = b.id
                left join 
                (	-- 应用来源（安卓、iOS、小米、华为、H5）
                    select 
                    cast(item_value as integer) as item_value,
                    item_name
                    from wow.wow_dict_item 
                    where dict_id  = 3
                ) d2 
                on cast(a.app_id as integer)  = d2.item_value 
                group by grouping sets((1,2,3),(1,2,3,4))
            ) t 
            where t.gap_days is not null 
        """.format(start_day ,end_day)

        holo_data_list = operate_hologres.query_data(holo_sql)

        operate_hologres_log = {}
        for k, v in operate_hologres.operate_result.items():
            if v[0] != '无':
                operate_hologres_log[k] = v

        operate_hologres.close_conn()

        # 目标表 数据2-处理后写入
        columns = 7
        insert_sql = "insert into wow_dws_measures_by_regdate_gapdays(part_day ,type ,gap_days ,source_name ,money ,times ,users) values ({0});".format(
            ','.join(['%s'] * columns))
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

    lst = [2]
    lst.reverse()
    for i in lst:
        start_time = time.time()
        last_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        last_15day = (datetime.datetime.now() - datetime.timedelta(days=i+15)).strftime('%Y-%m-%d')

        wow_dws_measures_by_regdate_gapdays = WowDwsMeasuresByRegdateGapdays()
        wow_dws_measures_by_regdate_gapdays.run(last_15day,last_day)
        end_time = time.time()
        print(last_day, '运行耗时：', end_time - start_time)