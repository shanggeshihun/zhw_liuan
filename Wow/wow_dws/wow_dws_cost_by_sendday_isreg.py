# _*_coding:utf-8 _*_

# @Time      : 2022/11/22  18:26
# @Author    : An
# @File      : wow_dws_cost_by_sendday_isreg.py
# @Software  : wow_dws_cost_by_sendday_isreg 不涉及 滞后更新的数据指标，需要注意关联抽奖距离实际发货超过90天的情况


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


class WowDwsCostBySenddayIsreg:
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
        mysql_sql = "delete from wow_dws_cost_by_sendday_isreg where part_day between '{0}' and '{1}'".format(start_day ,end_day)
        operate_mysql.delete_data(mysql_sql)

        # 原始表 数据处理
        holo_sql = """
        select part_day ,is_reg ,coalesce(pay_method,'-') as pay_method,coalesce(source_name,'-') as source_name ,
        coalesce(big_business,'-') as big_business ,
        cost_money ,send_users
        from 
        (
            select 
                a.part_day as part_day,
                case when reg.id is not null then 'reg' else 'non_reg' end as is_reg,
                coalesce(d1.item_name,'未充值') as pay_method,
                coalesce(d2.item_name,'其他') as source_name,
                a.big_business,
                sum(a.pm_buy/100.00) as cost_money,
                count(distinct a.uid) as send_users
            from 
            (
                select a.uid,a.part_day,'war' as big_business,b.pm_buy,p.pay_way as pay_type,
                case when a.app_id is null or a.app_id = '' then '99999999' else a.app_id end as app_id
                from
                (
                    select *,to_char(add_time,'yyyy-mm-dd') as part_day
                    from wow.wow_war_send
                    where true 
                    and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'
                ) a 
                left join 
                (
                    select id,pm,pm_buy,is_free,uid,pm_red_packet,red_packet_id 
                    from wow.wow_war
                    where true 
                    and to_char(addtime,'yyyy-mm-dd') between to_char(date '{0}' - integer '90','yyyy-mm-dd') and '{1}'
                ) b 
                on a.war_id = b.id 
                left join 
                (	-- 存在多抽对应同一个商户订单号
                    select id,pay_way
                    from wow.wow_recharge 
                    where true 
                    and to_char(addtime,'yyyy-mm-dd') between to_char(date '{0}' - integer '90','yyyy-mm-dd') and '{1}'
                ) p 
                on a.recharge_id = p.id
                
                union all 
                
                select uid,to_char(send_time,'yyyy-mm-dd') as part_day,'shop' as big_business,pm_buy*buy_num as pm_buy,pay_type,app_id
                from wow.wow_shop_order
                where true 
                and to_char(send_time,'yyyy-mm-dd') between '{0}' and '{1}'
            ) a 
            left join 
            (	-- 支付方式
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 16
            ) d1 
            on a.pay_type  = d1.item_value 
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
                and to_char(add_time,'yyyy-mm-dd') between '{0}' and '{0}'
            ) reg 
            on a.uid = reg.id and a.part_day = reg.part_day
            group by grouping sets((1,2),(1,2,3),(1,2,4),(1,2,5))
        ) t 
        """.format(start_day ,end_day)

        holo_data_list = operate_hologres.query_data(holo_sql)

        operate_hologres_log = {}
        for k, v in operate_hologres.operate_result.items():
            if v[0] != '无':
                operate_hologres_log[k] = v

        operate_hologres.close_conn()

        # 目标表 数据写入
        columns = 7
        insert_sql = "insert into wow_dws_cost_by_sendday_isreg(part_day,is_reg,pay_method,source_name,big_business,cost_money,send_users ) values ({0});".format(','.join(['%s'] * columns))
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

    for i in range(11,12):
        start_time = time.time()
        last_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1

        wow_dws_cost_by_sendday_isreg = WowDwsCostBySenddayIsreg()
        wow_dws_cost_by_sendday_isreg.run(last_day ,last_day)
        end_time = time.time()
        print(last_day, '运行耗时：', end_time - start_time)
