# _*_coding:utf-8 _*_

# @Time      : 2022/11/22  18:26
# @Author    : An
# @File      : wow_dwm_recharge_measures_by_addday_status.py
# @Software  : wow_dwm_recharge_measures_by_addday_status 涉及 滞后更新的数据指标 退款


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


class WowDwmRechargeMeasuresByAdddayStatus:
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

    def run(self, start_day, end_day):
        """
        :param start_day: 日期参数
        :param end_day: 日期参数
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
        mysql_sql = "delete from wow_dwm_recharge_measures_by_addday_status where part_day between '{0}' and '{1}'".format(start_day, end_day)
        operate_mysql.delete_data(mysql_sql)

        # 原始表 数据处理
        holo_sql = """
        select part_day ,coalesce(pay_method,'-') as pay_method ,coalesce(source_name,'-') as source_name ,coalesce(recharge_status,'-') as recharge_status ,
        coalesce(big_business,'-') as big_business , coalesce(business,'-') as business ,
        recharge_money ,recharge_times ,recharge_users ,refund_money ,refund_times ,refund_users
        from 
        (
            select 
                a.part_day as part_day,
                coalesce(d1.item_name,'其他') as pay_method,
                coalesce(d2.item_name,'其他') as source_name,
                case when a.status = 1  then '待支付' when a.status = 2 then '支付成功' when a.status = 3 then '支付失败' else '其他' end as recharge_status,
                case when a.business_code in (10000,30000) then 'war' when a.business_code in (20000,30001) then 'shop' else '其他' end as big_business,
                case 
                    when a.business_code = 10000 then '抽盒订单' 
                    when a.business_code = 20000 then '商城订单' 
                    when a.business_code = 30000 then '盲盒发货运费'
                    when a.business_code = 30001 then '商城发货运费'
                    else '其他' 
                end as business,
                sum(a.money/100.00) as recharge_money,
                count(a.out_trade_no) as recharge_times,
                count(distinct a.uid) as recharge_users,
                sum(a.return_money/100.00) as refund_money,
                count(case when a.return_money>0 then a.out_trade_no end) as refund_times,
                count(distinct case when a.return_money>0 then a.uid end) as refund_users
            from 
            (
                select status,business_code,money,out_trade_no,uid,return_money,pay_way,
                case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                to_char(addtime,'yyyy-mm-dd') as part_day
                from wow.wow_recharge
                where true 
                and to_char(addtime,'yyyy-mm-dd') between '{0}' and '{1}'
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
            (	-- 支付方式
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 16
            ) d1 
            on a.pay_way  = d1.item_value 
            group by grouping sets((1,4),(1,4,2),(1,4,3),(1,4,5),(1,4,6))
        ) t 
        """.format(start_day, end_day)
        holo_data_list = operate_hologres.query_data(holo_sql)

        operate_hologres_log = {}
        for k, v in operate_hologres.operate_result.items():
            if v[0] != '无':
                operate_hologres_log[k] = v

        operate_hologres.close_conn()

        # 目标表 数据写入
        columns = 12
        insert_sql = "insert into wow_dwm_recharge_measures_by_addday_status(part_day ,pay_method ,source_name ,recharge_status ,big_business ,business ,recharge_money ,recharge_times ,recharge_users ,refund_money ,refund_times ,refund_users) values ({0});".format(','.join(['%s'] * columns))
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
    lst = list(range(1,2))
    lst.reverse()
    for i in lst:
        start_time = time.time()
        last_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
        last_8day = (datetime.datetime.now() - datetime.timedelta(days=i+8)).strftime('%Y-%m-%d')

        wow_dwm_recharge_measures_by_addday_status = WowDwmRechargeMeasuresByAdddayStatus()
        wow_dwm_recharge_measures_by_addday_status.run(last_8day, last_day)
        end_time = time.time()
        print(last_day, '运行耗时：', end_time - start_time)
