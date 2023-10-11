# _*_coding:utf-8 _*_

# @Time      : 2022/12/7  15:34
# @Author    : An
# @File      : wow_dwm_war_recharge_by_prizeday.py
# @Software  : 该主题涉及可回收奖品状态，每日更新前8日的数据(鉴于 抽盒所得商品7日内未手动分解将会自动分解)


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


class WowDwmWarRechargeByPrizeday:
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

    def run(self, part_day):
        """
        :param part_day: 日期参数
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
        mysql_sql = "delete from wow_dwm_war_recharge_by_prizeday where part_day between date_format(date_add(str_to_date('{0}','%Y-%m-%d'), interval -7 day),'%Y-%m-%d') and '{0}'".format(part_day)
        operate_mysql.delete_data(mysql_sql)

        # 原始表 数据处理
        holo_sql = """
        select part_day ,coalesce(source_name,'-') as source_name ,coalesce(model_name,'-') as model_name ,
        will_rec_prize_times ,will_rec_prize_users ,will_rec_prize_money ,will_rec_use_red_packet
        from 
        (
            select 
                a.part_day,
                coalesce(d2.item_name,'其他') as source_name,
                case when a.m_type = 1  then '无限' when a.m_type = 2 then '欧皇' else '其他' end as model_name,
                count(case when a.status = 100 then id end) as will_rec_prize_times,
                count(distinct case when a.status = 100 then uid end) as will_rec_prize_users,
                sum(case when a.status = 100 then a.pm/100.00 end) as will_rec_prize_money,
                sum(case when a.status = 100 then a.pm_red_packet/100.00 end) as will_rec_use_red_packet
            from 
            (
                select m_type,status,id,uid,pm,pm_red_packet,
                case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                to_char(addtime,'yyyy-mm-dd') as part_day
                from wow.wow_war
                where true 
                and to_char(addtime,'yyyy-mm-dd') between to_char(date '{0}' - integer '7','yyyy-mm-dd') and '{0}'
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
            group by grouping sets((1),(1,2),(1,3),(1,2,3))
        ) t 
        """.format(part_day)
        holo_data_list = operate_hologres.query_data(holo_sql)

        operate_hologres_log = {}
        for k, v in operate_hologres.operate_result.items():
            if v[0] != '无':
                operate_hologres_log[k] = v

        operate_hologres.close_conn()

        # 目标表 数据写入
        columns = 7
        insert_sql = "insert into wow_dwm_war_recharge_by_prizeday(part_day ,source_name ,model_name ,will_rec_prize_times ,will_rec_prize_users ,will_rec_prize_money ,will_rec_use_red_packet) values ({0});".format(','.join(['%s'] * columns))
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

    for i in range(1, 17):
        start_time = time.time()
        day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1

        wow_dwm_war_recharge_by_prizeday = WowDwmWarRechargeByPrizeday()
        wow_dwm_war_recharge_by_prizeday.run(day_last_1)
        end_time = time.time()
        print(day_last_1, '运行耗时：', end_time - start_time)
