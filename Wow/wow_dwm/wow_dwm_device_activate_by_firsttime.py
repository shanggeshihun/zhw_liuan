# _*_coding:utf-8 _*_

# @Time      : 2022/11/22  18:26
# @Author    : An
# @File      : wow_dwm_device_activate_by_firsttime.py
# @Software  : PyCharm


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


class WowDwmDeviceActivateByFirsttime:
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
        # mysql_sql = "delete from wow_dwm_device_activate_by_firsttime where part_day between '{0}' and '{1}'".format(start_day, end_day)
        mysql_sql = "delete from wow_dwm_device_activate_by_firsttime where true".format(start_day)
        operate_mysql.delete_data(mysql_sql)

        # 原始表 数据处理
        holo_sql = """
            with tmp_device_reguid_mapping as (
                -- 通过点击登录建立注册用户id与设备id的关系
                -- 存在一个设备多个注册用户
                select c.device,b.id as uid,b.app_id,
                b.part_day,b.add_time,row_number()over(partition by c.device order by id asc) as regtime_rn  
                from 
                (
                    select id,
                    case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    to_char(add_time,'yyyy-mm-dd') as part_day,add_time
                    from wow.wow_user
                    where true 
                ) b 
                join 
                (
                    select part_day,uid,device
                    from 
                    (
                        select uid,device,to_char(addtime,'yyyy-mm-dd') as part_day,row_number()over(partition by to_char(addtime,'yyyy-mm-dd'),uid order by id desc) as rn 
                        from wow.wow_user_login_log
                    ) a 
                    where rn = 1 and device is not null 
                ) c 
                on b.id = c.uid and b.part_day = c.part_day
            )
            
            select device_id,coalesce(cast(app_id as integer),99999999) as app_id,coalesce(d2.item_name,'其他') as source_name,uid,version,os,os_version,manufacturing,device_name,ip,addtime,part_day,is_reg
            from 
            (
                select 
                    a.device_id,
                    a.app_id,
                    a.uid,a.version,a.os,a.os_version,a.manufacturing,a.device_name,a.ip,a.addtime,a.part_day,
                    case when d.device is not null then 1 else 0 end as is_reg
                from 
                (
                    -- 首次激活的设备
                    select device_id,case when app_id is null or app_id = '' then '99999999' else app_id end as app_id,
                    uid,version,os,os_version,manufacturing,device_name,ip,addtime,part_day
                    from 
                    (
                        select a.*,row_number()over(partition by a.device_id order by a.addtime asc) as rn 
                        from 
                        (
                            select * 
                            from wow.wow_stat_info 
                            where true 
                            and event_id = 'app_first_open_activation'
                        ) a
                    ) t 
                    where t.rn = 1 
                ) a 
                left join 
                (
                    select device,uid,app_id,part_day,add_time,regtime_rn
                    from tmp_device_reguid_mapping 
                    where regtime_rn = 1 
                ) d 
                on a.device_id = d.device and a.part_day = d.part_day
            ) c 
            left join 
            (	-- 应用来源（安卓、iOS、小米、华为、H5）
                select 
                cast(item_value as integer) as item_value,
                item_name
                from wow.wow_dict_item 
                where dict_id  = 3
            ) d2 
            on cast(c.app_id as integer)  = d2.item_value
        """.format(start_day, end_day)
        holo_data_list = operate_hologres.query_data(holo_sql)

        operate_hologres_log = {}
        for k, v in operate_hologres.operate_result.items():
            if v[0] != '无':
                operate_hologres_log[k] = v

        operate_hologres.close_conn()

        # 目标表 数据写入
        columns = 13
        insert_sql = "insert into wow_dwm_device_activate_by_firsttime(device_id ,app_id ,source_name ,uid ,version ,os ,os_version ,manufacturing ,device_name ,ip ,addtime ,part_day ,is_reg ) values ({0});".format(','.join(['%s'] * columns))
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

        wow_dwm_device_activate_by_firsttime = WowDwmDeviceActivateByFirsttime()
        wow_dwm_device_activate_by_firsttime.run(last_day, last_day)
        end_time = time.time()
        print(last_day, '运行耗时：', end_time - start_time)