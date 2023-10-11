# _*_coding:utf-8 _*_

# @Time      : 2023/2/19  18:26
# @Author    : An
# @File      : defend_dwd_hao_lock_with_check_info.py
# @Software  : 封号查询明细写入 public.zhw_hao_lock_with_check_info


import time, datetime, configparser, warnings, math, platform
import sys
import pymysql, presto
# import pypresto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysqlNew import OperateMysqlNew
from OperatePresto import OperatePresto
from OperateHologresNew import OperateHologresNew



class DefendDwdHaoLockWithCheckInfo:
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
        holo_public_sql = "delete from public.zhw_hao_lock_with_check_info where to_char(add_time,'yyyy-mm-dd') between '{0}' and '{1}'".format(start_day ,end_day)
        operate_hologres.update_data(holo_public_sql)
        # print('清理目标表数据')

        # 客服添加封号-端游(日期处理)
        holo_sql = """
            insert into public.zhw_hao_lock_with_check_info (
            add_time, -- 封号添加时间
            data_soure,-- 封号数据来源
            act_id,-- 封号货架
            act_zh,-- 封号账号
            start_time,-- 封号开始时间
            end_time,-- 封号结束时间
            lock_days,-- 封号天数 
            game_id,-- 游戏ID
            game_name,-- 游戏名称
            game_type,-- 游戏类型
            order_id, -- 导致封号订单号
            lock_type,-- 封号类型
            cs_remark,-- 客服备注
            record_remark,-- 封号记录备注
            verify_id,-- 核实ID
            verify_bz,-- 核实备注
            verify_order_info,-- 核实订单信息
            record_id,-- 核实记录ID
            strategy_desc,-- 违规描述
            punish_reason,-- 封号原因
            cheat_time, -- 违规时间
            banned_type -- 封号核实类型
            )
            
            select 
                add_time,
                data_soure,
                act_id,
                act_zh,
                case 
                    when cast(start_time as text)>='2099' then (timestamp '2099-12-31 23:59:59')
                    when cast(start_time as text)<='1970' then (timestamp '1970-01-01 00:00:00')
                    else start_time 
                end as start_time,
                case 
                    when cast(end_time as text)>='2099' then (timestamp '2099-12-31 23:59:59')
                    when cast(end_time as text)<='1970' then (timestamp '1970-01-01 00:00:00')
                    else end_time 
                end as end_time,
                lock_days,
                game_id,
                game_name,
                game_type,
                order_id,
                lock_type,
                cs_remark,
                record_remark,
                verify_id,
                verify_bz,
                verify_order_info,
                record_id,
                strategy_desc,
                punish_reason,
                cheat_time,
                banned_type
            from 
            (
                select 
                    t1.add_time,
                    '客服添加封号' as data_soure, --数据来源
                    t1.act_id,
                    t1.act_zh,
                    split_part(re,'封禁到期时间 ：',2)::timestamp - (split_part(split_part(re,'游戏被封',2),'天',1)::int8)*interval '1 day' as start_time,
                    split_part(re,'封禁到期时间 ：',2)::timestamp as end_time,
                    split_part(split_part(re,'游戏被封',2),'天',1)::int8 as lock_days,
                    t1.gid as game_id,
                    t1.game as game_name,
                    '端游' game_type,
                    case when bz ~ '([6|7|8]{{1}}[0-9]{{8}})' and bz !~ '([0-9]{{10,}})' then substring(bz from '([6|7|8]{{1}}[0-9]{{8}})') end::int8 as order_id,
                    cast(t1.lock_type as text) as lock_type,-- 封号类型
                    row_number() over(partition by t1.add_time,t1.act_id order by t2.t desc) row_num,
                    remark as cs_remark,-- 客服添加备注,
                    '' as record_remark,-- 封号记录备注
                    0 as verify_id,-- 核实id
                    bz as verify_bz,-- 核实备注
                    bz as verify_order_info,-- 核实订单信息
                    t1.id as record_id, -- 封号记录ID
                    '' as strategy_desc, -- 违规行为描述,
                    '' as punish_reason,-- 处罚原因,
                    (timestamp '1970-01-01 00:00:00') as cheat_time, -- 违规时间
                    '' as banned_type
                from ods_zhw.zhw_hao_lock t1
                left join
                (
                    select b.hid,a.re,a.t
                    from ods_zhw.zhw_ts a
                    left join ods_zhw.zhw_dingdan b 
                    on a.did = b.id and b.part_day between to_char(date('{0}') -30,'yyyy-mm-dd') and '{1}'
                    where a.part_day between to_char(date('{0}') -30,'yyyy-mm-dd') and '{1}' and re like '被惩罚原因:%'
                ) t2
                on t1.act_id = t2.hid
                where t1.part_day between '{0}' and '{1}' and is_related = 1 and gid in (11,17,24)
                -- 20230516 添加逻辑 客服添加封号时间在封号时间之后 且 客服添加封号时间在投诉时间之后
                and t1.add_time>(split_part(re,'封禁到期时间 ：',2)::timestamp - (split_part(split_part(re,'游戏被封',2),'天',1)::int8)*interval '1 day')
                and t1.add_time<=t2.t
            ) t
            where row_num = 1
        """.format(start_day ,end_day)
        operate_hologres.update_data(holo_sql)
        # print('part1 客服添加封号-端游(日期处理)')
        time.sleep(1)

        # 客服添加封号-手游/绝地(日期处理)
        holo_sql2 = """
            insert into public.zhw_hao_lock_with_check_info(
            add_time,
            data_soure,
            act_id,
            act_zh,
            start_time,
            end_time,
            lock_days,
            game_id,
            game_name,
            game_type,
            order_id,
            lock_type,
            cs_remark,
            record_remark,
            verify_id,
            verify_bz,
            verify_order_info,
            record_id,
            strategy_desc,
            punish_reason,
            cheat_time,
            banned_type
            )
            select    
                add_time,
                data_soure,
                act_id,
                act_zh,
                case 
                    when cast(start_time as text)>='2099' then (timestamp '2099-12-31 23:59:59')
                    when cast(start_time as text)<='1970' then (timestamp '1970-01-01 00:00:00')
                    else start_time 
                end as start_time,
                case 
                    when cast(end_time as text)>='2099' then (timestamp '2099-12-31 23:59:59')
                    when cast(end_time as text)<='1970' then (timestamp '1970-01-01 00:00:00')
                    else end_time 
                end as end_time,
                lock_days,
                game_id,
                game_name,
                game_type,
                order_id,
                lock_type,
                cs_remark,
                record_remark,
                verify_id,
                verify_bz,
                verify_order_info,
                record_id,
                strategy_desc,
                punish_reason,
                cheat_time,
                '' as banned_type
            from 
            (
                select 
                    add_time,
                    '客服添加封号' as data_soure, --数据来源
                    act_id,
                    act_zh,
                    case when lock_start <= add_time then lock_start else add_time end as start_time,
                    lock_end as end_time,
                    lock_sec/60/60/24 as lock_days,
                    gid as game_id,
                    game as game_name,
                    case when gid = 581 then '端游' else '手游' end game_type,
                    case when bz ~ '([6|7|8]{{1}}[0-9]{{8}})' and bz !~ '([0-9]{{10,}})' then substring(bz from '([6|7|8]{{1}}[0-9]{{8}})') end::int8 as order_id,
                    cast(lock_type as text) as lock_type,-- 封号类型
                    remark as cs_remark,-- 客服添加备注
                    '' as record_remark,-- 封号记录备注
                    0 as verify_id,-- 核实id
                    bz as verify_bz,-- 核实备注
                    bz as verify_order_info,-- 核实订单信息
                    id as record_id, -- 封号记录ID
                    '' as strategy_desc, -- 违规行为描述,
                    '' as punish_reason, -- 处罚原因,
                    (timestamp '1970-01-01 00:00:00') as cheat_time -- 违规时间
                from ods_zhw.zhw_hao_lock
                where part_day between '{0}' and '{1}'
                and is_related=1 
                -- and gid in (581,443,446,683)
                -- 20230413 封号增加 9款手游
                and gid in (581,443,446,683, 699 ,449 ,636 ,1088 ,988 ,1028 ,560 ,444 ,926)
            ) a 
        """.format(start_day, end_day)
        operate_hologres.update_data(holo_sql2)
        # print('part2 客服添加封号-手游/绝地(日期处理)')
        time.sleep(1)

        # 安防查询封号(日期处理)
        holo_sql3 = """
            insert into public.zhw_hao_lock_with_check_info(
            add_time,
            data_soure,
            act_id,
            act_zh,
            start_time,
            end_time,
            lock_days,
            game_id,
            game_name,
            game_type,
            order_id,
            lock_type,
            cs_remark,
            record_remark,
            verify_id,
            verify_bz,
            verify_order_info,
            record_id,
            strategy_desc,
            punish_reason,
            cheat_time,
            banned_type
            )
            select 
                a.create_time as add_time,
                '安防查询封号' as data_soure,
                b.hid,
                b.game_account,
                a.start_stmp_time,           
                case 
                when cast(a.end_stmp_time as text)>='2099' then (timestamp '2099-12-31 23:59:59')
                when cast(a.end_stmp_time as text)<='1970' then (timestamp '1970-01-01 00:00:00')
                else a.end_stmp_time 
                end as end_stmp_time,
                a.duration/60/60/24 as lock_days,
                a.game_id,
                case when a.game_name = '穿越火线-枪战王者' then '枪战王者' else a.game_name end,
                case when a.game_id in (11,17,24,581) then '端游' else '手游' end,
                case when c.order_id ~ '([6|7|8]{{1}}[0-9]{{8}})' and c.order_id !~ '([0-9]{{10,}})' then substring(c.order_id from '([6|7|8]{{1}}[0-9]{{8}})') end::int8 as order_id,
                c.banned_type as lock_type,
                '' as cs_remark,
                a.remark as record_remark,
                c.verify_id as verify_id,
                c.other as verify_bz,
                c.order_id as verify_order_info,
                a.id as record_id,
                a.strategy_desc as strategy_desc,
                a.reason as punish_reason,
                a.cheat_time as cheat_time,
                c.banned_type
            from ods_zhw.game_cheat_account_record a
            left join ods_zhw.game_cheat_account_info b on a.game_account_id=b.id
            left join ods_zhw.game_cheat_account_record_verify c on  a.id = c.record_id
            where type like '%封号%'
            -- and a.game_id in (11,17,24,581,443,446,683)
            -- 20230413 封号增加 9款手游
            and a.game_id in (11,17,24,581,443,446,683 ,699 ,449 ,636 ,1088 ,988 ,1028 ,560 ,444 ,926)
            and to_char(a.create_time,'YYYY-MM-DD') between '{0}' and '{1}'
            and (a.fpt = a.pt or a.pt = -1) and a.game_id = b.game_id -- 2023/3/26 1721 添加条件
        """.format(start_day, end_day)
        operate_hologres.update_data(holo_sql3)
        # print('part3 安防查询封号(日期处理)')
        time.sleep(1)

        # 安防查询封号-steam
        holo_sql4 = """
            insert into public.zhw_hao_lock_with_check_info (
            add_time,
            data_soure,
            act_id,
            act_zh,
            start_time,
            end_time,
            lock_days,
            game_id,
            game_name,
            game_type,
            order_id,
            lock_type,
            cs_remark,
            record_remark,
            verify_id,
            verify_bz,
            verify_order_info,
            record_id,
            strategy_desc,
            punish_reason,
            cheat_time,
            banned_type
            )
            select 
                a.create_time as add_time,
                '安防查询封号-steam' as data_soure,
                b.hid,
                b.game_account,
                a.start_stmp_time,
                a.end_stmp_time,
                a.duration/60/60/24 as lock_days,
                a.game_id,
                '绝地求生',
                '端游' game_type,
                case when c.order_id ~ '([6|7|8]{{1}}[0-9]{{8}})' and c.order_id !~ '([0-9]{{10,}})' then substring(c.order_id from '([6|7|8]{{1}}[0-9]{{8}})') end::int8 as order_id,
                c.banned_type as lock_type,
                '' as cs_remark,
                a.remark as record_remark,
                c.verify_id as verify_id,
                c.other as verify_bz,
                c.order_id as verify_order_info,
                a.id as record_id,
                '' as strategy_desc,
                a.reason as punish_reason,
                (timestamp '1970-01-01 00:00:00') as cheat_time,
                c.banned_type
            from ods_zhw.game_cheat_steam_record a
            left join ods_zhw.game_cheat_account_info b on a.game_account_id=b.id
            left join ods_zhw.game_cheat_account_record_verify c on  a.id = c.record_id
            where a.game_id = 581 and to_char(a.create_time,'YYYY-MM-DD') between '{0}' and '{1}'
        """.format(start_day, end_day)
        operate_hologres.update_data(holo_sql4)
        # print('part4 安防查询封号-steam')
        time.sleep(1)
        operate_hologres.close_conn()

if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    today_last_days_ = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # d

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    start_time = time.time()
    start_end_list = [
        ('2023-01-01', '2023-01-16'),
        ('2023-01-17', '2023-02-01'),
        ('2023-02-02', '2023-02-17'),
        ('2023-02-18', '2023-03-05'),
        ('2023-03-06', '2023-03-21'),
        ('2023-03-22', '2023-04-06'),
        ('2023-04-07', '2023-04-22'),
        ('2023-04-23', '2023-05-08'),
        ('2023-05-09', '2023-05-24'),
        ('2023-05-25', '2023-06-09'),
        ('2023-06-10', '2023-06-25'),
        ('2023-06-26', '2023-07-11'),
        ('2023-07-12', '2023-07-27')
    ]
    for idx,tup in enumerate(start_end_list):
        print(idx+1)
        start_day, last_day = tup[0], tup[1]
        zhw_hao_lock_with_check_info = DefendDwdHaoLockWithCheckInfo()
        zhw_hao_lock_with_check_info.run(start_day ,last_day)
        time.sleep(1)
    end_time = time.time()

    print('运行耗时：', end_time - start_time)