# _*_coding:utf-8 _*_

# @Time      : 2023/7/6  15:34
# @Author    : An
# @File      : gg_fenghao_orders_group.py
# @Software  : 日常指标预警

import time, datetime, configparser, warnings, math, platform , psycopg2
import sys
import pandas as pd
import pymysql, presto
from pyhive import presto
from sqlalchemy import create_engine
import json, requests

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

from OperateMysql import OperateMysql
from OperatePresto import OperatePresto
from SchedualToMysql import SchedualInfo
from OperateHologresNew import OperateHologresNew

from QqexmailSmtpAttach import QqExmailSmtp

warnings.filterwarnings("ignore")
# ------------------------数据库配置读取----------------------------
cf = configparser.ConfigParser()
if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

# hologres数据库
holo_host = cf.get("hologres-dofun", "host")
holo_port = cf.get("hologres-dofun", "port")
holo_database = cf.get("hologres-dofun", "DB")
holo_user = cf.get("hologres-dofun", "user")
holo_password = cf.get("hologres-dofun", "password")
holo_cnx = create_engine("postgresql+psycopg2://"+holo_user+":"+holo_password+"@"+holo_host+":"+holo_port+"/" + holo_database )

# presto数据库
host = cf.get("hive_presto_hive", "host")
username = cf.get("hive_presto_hive", "username")
port = cf.get("hive_presto_hive", "port")
schema = cf.get("hive_presto_hive", "schema")
catalog = cf.get("hive_presto_hive", "catalog")
presto_db = presto.connect(host=host, port=port, username=username, schema=schema, catalog=catalog)

current_time = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d %H:%M:%S')
current_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')

# 数据窗口日期
start_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')
end_day = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d')

part_day_h = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
part_day_t = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

# p1 封号率 按 日期、游戏、类型
banratio_dim_day_game = '''
    select 
        coalesce(t1.part_day,t2.part_day) as part_day,
        coalesce(t1.title,t2.title) as game_name,
        coalesce(t1.category,t2.category) as category,
        max(t1.orders) as orders,
        coalesce(max(t2.gt_7d_bans),0) as gt_7d_bans
    from
    (
        select 
            a.part_day,
            b.title,
            case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
            count(distinct hid) as hids,
            count(a.id) as orders
        from ods_zhw.zhw_dingdan a
        left join ods_zhw.zhw_game_info b 
        on a.gameid = b.id
        where a.part_day between '{0}' and '{1}' 
        and a.gameid in (443,446,683,11,17,24,581)
        group by 1,2,3
    ) t1
    full join
    (
        select 
            to_char(a.start_time,'yyyy-mm-dd') as part_day,
            b.title,
            case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
            count(distinct act_zh) as gt_7d_bans
        from ods_zhw.zhw_hao_lock_details a 
        left join ods_zhw.zhw_game_info b 
        on a.game_id = b.id
        where extract(epoch from a.end_time) - extract(epoch from a.start_time)>7*24*3600
        and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}' 
        and a.game_id in (443,446,683,11,17,24,581)
        group by 1,2,3 
    ) t2 
    on t1.part_day = t2.part_day and t1.title=t2.title and t1.category = t2.category
    group by 1,2,3
'''.format(start_day,end_day)

# p2 封号数 按 日期、游戏、类型
bans_dim_day_game_gap = '''
	select 
		to_char(a.start_time,'yyyy-mm-dd') as part_day,
		b.title,
		case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
		count(distinct case when a.lock_days=3 then act_zh end) as eq_3d_bans,
		count(distinct case when a.lock_days>3 then act_zh end) as gt_7d_bans
	from ods_zhw.zhw_hao_lock_details a 
	left join ods_zhw.zhw_game_info b 
	on a.game_id = b.id
	where true 
	and (lock_days=3 or lock_days>7)
	and to_char(a.start_time,'yyyy-mm-dd')  between '{0}' and '{1}' 
	and a.game_id in (443,446,683,11,17,24,581)
	group by 1,2,3
'''.format(start_day,end_day)

# p3 封号数 按 日期、游戏、封号原因
bans_dim_day_game_failreason = '''
    select 
        to_char(a.start_time,'yyyy-mm-dd') as part_day,
        b.title as game_name,
        a.punish_reason,
        count(distinct a.act_zh) as bans,
        count(distinct case when a.lock_days = 3 then a.act_zh end) as eq_3d_bans,
        count(distinct case when a.lock_days > 7 then a.act_zh end) as gt_7d_bans
    from ods_zhw.zhw_hao_lock_details a 
    left join ods_zhw.zhw_game_info b 
    on a.game_id = b.id 
    where a.data_soure = '安防查询封号'
    and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}' 
    and (a.lock_days = 3 or a.lock_days>7)
    and a.game_id in (443,446,683,11,17,24,581)
    group by 1,2,3
'''.format(start_day,end_day)

# p4 新封号查询数 按 日期、游戏
banqueries_dim_day_game = '''
    select  
         a.part_day,
         b.title as game_name,
         count(1) "查询次数",
         sum(case when a.task_status=30 then 1 end) "查询成功数"
    from kudu.safe_center.game_cheat_query_task a
    left join kudu.zhwdb.zhw_game_info b 
    on a.game_id=b.id
    where a.task_type=0 /*任务类型=封号查询*/
    and a.source=28     /*来源=新封号*/
    and a.part_day between '{0}' and '{1}' 
    group by 1,2
'''.format(start_day,end_day)

# p5 新封号查询失败数 按 日期、游戏、失败原因
queryfail_dim_day_game_reason = '''
    select  
        a.part_day,
        b.title as game_name,
        a.task_msg as fail_reason,
        count(1) as times
    from kudu.safe_center.game_cheat_query_task a
    left join kudu.zhwdb.zhw_game_info b 
    on a.game_id=b.id
    where a.task_type=0 --任务类型=封号查询
    and a.source=28     --来源=新封号
    and a.part_day between '{0}' and '{1}' 
    and a.task_status !=30 --查询失败
    group by 1,2,3
'''.format(start_day,end_day)

banratio_dim_day_game_df = pd.read_sql(banratio_dim_day_game, con=holo_cnx) # p1 封号率 按 日期、游戏、类型
mobile_df_

print(banratio_dim_day_game_df)
bans_dim_day_game_gap = pd.read_sql(bans_dim_day_game_gap, con=holo_cnx) # p2 封号数 按 日期、游戏、类型
print(bans_dim_day_game_gap)
banquery_dim_day_game_df = pd.read_sql(bans_dim_day_game_failreason, con=holo_cnx) # p3 封号数 按 日期、游戏、封号原因
print(banquery_dim_day_game_df)
banqueries_dim_day_game_df = pd.read_sql(banqueries_dim_day_game, con=presto_db) # p4 新封号查询数 按 日期、游戏
print(banqueries_dim_day_game_df)
queryfail_dim_day_game_reason_df = pd.read_sql(queryfail_dim_day_game_reason, con=presto_db) # p5 新封号查询失败数 按 日期、游戏、失败原因
print(queryfail_dim_day_game_reason_df)
