# _*_coding:utf-8 _*_

# @Time      : 2022/12/22  10:30
# @Author    : An
# @File      : tmp_print.py
# @Software  : PyCharm


# __author__ = 'ghk'
# -*- coding: utf-8 -*-

import time, datetime ,sys
from WorkWeixinRobot.work_weixin_robot import WWXRobot
import numpy as np
import pandas as pd
import configparser
import warnings
from datetime import date
# import yagmail
from sqlalchemy import create_engine
from pyhive import presto
import pymysql

warnings.filterwarnings("ignore")
# ------------------------数据库配置读取----------------------------
cf = configparser.ConfigParser()
if cf.read("D:/code/python/config.ini") == []:
    """服务器模式"""
    cf.read("/home/zhwom/config/config.ini")
else:
    """本地模式"""
    cf.read("D:/code/python/config.ini")

# ----------------------------------数据库（presto+redis）配置读取---------------------------------------------------
now = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
day_now_H_B = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
day_now_H_A = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

now_hour = datetime.datetime.now().hour
past_hour = now_hour - 1
now = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期

day_now = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
day_last_hb = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')  # t-2
day_last_db = (datetime.datetime.now() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')  # t-8
day_last_15 = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')  # t-15
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime('%Y-%m-%d')  # t-31
day_last_90 = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')  # t-60

month_now = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m")

print(day_now, day_last_db, day_last_hb, day_last_30, now, month_now, now_hour, past_hour)


sql_1 = """
--全网整体货架数量
select date('{0}') date_cd,
       '{0}' part_day,
       a.categoryid,
       a.yxqu,
       a.sh_type,
       a.new_hao_cnt,
       a.yx_cnt,
       b.hb_yx_cnt,
       c.tb_yx_cnt,
       d.cz_hao_cnt
from 
	(--新增货架量，有效货架量
    select t2.categoryid,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t4.yx like '%ios%' or t4.yx like '%苹果%' or t4.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct case when date(t4.ft) = date('{0}')  then t1.id end) new_hao_cnt,
	       count(distinct case when t1.zt in (0,1,2) then t1.id end ) yx_cnt
	from zhwdb.zhw_hao_archive_hour t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id
	inner join zhwdb.zhw_hao t4
	on t1.id = t4.id
	left join 
		(select distinct save_date,userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{0}'
		and sh_type > 0 
		) t3
	on t4.userid = t3.userid
	and t1.save_date = t3.save_date
	where t1.save_date = '{0}'
	group by 1,2,3
	) a
left join 
	(--环比货架量
    select t2.categoryid,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t4.yx like '%ios%' or t4.yx like '%苹果%' or t4.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct case when t1.zt in (0,1,2) then t1.id end ) hb_yx_cnt
	from zhwdb.zhw_hao_archive_hour t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id
	inner join zhwdb.zhw_hao t4
	on t1.id = t4.id
	left join 
		(select distinct save_date,userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{2}'
		and sh_type > 0 
		) t3
	on t4.userid = t3.userid
	and t1.save_date = t3.save_date
	where t1.save_date = '{2}'
	group by 1,2,3
	) b
on a.categoryid = b.categoryid
and a.sh_type = b.sh_type
and a.yxqu = b.yxqu
left join 
	(--同比货架量
    select t2.categoryid,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t4.yx like '%ios%' or t4.yx like '%苹果%' or t4.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct case when t1.zt in (0,1,2) then t1.id end ) tb_yx_cnt
	from zhwdb.zhw_hao_archive_hour t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id
	inner join zhwdb.zhw_hao t4
	on t1.id = t4.id
	left join 
		(select distinct save_date,userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{1}'
		and sh_type > 0 
		) t3
	on t4.userid = t3.userid
	and t1.save_date = t3.save_date
	where t1.save_date = '{1}'
	group by 1,2,3
	) c
on a.categoryid = c.categoryid
and a.sh_type = c.sh_type
and a.yxqu = c.yxqu
left join 
	(--1是手游 2是端游 3是页游
	select t2.categoryid,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t1.add_from = 22 then 'ios' else '安卓' end as yxqu,
	       count(distinct t1.hid) cz_hao_cnt
	from zhwdb.zhw_dingdan t1
	inner join zhwdb.zhw_game_info t2
	on t1.gameid = t2.id
	left join 
		(select distinct userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{0}'
		and sh_type > 0 
		) t3
	on t1.huserid = t3.userid
	where t1.part_day = '{0}'
	group by 1,2,3
	) d
on a.categoryid = d.categoryid
and a.sh_type = d.sh_type
and a.yxqu = d.yxqu
""".format(day_now, day_last_db, day_last_hb)

print(sql_1)
sys.exit()
result_1 = pd.read_sql(sql_1, con=db)
result_1.to_sql(name='zhw_hj_monitor_base', con=cnx_mysql, if_exists='append', index=False)
###插入指定的数据库的数据表，方式为插入，也可以改为replace替换


print(1)

sql_2 = """
--核心游戏货架
select date('{0}') date_cd,
       '{0}' part_day,
       a.title as categoryid,
       a.yxqu,
       a.sh_type,
       a.new_hao_cnt,
       a.yx_cnt,
       b.hb_yx_cnt,
       c.tb_yx_cnt,
       d.cz_hao_cnt
from 
	(select t2.title,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t4.yx like '%ios%' or t4.yx like '%苹果%' or t4.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct case when date(t4.ft) = date('{0}')  then t1.id end) new_hao_cnt,
	       count(distinct case when t1.zt in (0,1,2) then t1.id end ) yx_cnt
	from zhwdb.zhw_hao_archive_hour t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id
	inner join zhwdb.zhw_hao t4
	on t1.id = t4.id
	left join 
		(select distinct save_date,userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{0}'
		and sh_type > 0 
		) t3
	on t4.userid = t3.userid
	and t1.save_date = t3.save_date
	where t1.save_date = '{0}'
	and t2.title in ('王者荣耀','和平精英','枪战王者','火影忍者（手游）','QQ飞车(手游)','英雄联盟','绝地求生','穿越火线','逆战','CSGO','枪战王者')
	group by 1,2,3
	) a
left join 
	(select t2.title,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t4.yx like '%ios%' or t4.yx like '%苹果%' or t4.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct case when t1.zt in (0,1,2) then t1.id end ) hb_yx_cnt
	from zhwdb.zhw_hao_archive_hour t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id
	inner join zhwdb.zhw_hao t4
	on t1.id = t4.id
	left join 
		(select distinct save_date,userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{2}'
		and sh_type > 0 
		) t3
	on t4.userid = t3.userid
	and t1.save_date = t3.save_date
	where t1.save_date = '{2}'
	and t2.title in ('王者荣耀','和平精英','枪战王者','火影忍者（手游）','QQ飞车(手游)','英雄联盟','绝地求生','穿越火线','逆战','CSGO','枪战王者')
	group by 1,2,3
	) b
on a.title = b.title
and a.sh_type = b.sh_type
and a.yxqu = b.yxqu
left join 
	(select t2.title,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t4.yx like '%ios%' or t4.yx like '%苹果%' or t4.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct case when t1.zt in (0,1,2) then t1.id end ) tb_yx_cnt
	from zhwdb.zhw_hao_archive_hour t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id
	inner join zhwdb.zhw_hao t4
	on t1.id = t4.id
	left join 
		(select distinct save_date,userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{1}'
		and sh_type > 0 
		) t3
	on t4.userid = t3.userid
	and t1.save_date = t3.save_date
	where t1.save_date = '{1}'
	and t2.title in ('王者荣耀','和平精英','枪战王者','火影忍者（手游）','QQ飞车(手游)','英雄联盟','绝地求生','穿越火线','逆战','CSGO','枪战王者')
	group by 1,2,3
	) c
on a.title = c.title
and a.sh_type = c.sh_type
and a.yxqu = c.yxqu
left join 
	(--1是手游 2是端游 3是页游
	select t2.title,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t1.add_from = 22 then 'ios' else '安卓' end as yxqu,
	       count(distinct t1.hid) cz_hao_cnt
	from zhwdb.zhw_dingdan t1
	inner join zhwdb.zhw_game_info t2
	on t1.gameid = t2.id
	left join 
		(select distinct userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{0}'
		and sh_type > 0 
		) t3
	on t1.huserid = t3.userid
	where t1.part_day = '{0}'
	and t2.title in ('王者荣耀','和平精英','枪战王者','火影忍者（手游）','QQ飞车(手游)','英雄联盟','绝地求生','穿越火线','逆战','CSGO','枪战王者')
	group by 1,2,3
	) d
on a.title = d.title
and a.sh_type = d.sh_type
and a.yxqu = d.yxqu
""".format(day_now, day_last_db, day_last_hb)

result_2 = pd.read_sql(sql_2, con=db)
result_2.to_sql(name='zhw_hj_monitor_base', con=cnx_mysql, if_exists='append', index=False)
###插入指定的数据库的数据表，方式为插入，也可以改为replace替换


print(2)

sql_3 = """
--货架留存监控
select date('{0}') date_cd,
       '{0}' part_day,
       a.categoryid,
       a.yxqu,
       a.sh_type,
       a.new_hao_cnt,
       b.fb_cnt_1,
       b.lc_cnt_1,
       c.fb_cnt_7,
       c.lc_cnt_7,
       d.fb_cnt_30,
       d.lc_cnt_30
from 
    (
    --发布数量
    select t2.categoryid,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t4.yx like '%ios%' or t4.yx like '%苹果%' or t4.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct case when date(t4.ft) = date('{0}')  then t1.id end) new_hao_cnt
	from zhwdb.zhw_hao_archive_hour t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id
	inner join zhwdb.zhw_hao t4
	on t1.id = t4.id
	left join 
		(select distinct save_date,userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{0}'
		and sh_type > 0 
		) t3
	on t4.userid = t3.userid
	and t1.save_date = t3.save_date
	where t1.save_date = '{0}'
	group by 1,2,3
	) a
left join 
	(--次日留存
	select t2.categoryid,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t1.yx like '%ios%' or t1.yx like '%苹果%' or t1.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct t1.id) fb_cnt_1,
	       count(distinct t4.id) lc_cnt_1
	from zhwdb.zhw_hao t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id 
	left join 
			(select distinct userid 
			from zhwdb.zhw_shanghu_type_log_archive_day
			where save_date = '{1}'
			and sh_type > 0 
			) t3
	on t1.userid = t3.userid
	left join 
		(select distinct id
		from zhwdb.zhw_hao_archive_hour 
		where save_date = '{0}'
		and zt in (0,1,2) 
		) t4
	on t1.id = t4.id
	where t1.part_month = date_format(date('{1}'),'%Y-%m')
	and date(t1.ft) = date('{1}')
	group by 1,2,3
	) b
on a.categoryid = b.categoryid
and a.sh_type = b.sh_type
and a.yxqu = b.yxqu
left join 
	(--7日留存
	select t2.categoryid,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t1.yx like '%ios%' or t1.yx like '%苹果%' or t1.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct t1.id) fb_cnt_7,
	       count(distinct t4.id) lc_cnt_7
	from zhwdb.zhw_hao t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id 
	left join 
			(select distinct userid 
			from zhwdb.zhw_shanghu_type_log_archive_day
			where save_date = '{2}'
			and sh_type > 0 
			) t3
	on t1.userid = t3.userid
	left join 
		(select distinct id
		from zhwdb.zhw_hao_archive_hour 
		where save_date = '{0}'
		and zt in (0,1,2) 
		) t4
	on t1.id = t4.id
	where t1.part_month = date_format(date('{2}'),'%Y-%m')
	and date(t1.ft) = date('{2}')
	group by 1,2,3
	) c
on a.categoryid = c.categoryid
and a.sh_type = c.sh_type
and a.yxqu = c.yxqu
left join 
	(--30日留存
	select t2.categoryid,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t1.yx like '%ios%' or t1.yx like '%苹果%' or t1.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct t1.id) fb_cnt_30,
	       count(distinct t4.id) lc_cnt_30
	from zhwdb.zhw_hao t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id 
	left join 
			(select distinct userid 
			from zhwdb.zhw_shanghu_type_log_archive_day
			where save_date = '{3}'
			and sh_type > 0 
			) t3
	on t1.userid = t3.userid
	left join 
		(select distinct id
		from zhwdb.zhw_hao_archive_hour 
		where save_date = '{0}'
		and zt in (0,1,2) 
		) t4
	on t1.id = t4.id
	where t1.part_month = date_format(date('{3}'),'%Y-%m')
	and date(t1.ft) = date('{3}')
	group by 1,2,3
	) d
on a.categoryid = d.categoryid
and a.sh_type = d.sh_type
and a.yxqu = d.yxqu
""".format(day_now, day_last_hb, day_last_db, day_last_30)

result_3 = pd.read_sql(sql_3, con=db)
result_3.to_sql(name='zhw_hj_lc_monitor', con=cnx_mysql, if_exists='append', index=False)
###插入指定的数据库的数据表，方式为插入，也可以改为replace替换

print(3)

sql_4 = """
--核心游戏货架留存监控
select date('{0}') date_cd,
       '{0}' part_day,
       a.title as categoryid,
       a.yxqu,
       a.sh_type,
       a.new_hao_cnt,
       b.fb_cnt_1,
       b.lc_cnt_1,
       c.fb_cnt_7,
       c.lc_cnt_7,
       d.fb_cnt_30,
       d.lc_cnt_30
from 
    (
    --发布数量
    select t2.title,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t4.yx like '%ios%' or t4.yx like '%苹果%' or t4.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct case when date(t4.ft) = date('{0}')  then t1.id end) new_hao_cnt
	from zhwdb.zhw_hao_archive_hour t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id
	inner join zhwdb.zhw_hao t4
	on t1.id = t4.id
	left join 
		(select distinct save_date,userid 
		from zhwdb.zhw_shanghu_type_log_archive_day
		where save_date = '{0}'
		and sh_type > 0 
		) t3
	on t4.userid = t3.userid
	and t1.save_date = t3.save_date
	where t1.save_date = '{0}'
	and t2.title in ('王者荣耀','和平精英','枪战王者','火影忍者（手游）','QQ飞车(手游)','英雄联盟','绝地求生','穿越火线','逆战','CSGO','枪战王者')
	group by 1,2,3
	) a
left join 
	(--次日留存
	select t2.title,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t1.yx like '%ios%' or t1.yx like '%苹果%' or t1.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct t1.id) fb_cnt_1,
	       count(distinct t4.id) lc_cnt_1
	from zhwdb.zhw_hao t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id 
	left join 
			(select distinct userid 
			from zhwdb.zhw_shanghu_type_log_archive_day
			where save_date = '{1}'
			and sh_type > 0 
			) t3
	on t1.userid = t3.userid
	left join 
		(select distinct id
		from zhwdb.zhw_hao_archive_hour 
		where save_date = '{0}'
		and zt in (0,1,2) 
		) t4
	on t1.id = t4.id
	where t1.part_month = date_format(date('{1}'),'%Y-%m')
	and date(t1.ft) = date('{1}')
	and t2.title in ('王者荣耀','和平精英','枪战王者','火影忍者（手游）','QQ飞车(手游)','英雄联盟','绝地求生','穿越火线','逆战','CSGO','枪战王者')
	group by 1,2,3
	) b
on a.title = b.title
and a.sh_type = b.sh_type
and a.yxqu = b.yxqu
left join 
	(--7日留存
	select t2.title,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t1.yx like '%ios%' or t1.yx like '%苹果%' or t1.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct t1.id) fb_cnt_7,
	       count(distinct t4.id) lc_cnt_7
	from zhwdb.zhw_hao t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id 
	left join 
			(select distinct userid 
			from zhwdb.zhw_shanghu_type_log_archive_day
			where save_date = '{2}'
			and sh_type > 0 
			) t3
	on t1.userid = t3.userid
	left join 
		(select distinct id
		from zhwdb.zhw_hao_archive_hour 
		where save_date = '{0}'
		and zt in (0,1,2) 
		) t4
	on t1.id = t4.id
	where t1.part_month = date_format(date('{2}'),'%Y-%m')
	and date(t1.ft) = date('{2}')
	and t2.title in ('王者荣耀','和平精英','枪战王者','火影忍者（手游）','QQ飞车(手游)','英雄联盟','绝地求生','穿越火线','逆战','CSGO','枪战王者')
	group by 1,2,3
	) c
on a.title = c.title
and a.sh_type = c.sh_type
and a.yxqu = c.yxqu
left join 
	(--30日留存
	select t2.title,
	       case when t3.userid is not null then '商户' else '普通号主' end as sh_type,
           case when t1.yx like '%ios%' or t1.yx like '%苹果%' or t1.yx like '%IOS%' then 'ios' else '安卓' end as yxqu,
	       count(distinct t1.id) fb_cnt_30,
	       count(distinct t4.id) lc_cnt_30
	from zhwdb.zhw_hao t1
	inner join zhwdb.zhw_game_info t2
	on t1.gid = t2.id 
	left join 
			(select distinct userid 
			from zhwdb.zhw_shanghu_type_log_archive_day
			where save_date = '{3}'
			and sh_type > 0 
			) t3
	on t1.userid = t3.userid
	left join 
		(select distinct id
		from zhwdb.zhw_hao_archive_hour 
		where save_date = '{0}'
		and zt in (0,1,2) 
		) t4
	on t1.id = t4.id
	where t1.part_month = date_format(date('{3}'),'%Y-%m')
	and date(t1.ft) = date('{3}')
	and t2.title in ('王者荣耀','和平精英','枪战王者','火影忍者（手游）','QQ飞车(手游)','英雄联盟','绝地求生','穿越火线','逆战','CSGO','枪战王者')
	group by 1,2,3
	) d
on a.title = d.title
and a.sh_type = d.sh_type
and a.yxqu = d.yxqu
""".format(day_now, day_last_hb, day_last_db, day_last_30)

result_4 = pd.read_sql(sql_4, con=db)
result_4.to_sql(name='zhw_hj_lc_monitor', con=cnx_mysql, if_exists='append', index=False)

print(4)

