# _*_coding:utf-8 _*_

# @Time      : 2023/3/24  17:30
# @Author    : An
# @File      : shouyou_fenghao_match_group.py
# @Software  : PyCharm


import time, datetime
import pandas as pd
import requests
import redis
import json
from pyhive import presto
import psycopg2
from sqlalchemy import create_engine
import configparser
from dateutil.relativedelta import relativedelta
from collections import defaultdict

# ------------------------参数变量区----------------------------
# 配置运营数据库地址
cf = configparser.ConfigParser()
if cf.read("D:/code/python/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/usr/model/zhw_product/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("D:/code/python/config.ini", encoding='utf-8')

bigdata_redis_host = cf.get("bigdata_redis", "host")
bigdata_redis_port = cf.get("bigdata_redis", "port")
bigdata_redis_password = cf.get("bigdata_redis", "password")
bigdata_redis_db = cf.get("bigdata_redis", "user_db")

host = cf.get("hive_presto", "host")
username = cf.get("hive_presto", "username")
port = cf.get("hive_presto", "port")
schema = cf.get("hive_presto", "schema")
catalog = cf.get("hive_presto", "catalog")
presto_db = presto.connect(host=host, port=port, username=username, schema=schema, catalog=catalog)

holo_host = cf.get("Hologres", "host")
holo_user = cf.get("Hologres", "user")
holo_password = cf.get("Hologres", "password")
holo_DB = cf.get("Hologres", "db")
holo_port = cf.get("Hologres", "port")
holo_cnx = create_engine(
    "postgresql+psycopg2://" + holo_user + ":" + holo_password + "@" + holo_host + ":" + holo_port + "/" + holo_DB)

wj_host = cf.get("Mysql-sjwj", "host")
wj_user = cf.get("Mysql-sjwj", "user")
wj_password = cf.get("Mysql-sjwj", "password")
wj_DB = cf.get("Mysql-sjwj", "DB")
wj_port = cf.get("Mysql-sjwj", "port")
cnx = create_engine("mysql+pymysql://" + wj_user + ":" + wj_password + "@" + wj_host + ":" + wj_port + "/" + wj_DB,
                    echo=False)

wj_host = cf.get("Mysql-Database-test", "host")
wj_user = cf.get("Mysql-Database-test", "user")
wj_password = cf.get("Mysql-Database-test", "password")
wj_DB = cf.get("Mysql-Database-test", "DB")
# wj_port = cf.get("Mysql-Database-test","port")
test_cnx = create_engine("mysql+pymysql://" + wj_user + ":" + wj_password + "@" + wj_host + ":" + wj_port + "/" + wj_DB,
                         echo=False)
# ------------------------参数配置----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
day_last_2 = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')  # t-2
day_last_3 = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')  # t-3
day_last_7 = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')  # t-7
day_last_15 = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')  # t-30
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')  # t-30
day_last_365 = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')  # t-30
month_last_3 = (datetime.date.today() - relativedelta(months=+3)).strftime('%Y%m%d')

day_now_H = (datetime.datetime.now()).strftime('%Y%m%d%H')  # h
day_now_H_Last = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h-1
day_now_H_Last_2 = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime('%Y%m%d%H')  # h-2
day_now_H_Last_4 = (datetime.datetime.now() - datetime.timedelta(hours=4)).strftime('%Y%m%d%H')  # h-4
day_now_H_Last_13 = (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime('%Y%m%d%H')  # h-12
day_now_H_Last_24 = (datetime.datetime.now() - datetime.timedelta(hours=24)).strftime('%Y%m%d%H')  # h-24

month = (datetime.date.today()).strftime('%Y-%m')  # t-1 #当月日期
last_month = (datetime.date.today() - relativedelta(months=+1)).strftime('%Y-%m')  # t-1 #上月日期
last_month_1 = (datetime.date.today() - relativedelta(months=+2)).strftime('%Y-%m')  # t-2
day_Week = datetime.datetime.now().weekday()
pre_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).hour


# ----------------------配置信息-------------------------
# mysql -h rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com -u datawj -P 3306 -p
# 密码：oKSq77hSJKMX825GFL

# insert into safe_center_lock_target_activity_manage (place_type_more,place_type,group_code,group_name,group_statue_code,abtest_scale)values('1',1,'pc_game_cancel_order_user_group_3','pc游戏撤单率高导致封号用户群（关闭3天）',1,0);


def shouyou_fenghao_match_group(day_last_1, now_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:
    '''
    sql = """    	
		with tmp_hao_lock_details as (
			select (case when hao_ext::json->>'zh' is not null then hao_ext::json->>'zh' else hao_ext::json->>'act_zh' end) as act_zh,start_time,gid as game_id,event_time as add_time
			from dw.t_dwd_user_event
			where event_name ='account_banned'
			and gid in (443,446,683)
			and invertal_seconds>7*24*3600
			and part_day>= to_char(date('{0}') - interval '30 days', 'yyyy-mm-dd')
		),
		zc_tmp as (
			select a.*,
			c.id as m_order,c.hid as m_hid,c.ip as m_ip,c.gameid as m_gameid,c.userid as m_userid,c.huserid as m_huserid,c.stimer as m_stimer,c.etimer as m_etimer,
			c.item_name as addfrom_name,c.zh as m_zh,
			g.title as game_name_m,
			1 as pn
			from tmp_hao_lock_details a 
			join 
			(
				select c.id,c.hid,c.ip,c.gameid,case when f.order_id is null then c.userid else f.username end as userid,c.huserid,c.stimer,c.etimer,c.add_from,d.zh,
				e.item_name
				from 
				(
					select id,hid,ip,gameid,userid,huserid,stimer,etimer,part_day,add_from
					from ods_zhw.zhw_dingdan 
					where true 
					and gameid in (443,446,683)
					and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and zt = 2 
				) c
				join 
				(
					select cast(item_value as int) as key,item_name 
					from ods_zhw.zhw_dict_item 
					where dict_id = 51
				) e
				on c.add_from = e.key
				join
				(
					select id,save_date,zh,gid
					from ods_zhw.zhw_hao_archive_day
					where true 
					and save_date between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and gid in (443,446,683)
					union all
					select id,to_char(current_date,'yyyy-mm-dd') as save_date,zh,gid
					from ods_zhw.zhw_hao
					where true
					and gid in (443,446,683)
				) d
				on c.hid = d.id and c.part_day = d.save_date and c.gameid = d.gid
				left join 
				(
					select order_id,username
					from ods_zhw.zhw_fx_order 
					where true 
					and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
				) f 
				on c.id = f.order_id
			) c
			on c.zh = a.act_zh and a.start_time between c.stimer and c.etimer and 
			left join ods_zhw.zhw_game_info g 
			on a.game_id = g.id 
			where to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}' -- 其实封号时间
		) ,
		cd_tmp as (
			select a.*,
			c.id as m_order,c.hid as m_hid,c.ip as m_ip,c.gameid as m_gameid,c.userid as m_userid,c.huserid as m_huserid,c.stimer as m_stimer,c.etimer as m_etimer,
			c.item_name as addfrom_name,c.zh as m_zh,
			g.title as game_name_m,
			2 as pn
			from tmp_hao_lock_details a 
			join 
			(
				select c.id,c.hid,c.ip,c.gameid,case when f.order_id is null then c.userid else f.username end as userid,c.huserid,c.stimer,c.etimer,c.add_from,d.zh,
				b.t,e.item_name
				from 
				(
					select id,hid,ip,gameid,userid,huserid,stimer,etimer,part_day,add_from
					from ods_zhw.zhw_dingdan 
					where true 
					and gameid in (443,446,683)
					and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and zt = 3
				) c
				join 
				(
					select cast(item_value as int) as key,item_name 
					from ods_zhw.zhw_dict_item 
					where dict_id = 51
				) e
				on c.add_from = e.key
				join 
				(

					select did,t
					from ods_zhw.zhw_ts
					where true 
					and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and gameid in (443,446,683)
				) b
				on c.id = b.did
				join
				(
					select id,save_date,zh,gid
					from ods_zhw.zhw_hao_archive_day
					where true 
					and save_date between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and gid in (443,446,683)
					union all
					select id,to_char(current_date,'yyyy-mm-dd') as save_date,zh,gid
					from ods_zhw.zhw_hao
					where true
					and gid in (443,446,683)
				) d
				on c.hid = d.id and c.part_day = d.save_date and c.gameid = d.gid
				left join 
				(
					select order_id,username
					from ods_zhw.zhw_fx_order 
					where true 
					and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
				) f 
				on c.id = f.order_id
			) c
			on c.zh = a.act_zh and a.start_time between c.stimer and c.t
			left join ods_zhw.zhw_game_info g 
			on a.game_id = g.id 
			where to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}'
		),
		zc_cd_tmp as (
			select *
			from 
			(
				select *,row_number()over(partition by add_time,act_zh order by start_time asc,pn asc ) as rn 
				from 
				(
					select *
					from zc_tmp
					union all 
					select *
					from cd_tmp
				) aa 
			) tt
			where rn = 1 
		)


		select
			{2} as fk_target_id,
			1 as fk_module_type,
			t2.id as fk_uid,
			t1.userid,
			t1.userip as userip,
			t1.gameid as gameid,
			t1.did as did,
			0 as user_mark,
			0 as push_no,
			cast (now() as timestamp)  as push_time,
			0 as is_inc,
			0 as deal_flag 
			from
		(
			select m_order as did,m_userid as userid ,m_ip as userip,m_gameid as gameid
			from zc_cd_tmp
			where true 
			addfrom_name not in('分销合伙人','新转租平台','分销普通版','分销尊享版','分销高级版')
			and m_order not in (select did from ods_zhw.zhw_app_oaid where oaid_sh='0000000000000000|' or oaid_xd ='0000000000000000|')
		) t1 
		left join
		(
			select id ,jkx_userid from ods_zhw.zhw_user
		) t2
		on t1.userid=t2.jkx_userid
		left join
		(
			select userid from public.zhw_shanghu_type_all group by 1
		) t3
		on t1.userid=t3.userid
		left join
		(
			select userid from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2} group by 1
		) t4
		on t1.userid=t4.userid
		left join
		(
			select userid from ods_zhw.zhw_fx_sublet_kf where status=1
		) t5
		on t1.userid=t5.userid
		where t3.userid is null
		and t4.userid is null
		and t5.userid is null 
    """.format(day_last_1, now_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)

    print(report)
    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                      'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """2023-02-22，针对手游查询到的封号匹配出对应封号订单的租客，限制租赁3款手游"""
    try:
        shouyou_fenghao_match_group(day_last_1, now_day, 44)
    except Exception as e:
        print('shouyou_fenghao_match_group 异常信息:\n', e)
    pass
