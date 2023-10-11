# _*_coding:utf-8 _*_

# @Time      : 2023/3/24  17:30
# @Author    : An
# @File      : shouyou_fenghao_match_for_sh_group.py
# @Software  : PyCharm

# 20230406 1836 按照检测时间推送

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


"""
20230516 订单匹配逻辑依赖的数据源仅从新封号查询获取
"""


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

holo_host = cf.get("Hologres_defend_r", "host")
holo_user = cf.get("Hologres_defend_r", "user")
holo_password = cf.get("Hologres_defend_r", "password")
holo_DB = cf.get("Hologres_defend_r", "db")
holo_port = cf.get("Hologres_defend_r", "port")
holo_cnx = create_engine(
    "postgresql+psycopg2://" + holo_user + ":" + holo_password + "@" + holo_host + ":" + holo_port + "/" + holo_DB)

wj_host = cf.get("Mysql-sjwj", "host")
wj_user = cf.get("Mysql-sjwj", "user")
wj_password = cf.get("Mysql-sjwj", "password")
wj_DB = cf.get("Mysql-sjwj", "DB")
wj_port = cf.get("Mysql-sjwj", "port")
cnx = create_engine("mysql+pymysql://" + wj_user + ":" + wj_password + "@" + wj_host + ":" + wj_port + "/" + wj_DB,
                    echo=False)


# ------------------------参数配置----------------------------

def shouyou_fenghao_match_for_sh_group(start_day, end_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:针对手游查询到的封号匹配出对应封号订单，针对商户订单 封禁上号OAID
    '''
    sql = """    	
		with tmp_hao_lock_details as (
            select 
               b.game_account as act_zh,
               a.start_stmp_time as start_time,
               a.game_id,
               a.create_time as add_time
            from ods_zhw.game_cheat_account_record a
            left join ods_zhw.game_cheat_account_info b on a.game_account_id=b.id
            where a.type like '%封号%'
            and a.duration/60/60/24>7
            and a.game_id in (443,446,683)
            and (a.fpt = a.pt or a.pt = -1) and a.game_id = b.game_id
            and to_char(a.create_time,'YYYY-MM-DD') >= to_char(date('{0}') - interval '30 days', 'yyyy-mm-dd')
            and to_char(a.create_time,'YYYY-MM-DD') = '{0}' 
            group by 1,2,3,4
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
					select id,save_date,zh
					from ods_zhw.zhw_hao_archive_day
					where true 
					and save_date between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and gid in (443,446,683)
					union all
					select id,to_char(current_date,'yyyy-mm-dd') as save_date,zh
					from ods_zhw.zhw_hao
					where true
					and gid in (443,446,683)
				) d
				on c.hid = d.id and c.part_day = d.save_date
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
			on c.zh = a.act_zh and a.start_time between c.stimer and c.etimer
			left join ods_zhw.zhw_game_info g 
			on a.game_id = g.id 
			where true 
			-- and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}' -- 其实封号时间
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
					select id,save_date,zh
					from ods_zhw.zhw_hao_archive_day
					where true 
					and save_date between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and gid in (443,446,683)
					union all
					select id,to_char(current_date,'yyyy-mm-dd') as save_date,zh
					from ods_zhw.zhw_hao
					where true
					and gid in (443,446,683)
				) d
				on c.hid = d.id and c.part_day = d.save_date
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
			where true 
			-- and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}'
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
			and m_order not in (select did from ods_zhw.zhw_app_oaid where oaid_sh='0000000000000000|' or oaid_xd ='0000000000000000|')
		) t1 
		left join
		(
			select id ,jkx_userid from ods_zhw.zhw_user
		) t2
		on t1.userid=t2.jkx_userid
        left join
        (
            select userid from public.zhw_shanghu_type_all  where save_date = to_char(current_date-1,'yyyy-mm-dd') group by 1
        ) t3
        on t1.userid=t3.userid
        left join
        (
            select did from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2} group by 1
        ) t4
        on t1.did=t4.did
        -- left join
        -- (
        --     select userid from ods_zhw.zhw_fx_sublet_kf where status=1
        -- ) t5
        -- on t1.userid=t5.userid
        where t3.userid is not null -- 商户身份
        and t4.did is null -- 已推送的订单
        -- and t5.userid is null 
    """.format(start_day, end_day, group_id)
    print(sql)
    report = pd.read_sql(sql, con=holo_cnx)

    print(report)
    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                      'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """
    2023-03-24，针对手游查询到的封号匹配出对应封号订单，针对商户订单 封禁上号OAID
    group_name 手游查询封号用户群(商户单-关闭上号oaid)
    """
    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    start_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1

    shouyou_fenghao_match_for_sh_group(end_day, end_day, 47)
