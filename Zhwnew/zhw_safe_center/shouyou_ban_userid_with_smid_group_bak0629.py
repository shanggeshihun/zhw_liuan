# _*_coding:utf-8 _*_

# @Time      : 2023/4/14  17:30
# @Author    : An
# @File      : shouyou_ban_userid_with_smid_group.py
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


# ------------------------参数配置----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1


def shouyou_ban_userid_with_smid_group(day_last_1, now_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:手游导致封号普通用户及SMID关联用户-关闭封杀
    '''
    sql = """    	
		select distinct 
			{2} as fk_target_id,
			1 as fk_module_type,
			t2.id as fk_uid,
			t1.userid,
			'' as userip,
			t1.gameid as gameid,
			0 as did,
			0 as user_mark,
			0 as push_no,
			cast (now() as timestamp)  as push_time,
			0 as is_inc,
			0 as deal_flag 
        from
		(
			select m_order as did,m_userid as userid ,m_ip as userip,m_gameid as gameid
			from 
			(
				-- 近30分钟添加封号数据的封号订单
				select *
				from public.zhw_hao_lock_order_match
				where target_fk_id = {2}
				and push_time>=to_char(current_timestamp  - interval '40 minutes','yyyy-mm-dd hh24:mi:ss')
				and addfrom_name not in('分销合伙人','新转租平台','分销普通版','分销尊享版','分销高级版')
				and m_order not in (select did from ods_zhw.zhw_app_oaid where oaid_sh in ('0000000000000000|','0000000000000000|','00000000000000000000000000000000'))
				-- and (game_id in (443) or (game_id in (446,683) and m_os = 'ios'))
				and game_id in (443,446,683) 
			) a 
			where true 			
			
			union all 
			select 0 as did,c.user_id as userid,'' as userip,0 as gameid
			from 
			(	-- 近30分钟添加封号数据的封号订单的用户
				select m_userid
				from public.zhw_hao_lock_order_match
				where target_fk_id = {2}
				and push_time>=to_char(current_timestamp  - interval '40 minutes','yyyy-mm-dd hh24:mi:ss')
				and addfrom_name not in('分销合伙人','新转租平台','分销普通版','分销尊享版','分销高级版')
				and m_order not in (select did from ods_zhw.zhw_app_oaid where oaid_sh in ('0000000000000000|','0000000000000000|','00000000000000000000000000000000'))
				-- and (game_id in (443) or (game_id in (446,683) and m_os = 'ios'))
				and game_id in (443,446,683) -- 20230523 3款手游IOS+Android
				group by 1 
			) a
			join 
			(	-- 近2天的数美请求
				select smid,user_id 
				from ods_zhw.zhw_safe_defense_detail
				where true 
				and part_day between '{0}' and '{1}'
				group by 1,2
			) b 
			on a.m_userid = b.user_id 
			join 
			(	-- 近2天的数美请求
				select smid,user_id 
				from ods_zhw.zhw_safe_defense_detail
				where true 
				and part_day between '{0}' and '{1}'
				group by 1,2
			) c 
			on b.smid = c.smid
			group by 1,2,3,4
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

    """
    手游导致封号普通用户及SMID关联用户-关闭封杀
    """
    now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
    shouyou_ban_userid_with_smid_group(day_last_1,now_day,50)