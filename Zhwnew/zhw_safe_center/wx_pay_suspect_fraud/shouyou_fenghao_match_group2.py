# _*_coding:utf-8 _*_

# @Time      : 2023/3/22  14:40
# @Author    : An
# @File      : .py
# @Software  : PyCharm

import time, datetime, configparser, warnings, math, platform , psycopg2
import sys
import pandas as pd
import pymysql, presto
import pyhive.presto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
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


def shouyou_fenghao_match_group(start_day, end_day, group_id):
    sql = """    
		with tmp_hao_lock_details as (
            select add_time,
                   '客服添加封号' as data_soure, --数据来源
                   act_id,
                   act_zh,
                   case when lock_start <= add_time then lock_start else add_time end as start_time,
                   lock_end as end_time,
                   lock_sec/60/60/24 as lock_days,
                   gid as game_id,
                   game as game_name,
                   case when gid = 581 then '端游' else '手游' end game_type,
                   case when bz ~ '([6|7|8]{{1}}[0-9]{{8}})' and bz !~ '([0-9]{{10,}})' then substring(bz from '([6|7|8]{{1}}[0-9]{{8}})') end::int8 as order_id
            from ods_zhw.zhw_hao_lock
            where part_day between '{0}' and '{1}'
            and is_related=1 
            and gid in (443,446,683)

            union all 

            select a.create_time as add_time,
                   '安防查询封号' as data_soure,
                   b.hid,
                   b.game_account,
                   a.start_stmp_time,
                   a.end_stmp_time,
                   a.duration/60/60/24 as lock_days,
                   a.game_id,
                   case when a.game_name = '穿越火线-枪战王者' then '枪战王者' else a.game_name end,
                   case when a.game_id in (11,17,24,581) then '端游' else '手游' end,
                   case when c.order_id ~ '([6|7|8]{{1}}[0-9]{{8}})' and c.order_id !~ '([0-9]{{10,}})' then substring(c.order_id from '([6|7|8]{{1}}[0-9]{{8}})') end::int8 as order_id
            from ods_zhw.game_cheat_account_record a
            left join ods_zhw.game_cheat_account_info b on a.game_account_id=b.id
            left join ods_zhw.game_cheat_account_record_verify c on  a.id = c.record_id
            where type like '%%封号%%'
            and a.game_id in  (443,446,683)
            and to_char(a.create_time,'yyyy-mm-dd') between '{0}' and '{1}'
            and (a.fpt = a.pt or a.pt = -1) and a.game_id = b.game_id 
		),
		zc_tmp as (
			select a.*,
			c.id as m_order,c.hid as m_hid,c.ip as m_ip,c.gameid as m_gameid,c.userid as m_userid,c.huserid as m_huserid,c.stimer as m_stimer,c.etimer as m_etimer,
			c.item_name as addfrom_name,c.zh as m_zh,
			g.title as game_name_m,
			1 as pn
			from tmp_hao_lock_details a 
			left join 
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
		) ,
		cd_tmp as (
			select a.*,
			c.id as m_order,c.hid as m_hid,c.ip as m_ip,c.gameid as m_gameid,c.userid as m_userid,c.huserid as m_huserid,c.stimer as m_stimer,c.etimer as m_etimer,
			c.item_name as addfrom_name,c.zh as m_zh,
			g.title as game_name_m,
			2 as pn
			from tmp_hao_lock_details a 
			left join 
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
			*
        from
		(
			select *
			from zc_cd_tmp
		) t1 
		left join
		(
			select id ,jkx_userid from ods_zhw.zhw_user
		) t2
		on t1.m_userid=t2.jkx_userid
		left join
		(
			select userid from public.zhw_shanghu_type_all where save_date = '{1}' group by 1
		) t3
		on t1.m_userid=t3.userid
		left join
		(
			select userid from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2} group by 1
		) t4
		on t1.m_userid=t4.userid
		left join
		(
			select userid from ods_zhw.zhw_fx_sublet_kf where status=1
		) t5
		on t1.m_userid=t5.userid
		where true 
		and t3.userid is null -- 非商户
		-- and t4.userid is null -- 已推送
		and t5.userid is null -- 非分销客服
    """.format(start_day, end_day, group_id)
    print(sql)
    report = pd.read_sql(sql, con=holo_cnx)
    report.to_excel(r"E:\工作文件\在刀锋\dofun\07安全防御\需求脚本\王建委\封号账号使用订单匹配逻辑的准确率与覆盖率对比分析\r1.xlsx")
    print(report)
    # report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did','user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    # report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)


if __name__ == '__main__':
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    start_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1

    start_day, end_day = '2023-04-20', '2023-04-23'
    shouyou_fenghao_match_group(start_day,end_day,47)

