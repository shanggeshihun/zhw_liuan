-- 20230529 封号源数据仅考虑封号查询数据
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
    and to_char(a.create_time,'yyyy-mm-dd') >= to_char(date('{0}') - interval '30 days', 'yyyy-mm-dd')
    and to_char(a.create_time,'yyyy-mm-dd') = '{0}'
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
	on c.hid = a.act_id and a.start_time between c.stimer and c.etimer
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
	on c.hid = a.act_id and a.start_time between c.stimer and c.t
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
-- 	t1.act_zh,t1.start_time,t1.game_id,t1.add_time,t1.m_order,t1.m_hid,t1.m_ip,t1.m_gameid,t1.m_userid,t1.m_huserid,t1.m_stimer,t1.m_etimer,t1.addfrom_name,t1.m_zh,t1.game_name_m,
-- 	cast (now() as timestamp) as push_time
	t1.m_hid as "货架号",t1.act_zh as "游戏账号",t1.start_time as "封号时间",t1.m_order as "匹配订单",
	t1.m_stimer as "订单开始时间",t1.m_etimer as "订单结束时间",t1.m_userid as "用户名"
from
(

    select *
	-- m_order as did,m_userid as userid ,m_ip as userip,m_gameid as gameid
    from zc_cd_tmp
    where addfrom_name not in('分销合伙人','新转租平台','分销普通版','分销尊享版','分销高级版')
) t1
left join
(
	select id ,jkx_userid
	from ods_zhw.zhw_user
) t2
on t1.m_userid=t2.jkx_userid
left join
(
	select userid
	from ads.zhw_shanghu_type_all
	group by 1
) t3
on t1.m_userid=t3.userid
left join
(
	select userid
	from ods_zhw.zhw_fx_sublet_kf
	where status=1
) t5
on t1.m_userid=t5.userid
where t3.userid is null
and t5.userid is null
order by 2 desc