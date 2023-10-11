-- 暑期活动无门槛活动
with no_limit_hongbao as (
	select a.id as issue,a.type_id as type,b.name
	from ods_zhw.zhw_red_packet a 
	left join ods_zhw.zhw_red_packet_type b 
	on a.type_id = b.id 
	where b.name like '%暑期%'
	and a.use_limit = 0
	and to_char(a.create_time,'yyyy') = '2023'
),
-- 商户身份
huserid_identity as (
	select b.save_date,b.add_time,b.userid,
	case 
		when b.type_id_new = 1 then '月商'
		when b.type_id_new = 2 then '年商'
		when b.type_id_new = 3 then '分销商'
		when b.type_id_new = 4 then '新转租'
		when b.type_id_new = 5 then '合伙人'
	end as sh_type,
	b.start_time,b.end_time
	from 
	(
		select a.*,row_number()over(partition by userid order by add_time desc,type_id_new desc) as rn 
		from 
		(
			-- 1:月商,2:年商,3:分销商(普通高级尊享),4:合伙人,5:新转租
			select *,case when type_id = 4 then 5 when type_id = 5 then 4 else type_id end as type_id_new
			from public.zhw_shanghu_type_all 
			where true 
			and save_date = '2023-07-04' -- 固定
		) a 
	) b 
	where b.rn = 1 
),
-- 领取红包用户
user_receive_hongbao as (
select a.jkx_userid as userid,
	a.part_day,
	count(a.id)as receive_hbids,
	count(distinct b.issue) as receive_hbissues,
	count(distinct b.type) as receive_types
from ods_zhw.zhw_hongbao a 
join no_limit_hongbao b 
on a.issue = b.issue and a.type = b.type 
where true 
and a.part_day between '{0}' and '{1}'
group by 1,2
),
-- 领取红包用户信息(身份证等)
user_receive_hongbao_info as (
	-- 领取红包用户信息
	select a.userid,a.part_day,a.receive_hbids,a.receive_hbissues,a.receive_types,
	b.jkx_userphone,b.jkx_username,b.jkx_usercard
	from user_receive_hongbao a 
	left join ods_zhw.zhw_user b 
	on a.userid = b.jkx_userid
)

insert into tmp_zhw_holiday_risk_user_info(
holiday_name,theme,dim_desc,value_desc,dim_text_1,dim_text_2,value_int_1,value_int_2,
param_start_day,param_end_day,
record_deal_flag,push_time
)
select
    '2023-暑假' as holiday_name,
    '无门槛红包' as theme,
    'dim_text_1 as part_day,dim_text_2 as ip' as dim_desc,
    'value_int_1 as users, value_int_2 as accum_users' as value_desc,
    c.part_day as dim_text_1,c.ip as dim_text_2,c.users as value_int_1,c.accum_users as value_int_2,
    '{0}' as param_start_day,'{1}' as param_end_day,0 as record_deal_flag,current_timestamp as push_time
from
(
	select a.part_day,ip,users,sum(users)over(partition by ip order by part_day desc) as accum_users
	from
	(
		select
			a.part_day,
			c.ip,
			count(a.jkx_userid) as users
		from ods_zhw.zhw_hongbao a
		join no_limit_hongbao b
		on a.issue = b.issue and a.type = b.type
		join ods_zhw.zhw_activity_deliver_record c
		on a.id = cast(c.ext_field::jsonb->>'hb_id' as int8)
		and c.part_day between '{0}' and '{1}'
		and c.ip<>'' and c.ip not in ('0.0.0.0')
		and c.aid = 47 and c.prize_type= 101
		where true
		and a.part_day between '{0}' and '{1}'
		group by 1,2
	) a
) c
where true
and users>5 -- 单日同IP下用户超过20
and accum_users>20

