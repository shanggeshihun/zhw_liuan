/*wx_pay_suspect_fraud_user_info*/
-- 20230321渠道支付诈骗-策略评估
select t1.*,
case
	when t4.jkx_userstatus = 0 then '系统关闭会员'
	when t4.jkx_userstatus = 1 then '开启'
	when t4.jkx_userstatus = 2 then '商户关闭会员'
	when t4.jkx_userstatus = 3 then '注销会员'
end as userstatus,
t4.jkx_usermoney,t4.jkx_bz,
t2.max_part_day,
date('{0}') - 1 - date(t2.max_part_day) as recent_order_days,
t2.order_days,t2.orders,t2.order_pm,
t5.order_days as last2w_order_days,t5.orders as last2w_orders,t5.order_pm as last2w_order_pm,
date(t4.jkx_timer) as reg_time,
date('{0}') - 1 - date(t4.jkx_timer) as reg_days,
t4.closetimer
from
(
	select
		case when user_type = 0 then '非商户非分销' else '其他' end as user_type,
		username,open_num,money
	from
	(
		select
			case when t2.userid is not null then 1 else 0 end user_type,username,
			count(distinct pay_num) open_num,
			sum(money) as money
		from ods_zhw.zhw_recharge t1
		join ods_zhw.zhw_user u
		on t1.username = u.jkx_userid and u.closetimer between cast('{0}' || ' 00:00:00' as timestamp) and cast('{0}' || ' 23:59:59' as timestamp)
		left join ods_zhw.zhw_shanghu_type_log t2
		on t1.username = t2.userid and sh_type > 0
		where t1.part_day between to_char(date('{0}') - interval '6 days','yyyy-mm-dd')  and to_char(date('{0}') - interval '0 days','yyyy-mm-dd')
		and status = 2 and viaid = 3 and pay_num <> ''
		group by 1,2
	) a
	where open_num >=4
	and user_type = 0 -- 非商户非分销
	and money>200
) t1
left join
(
	select userid,sum(pm) order_pm,max(part_day) as max_part_day,count(distinct part_day) as order_days,count(id) as orders
	from ods_zhw.zhw_dingdan
	where part_day between to_char(date('{0}') - interval '6 days','yyyy-mm-dd')  and to_char(date('{0}') - interval '0 days','yyyy-mm-dd')
	group by 1
) t2
on t1.username = t2.userid
--left join (select charge_id,sum(charge_cprice) charge_cprice from ods_zhw.zhw_c where charge_game_cp like '%提现%' group by 1) t3 on t1.username = t3.charge_id
left join ods_zhw.zhw_user t4
on t1.username = t4.jkx_userid
left join
(
	select userid,sum(pm) order_pm,max(part_day) as max_part_day,count(distinct part_day) as order_days,count(id) as orders
	from ods_zhw.zhw_dingdan
	where part_day between to_char(date('{0}') - interval '13 days','yyyy-mm-dd')  and to_char(date('{0}') - interval '7 days','yyyy-mm-dd')
	group by 1
) t5
on t1.username = t5.userid
order by 1,3 desc,4 desc
