select
    substring('{0}',1,7) as part_month,
	sum(case when jkx_usermoney>=dedit_money then dedit_money else jkx_usermoney end) as dedit_money
from 
(
	select userid,max(jkx_usermoney) as jkx_usermoney,max(dedit_money) as dedit_money
	from 
	(
		--白嫖
		select u.jkx_userid as userid,max(u.jkx_usermoney) as jkx_usermoney,sum(b.pm) as dedit_money
		from ods_zhw.zhw_dingdan b 
		join ods_zhw.zhw_user  u 
		on b.userid = u.jkx_userid and u.jkx_usermoney > 0 and u.closetimer between (timestamp '{0} 00:00:00') and (timestamp '{1} 23:59:59')
		where b.part_day between '2021-01-01' and '{1}'
		and b.zt = 3 
		group by 1

		union all 
		--刷红包
		select u.jkx_userid as userid,max(u.jkx_usermoney) as jkx_usermoney ,sum(b.pm) as dedit_money
		from ods_zhw.zhw_hongbao_order a 
		join ods_zhw.zhw_dingdan b 
		on a.order_id = b.id and a.use_money = b.pm and b.part_day between '2021-01-01' and '{1}'
		join ods_zhw.zhw_user  u 
		on b.userid = u.jkx_userid and u.jkx_usermoney > 0 and u.closetimer between (timestamp '{0} 00:00:00') and (timestamp '{1} 23:59:59')
		where a.part_day between '2021-01-01' and '{1}'
		group by 1

		union all 
		--免费体验
		select u.jkx_userid as userid,max(u.jkx_usermoney) as jkx_usermoney,count(b.order_id)*2 as dedit_money
		from ods_zhw.zhw_free_play_order b 
		join ods_zhw.zhw_user  u 
		on b.userid = u.jkx_userid and u.jkx_usermoney > 0 and u.closetimer between (timestamp '{0} 00:00:00') and (timestamp '{1} 23:59:59')
		where b.part_day between '2021-01-01' and '{1}'
		group by 1
	) a 
	group by 1 
) a
group by 1