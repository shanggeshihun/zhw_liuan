select substring('{1}',1,7) as part_month,count(*)  as users
from ods_zhw.zhw_user
where true
and closetimer between (timestamp '{0} 00:00:00') and (timestamp '{1} 23:59:59')
and jkx_usermoney > 0
group by 1