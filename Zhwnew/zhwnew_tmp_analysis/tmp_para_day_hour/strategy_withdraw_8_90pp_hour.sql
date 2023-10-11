with tmp_sh_users as (
    select userid
    from ads.zhw_shanghu_type_all
    where save_date = '{1}'
    group by 1
)

select '{3}' as part_day,a.userid,count(distinct a.id) as orders,count(distinct b.did) as cd_orders,count(distinct b.did)*1.00/count(distinct a.id) as cd_ratio
from ods_zhw.zhw_dingdan  a
left join ods_zhw.zhw_ts b
on a.id = b.did and b.lx not in ('上号器自动投诉（qq冻结）','上号器自动投诉（账号密码错误）','上号器自动投诉 (人脸识别)')
and b.part_day between '{0}' and to_char(date('{1}') + 1,'yyyy-mm-dd')
left join tmp_sh_users c
on a.userid = c.userid
left join
(
    select userid
    from ods_zhw.zhw_fx_sublet_kf
    group by 1
) d
on a.userid = d.userid
where a.part_day between '{0}' and '{1}'
and a.add_time>=(timestamp '{2}')
and a.add_time<(timestamp '{3}')
and a.gameid in (443,683,560,446,636,11,17,24,581)
and c.userid is null and d.userid is null
group by 1,2
having count(distinct a.id)>=8 and count(distinct b.did)*1.00/count(distinct a.id)>0.9


