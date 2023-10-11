-- TOP游戏撤单率
select
    a.gameid,
    g.title as game_name,
    count(a.id) as orders,
    count(case when a.zt = 3 then a.id end) as withdraw_orders
from
(
    select gameid,id,zt,add_from
    from hive.zhwdb.zhw_dingdan
    where true
    and part_day between '{0}' and '{1}'
    and gameid in (11 ,443 ,560 ,683 ,446 ,1088 ,17 ,699 ,581 ,698)
) a
join hive.zhwdb.zhw_game_info g
on a.gameid = g.id
group by 1,2