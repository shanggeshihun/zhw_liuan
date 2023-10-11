-- TOP游戏撤单率
select t1.gameid,t1.game_name,t1.orders,coalesce(t1.withdraw_orders,0) as withdraw_orders,
    coalesce(t2.succ_orders,0) as succ_orders,coalesce(t2.quick_orders,0) as quick_orders
from
(
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
) t1
left join
(
    select a.gid as gameid,g.title as game_name,
        count(distinct case when a.is_success = 1 then order_id end) as succ_orders,
        count(distinct order_id) as quick_orders
    from
    (
        -- U1
        select gid,order_id,id,case when status = 1 then 1 end is_success,'QQ' as serve
        from hive.zhwdb.zhw_quick_login_log
        where part_day between '{0}' and '{1}'
        and use_type = 2
        and quick_type not in ('default','server')
        and order_id>0
        and gid in (11 ,443 ,560 ,683 ,446 ,1088 ,17 ,699 ,581 ,698)

        union all
        -- U2
        select gid,order_id,id,case when status = 301 then 1 end is_success,'QQ' as serve
        from hive.zhwdb.zhw_quick_zhw_quick_queue
        where part_day between '{0}' and '{1}'
        and type in (3,4) -- 3，4-订单上号
        and gid in (11 ,443 ,560 ,683 ,446 ,1088 ,17 ,699 ,581 ,698)

        union all
        -- U3
        select gid,order_id,id,case when status = 304 then 1 end is_success,'微信' as serve
        from hive.zhwdb.zhw_quick_wx_queue
        where part_day between '{0}' and '{1}'
        and order_id>0
    ) a
    left join hive.zhwdb.zhw_game_info g
    on a.gid = g.id
    group by 1,2
) t2
on t1.gameid = t2.gameid