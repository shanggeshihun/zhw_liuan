select t1.gameid,t1.game_name,t1.serve,
    t1.channel,
    coalesce(t1.orders,0) as orders,
    coalesce(t1.withdraw_orders,0) as withdraw_orders,
    coalesce(t2.quick_orders,0) as quick_orders,
    coalesce(t2.succ_orders,0) as succ_orders
from
(
    -- TOP游戏  渠道、大区  撤单率
    select
        a.gameid,
        g.title as game_name,
        case
            when a.add_from in (2,21) then '安卓主版本'
            when a.add_from in (30,35) then '应用市场'
            when a.add_from in (22) then 'IOS'
            when b.did is not null then '转租上号器'
            else '其他'
        end as channel,
        case
            when regexp_like(a.yxqu,'QQ|手Q') then 'QQ'
            when a.yxqu like '微信%' then '微信'
            else '其他'
        end as serve,
        count(a.id) as orders,
        count(case when a.zt = 3 then a.id end) as withdraw_orders
    from
    (
        select gameid,id,zt,add_from,yxqu
        from hive.zhwdb.zhw_dingdan
        where true
        and part_day between '{0}' and '{1}'
        and gameid in (11 ,443 ,560 ,683 ,446 ,1088 ,17 ,699 ,581 ,698)
    ) a
    join hive.zhwdb.zhw_game_info g
    on a.gameid = g.id
    left join
    (
        select did
        from hive.zhwdb.zhw_fx_sublet_order
        where true
        and format_datetime(add_time,'yyyy-MM-dd') between '{0}' and '{1}'
    ) b
    on a.id = b.did
    group by 1,2,3,4
) t1
left join
(
    -- TOP游戏 渠道、大区 上号成功率数据统计
    select
        a.gid as gameid,
        g.title as game_name,
        case
            when b.add_from in (2,21) then '安卓主版本'
            when b.add_from in (30,35) then '应用市场'
            when b.add_from in (22) then 'IOS'
            when c.did is not null then '转租上号器'
            else '其他'
        end as channel,
        -- 通过验证1001-1002期间通过U1,U2的订单关联订单获取的区服有QQ和其他，没有微信基本与查询结果一致
        -- case
            -- when regexp_like(b.yxqu,'QQ|手Q') then 'QQ'
            -- when b.yxqu like '微信%' then '微信'
            -- else '其他'
        -- end as serve,
        a.serve,
        count(distinct a.order_id) as quick_orders,
        count(distinct if(a.is_success =1,a.order_id,null)) as succ_orders
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
    left join
    (
        select *
        from hive.zhwdb.zhw_dingdan
        where true
        and part_day between '{0}' and '{1}'
        and gameid in (11 ,443 ,560 ,683 ,446 ,1088 ,17 ,699 ,581 ,698)
    ) b
    on a.order_id = b.id
    left join
    (
        select did
        from hive.zhwdb.zhw_fx_sublet_order
        where true
        and format_datetime(add_time,'yyyy-MM-dd') between '{0}' and '{1}'
        and part_month between format_datetime(date('{0}'),'yyyy-MM') and format_datetime(date('{1}'),'yyyy-MM')
    ) c
    on b.id = c.did
    left join hive.zhwdb.zhw_game_info g
    on a.gid = g.id
    group by 1,2,3,4
) t2
on t1.gameid = t2.gameid and t1.channel = t2.channel and t1.serve = t2.serve