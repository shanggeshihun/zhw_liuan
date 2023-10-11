# _*_coding:utf-8 _*_

# @Time      : 2022/8/12  13:00
# @Author    : An
# @File      : DwstStatistics.py
# @Software  : zhw_dws_msg_recall_statistics,zhw_dws_red_packet_type_id_msg,zhw_dws_push_recall_statistics。2022/8/5 将 【3日内下单】【3日红包下单】分开执行，【3日红包下单】 红包先关联红包下单再关联下单表，红包订单表 添加type/issue条件;添加使用红包复购

import time, datetime, configparser, warnings, math, platform
import sys
import pymysql, presto
import pyhive.presto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine
import pandas as pd

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysql import OperateMysql
from OperatePresto import OperatePresto
from SchedualToMysql import SchedualInfo


class Dws:
    def __init__(self):
        warnings.filterwarnings("ignore")
        # ------------------------数据库配置读取----------------------------
        cf = configparser.ConfigParser()
        if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
            """服务器模式"""
            cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
        else:
            """本地模式"""
            cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

        # 运营数据库
        self.mysql_host = cf.get("Mysql-data_yunying", "host")
        self.mysql_user = cf.get("Mysql-data_yunying", "user")
        self.mysql_password = cf.get("Mysql-data_yunying", "password")
        self.mysql_db = cf.get("Mysql-data_yunying", "DB")
        self.mysql_port = cf.get("Mysql-data_yunying", "port")

        # hive数据库
        self.presto_host = cf.get("hive_presto", "host")
        self.presto_username = cf.get("hive_presto", "username")
        self.presto_port = cf.get("hive_presto", "port")
        self.presto_schema = cf.get("hive_presto", "schema")
        self.presto_catalog = cf.get("hive_presto", "catalog")

    def zhw_dws_msg_recall_statistics(self, part_day):
        """
        :param part_day: 日期参数
        :return: 短信召回 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # msg 推送的红包类型和红包id
        msg_config_sql = """
            select packet_type,packet_id
            from red_packet_trigger_way 
            where way = 'msg'
        """
        msg_config_result = operate_mysql.query_data(msg_config_sql)
        msg_config_result_columns = operate_mysql.query_data_index()
        msg_config_df = pd.DataFrame(msg_config_result)
        msg_config_df.columns = msg_config_result_columns

        packet_type_tuple = tuple(set(msg_config_df.packet_type))
        packet_id_tuple = tuple(set(msg_config_df.packet_id))

        # 清空当日数据  维度  日期+抽奖用户
        mysql_sql = "delete from zhw_dws_msg_recall_statistics where part_day between '{0}' and date_add(str_to_date('{0}','%Y-%m-%d'),interval 3 day)".format(
            part_day)
        operate_mysql.update_data(mysql_sql)
        operate_mysql.close_conn()

        start_time = time.time()
        # 红包领取 & 红包使用(sql_1 + sql_4) 金额不存在重复
        sql_1 = '''
            select
                a.part_day,
                a.type,
                a.issue,
                count(distinct a.jkx_userid)  as hb_rece_user,-- 红包领取人数
                count(a.id) as hb_rece_num, -- 红包领取数
                sum(a.money) as hb_rece_money, -- 红包领取金额
                count(distinct case when a.usemoney>0 then a.jkx_userid end ) as hb_use_user,--红包累积使用金额
                count(case when a.usemoney>0 then a.id end) as hb_use_num,
                sum(a.usemoney) as hb_use_money, -- 红包累积使用金额
                
                count(distinct c.userid)  curr_hb_did_user,-- 当日领取当日红包使用人数
                count(c.id) as curr_hb_did_num, -- 当日领取当日红包下单订单量
                sum(c.pm) as curr_hb_did_money  -- 当日领取当日红包下单金额
            from
            (
                select part_day,type,issue,jkx_userid,id,money,usemoney
                from zhwdb.zhw_hongbao
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                -- 满足以下条件的数据从2022年3月开始（2022-03-05）
                and (type in {1} and issue in {2})
            ) a
            
            left join 
            (
                select order_id,hb_id,part_day 
                from zhwdb.zhw_hongbao_order 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) b 
            on a.id=b.hb_id and a.part_day=b.part_day 
            left join 
            (
                select userid,id,pm,part_day 
                from zhwdb.zhw_dingdan 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
            ) c 
            on b.order_id=c.id  and b.part_day=c.part_day 
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_1 = operate_presto.query_data(sql_1)

        if not presto_data_list_1:
            return

        result_columns_1 = operate_presto.query_data_index()
        df_1 = pd.DataFrame(presto_data_list_1)
        df_1.columns = result_columns_1
        time.sleep(1)
        end_time = time.time()
        print('sql_1', end_time - start_time)

        start_time = time.time()
        # 领取红包且活跃，领取红包有效期内活跃 不统计金额
        sql_2 = '''
            select
                a.part_day,
                a.type,
                a.issue,
                count(distinct b.userid) as curr_user,  -- 当日领取且活跃人数
                count(distinct c.userid) as youxiao_user  -- 当日领取且有效期内活跃人数
            from
            (
                select part_day,type,issue,jkx_userid,recetime,outtime
                from zhwdb.zhw_hongbao
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a
            left join
            (
                select part_day,userid,usertimer
                from zhwdb.zhw_user_login_log_extend
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                -- group by 1,2
            ) b ---红包有效期内一般[1,10]的活跃情况
            -- 当日领取当日活跃(领取后活跃)
            on a.jkx_userid=b.userid and a.part_day=b.part_day and to_unixtime(b.usertimer) between a.recetime and a.outtime 
            left join
            (
                select part_day,userid,usertimer
                from zhwdb.zhw_user_login_log_extend
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar)
                -- group by 1,2,3
            ) c ---红包有效期[1,10]内的活跃情况
            on a.jkx_userid=c.userid and to_unixtime(c.usertimer) between a.recetime and a.outtime
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_2 = operate_presto.query_data(sql_2)
        if not presto_data_list_2:
            df_2 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'curr_user': [0], 'youxiao_user': [0]}
            )
        else:
            result_columns_2 = operate_presto.query_data_index()
            df_2 = pd.DataFrame(presto_data_list_2)
            df_2.columns = result_columns_2
        time.sleep(1)
        end_time = time.time()
        print('sql_2', end_time - start_time)

        start_time = time.time()
        # 当日领取当日下单 金额不存在重复
        sql_3 = '''
            select
                a.part_day,
                a.type,
                a.issue,
                count(distinct b.userid) as curr_fufei_user,--当日领取当日下单人数
                count(b.id) as curr_fufei_num,
                sum(b.pm) as curr_fufei_money
            from
            (
                select part_day,type,issue,jkx_userid,recetime,outtime
                from zhwdb.zhw_hongbao
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a
            left join
            (
                select part_day,userid,id,pm,add_time
                from zhwdb.zhw_dingdan
                where  part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
            ) b
            -- 当日领取当日下单(领取后下单)
            on a.part_day=b.part_day and a.jkx_userid=b.userid and to_unixtime(b.add_time) between a.recetime and a.outtime

            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_3 = operate_presto.query_data(sql_3)
        if not presto_data_list_3:
            df_3 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'curr_fufei_user': [0], 'curr_fufei_num': [0],
                 'curr_fufei_money': [0]}
            )
        else:
            result_columns_3 = operate_presto.query_data_index()
            df_3 = pd.DataFrame(presto_data_list_3)
            df_3.columns = result_columns_3
        time.sleep(1)
        end_time = time.time()
        print('sql_3', end_time - start_time)

        start_time = time.time()
        # 3日内活跃用户数 不统计金额
        sql_5 = '''
            /*3日内活跃用户数*/
            select a.part_day,a.type,a.issue,
            count(distinct b.userid) as threeday_login_user  --当日领取且3日内活跃
            from 
            (
                select part_day,type,issue,jkx_userid,recetime as dt1 
                from zhwdb.zhw_hongbao 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            left join 
            (
                select part_day,userid,to_unixtime(usertimer) as dt2 
                from zhwdb.zhw_user_login_log_extend 
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
            ) b 
            -- 3日内活跃
            -- on b.dt2>=a.dt1 and b.dt2<=a.dt1+259200 and a.jkx_userid=b.userid 
            on b.dt2>=a.dt1 and b.dt2<=to_unixtime(date_add('day',2,cast(a.part_day as date)))+ 86399.99 and a.jkx_userid=b.userid 
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_5 = operate_presto.query_data(sql_5)

        if not presto_data_list_5:
            df_5 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'threeday_login_user': [0]
                 }
            )
        else:
            result_columns_5 = operate_presto.query_data_index()
            df_5 = pd.DataFrame(presto_data_list_5)
            df_5.columns = result_columns_5
        time.sleep(1)
        end_time = time.time()
        print('sql_5', end_time - start_time)

        start_time = time.time()
        # 3日内付费 -- 3日下单，3日红包下单 分开
        sql_6 = '''
        select coalesce(t1.part_day,t2.part_day) as part_day,coalesce(t1.type,t2.type) as type,
            coalesce(t1.issue,t2.issue) as issue,
            max(threeday_fufei_user) as threeday_fufei_user,
            max(threeday_fufei_num) as threeday_fufei_num,
            max(threeday_fufei_money) as threeday_fufei_money,
            max(threeday_hb_fufei_user) as threeday_hb_fufei_user,
            max(threeday_hb_fufei_num) as threeday_hb_fufei_num,
            max(threeday_hb_fufei_money) as threeday_hb_fufei_money
        from 
        (
            select a.part_day,a.type,a.issue,
            count(distinct b.userid) as threeday_fufei_user,  --当日领取且3日内下单
            count(distinct b.id) as threeday_fufei_num, --当日领取且3日内下单量
            sum(b.pm) as threeday_fufei_money --当日领取且3日内下单金额
            from 
            (
                select part_day,type,issue,jkx_userid,recetime as dt1 
                from zhwdb.zhw_hongbao 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            left join 
            (
                select part_day,userid,to_unixtime(add_time) as dt2,id,pm
                from zhwdb.zhw_dingdan 
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
            ) b 
            -- 3日内下单
            -- on b.dt2>=a.dt1 and b.dt2<=a.dt1+259200 and a.jkx_userid=b.userid 
            on b.dt2>=a.dt1 and b.dt2<=to_unixtime(date_add('day',2,cast(a.part_day as date)))+ 86399.99 and a.jkx_userid=b.userid 
            group by 1,2,3
        ) t1 
        full join 
        (
            select a.part_day,a.type,a.issue,
            count(distinct c.userid) as threeday_hb_fufei_user,  --当日领取且3日内红包下单
            count(distinct c.id) as threeday_hb_fufei_num, --当日领取且3日内红包下单量
            sum(c.pm) as threeday_hb_fufei_money --当日领取且3日内红包下单金额
            from 
            (
                select part_day,type,issue,jkx_userid,recetime as dt1,id
                from zhwdb.zhw_hongbao 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            join 
            ( 
                select part_day,jkx_userid as userid,order_id,usetime,hb_id
                from zhwdb.zhw_hongbao_order 
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
                and (type in {1} and issue in {2})
            ) b 
            on a.id =b.hb_id
            join
            (
                select part_day,userid,to_unixtime(add_time) as dt3,id,pm
                from zhwdb.zhw_dingdan 
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
            ) c 
            -- 3日内红包下单
            -- on c.dt3>=a.dt1 and c.dt3<=a.dt1+259200 and a.jkx_userid=c.userid 
            on b.order_id = c.id and c.dt3>=a.dt1 and c.dt3<=to_unixtime(date_add('day',2,cast(a.part_day as date)))+ 86399.99
            group by 1,2,3
        ) t2 
        on t1.part_day = t2.part_day and t1.type = t2.type and t1.issue = t2.issue
        group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_6 = operate_presto.query_data(sql_6)

        if not presto_data_list_6:
            df_6 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'threeday_fufei_user': [0],
                 'threeday_fufei_num': [0], 'threeday_fufei_money': [0], 'threeday_hb_fufei_user': [0],
                 'threeday_hb_fufei_num': [0], 'threeday_hb_fufei_money': [0]
                 }
            )
        else:
            result_columns_6 = operate_presto.query_data_index()
            df_6 = pd.DataFrame(presto_data_list_6)
            df_6.columns = result_columns_6
        time.sleep(1)
        end_time = time.time()
        print('sql_6', end_time - start_time)

        # 7日留存率 + 15日留存率
        start_time = time.time()
        # 20220701 sql_7 + sql_8 拆开，数据大无法成功跑出来
        sql_7 = '''
            /*7日留存率*/
            select 
            a.part_day,
            a.type ,
            a.issue,
            count(distinct c.userid) sevenday_login_user
            from 
            (
                select part_day,type,issue,jkx_userid,recetime 
                from zhwdb.zhw_hongbao  
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            left join 
            (
                select part_day,userid,usertimer
                from zhwdb.zhw_user_login_log_extend  
                where part_day between '{0}' and cast(date_add('day',14,cast('{0}' as date)) as varchar) 
                -- group by 1,2
            ) c  
            -- 7日活跃留存
            -- on a.jkx_userid=c.userid and date(c.part_day)>date(a.part_day)
            -- and date(c.part_day)<date_add('day',7,date(a.part_day))
            on a.jkx_userid=c.userid and to_unixtime(c.usertimer)>=a.recetime
            and to_unixtime(c.usertimer)<=to_unixtime(date_add('day',6,cast(a.part_day as date)))+ 86399.99
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_7 = operate_presto.query_data(sql_7)
        if not presto_data_list_7:
            df_7 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'sevenday_login_user': [0]}
            )
        else:
            result_columns_7 = operate_presto.query_data_index()
            df_7 = pd.DataFrame(presto_data_list_7)
            df_7.columns = result_columns_7
        time.sleep(1)
        end_time = time.time()
        print('sql_7', end_time - start_time)

        start_time = time.time()
        sql_8 = '''
            /* 15日留存率 */
            select 
            a.part_day,
            a.type ,
            a.issue,
            count(distinct cc.userid)  fifteen_login_user
            from 
            (
                select part_day,type,issue,jkx_userid,recetime 
                from zhwdb.zhw_hongbao  
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            left join 
            (
                select part_day,userid,usertimer
                from zhwdb.zhw_user_login_log_extend  
                where part_day between '{0}' and cast(date_add('day',22,cast('{0}' as date)) as varchar) 
                -- group by 1,2
            ) cc  
            -- 15日活跃留存
            -- on a.jkx_userid=cc.userid and date(cc.part_day)>date(a.part_day)
            -- and date(cc.part_day)<date_add('day',15,date(a.part_day))
            on a.jkx_userid=cc.userid and to_unixtime(cc.usertimer)>=a.recetime
            and to_unixtime(cc.usertimer)<=to_unixtime(date_add('day',14,cast(a.part_day as date)))+ 86399.99
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_8 = operate_presto.query_data(sql_8)
        if not presto_data_list_8:
            df_8 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0],
                 'fifteen_login_user': [0]}
            )
        else:
            result_columns_8 = operate_presto.query_data_index()
            df_8 = pd.DataFrame(presto_data_list_7)
            df_8.columns = result_columns_8
        time.sleep(1)
        end_time = time.time()
        print('sql_8', end_time - start_time)

        start_time = time.time()
        # sql_q
        sql_9 = '''
            /*7日付费留存率   使用红包后7内有下单 */
            select 
            c.part_day,
            c.type,
            c.issue,
            count(distinct d.userid)  as sevenday_fufei_user
            from 
            (   -- 领取日期，使用红包日期
                select a.part_day,a.type,a.issue,b.jkx_userid,max(b.usetime) as max_usetime,max(b.part_day) as max_user_day
                from 
                (
                    select part_day,type,issue,id,jkx_userid,recetime
                    from zhwdb.zhw_hongbao  
                    where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                    and (type in {1} and issue in {2})
                ) a  
                left join 
                (   -- 红包有效期[1,10]
                    select jkx_userid,hb_id,usetime,part_day
                    from zhwdb.zhw_hongbao_order 
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
                    and (type in {1} and issue in {2})
                ) b 
                -- 红包有效期 最长7日最短1日
                on a.id=b.hb_id 
                group by 1,2,3,4
            ) c
            left join 
            (
                select add_time,id,userid,pm 
                from zhwdb.zhw_dingdan 
                where part_day between '{0}' and cast(date_add('day',26,cast('{0}' as date)) as varchar) 
            ) d 
            -- 7日内付费
            on c.jkx_userid=d.userid and to_unixtime(d.add_time)>=c.max_usetime 
            and to_unixtime(d.add_time)<=to_unixtime(date_add('day',6,cast(c.max_user_day as date)))+ 86399.99
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_9 = operate_presto.query_data(sql_9)
        if not presto_data_list_9:
            df_9 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'sevenday_fufei_user': [0]
                 }
            )
        else:
            result_columns_9 = operate_presto.query_data_index()
            df_9 = pd.DataFrame(presto_data_list_9)
            df_9.columns = result_columns_9
        time.sleep(1)
        end_time = time.time()
        print('sql_9', end_time - start_time)

        start_time = time.time()
        # sql_10
        sql_10 = '''
            /*15日付费留存率   使用红包后15天内有下单 */
            select 
            c.part_day,
            c.type,
            c.issue,
            count(distinct dd.userid) as fifteen_fufei_user
            from 
            (   -- 领取日期，使用红包日期
                select a.part_day,a.type,a.issue,b.jkx_userid,max(b.usetime) as max_usetime,max(b.part_day) as max_user_day
                from 
                (
                    select part_day,type,issue,id,jkx_userid,recetime
                    from zhwdb.zhw_hongbao  
                    where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                    and (type in {1} and issue in {2})
                ) a  
                left join 
                (   -- 红包有效期[1,10]
                    select jkx_userid,hb_id,usetime,part_day
                    from zhwdb.zhw_hongbao_order 
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
                    and (type in {1} and issue in {2})
                ) b 
                -- 红包有效期 最长7日最短1日
                on a.id=b.hb_id 
                group by 1,2,3,4
            ) c
            -- 15日内付费
            left  join 
            (
                select add_time,id,userid,pm 
                from zhwdb.zhw_dingdan 
                where part_day between '{0}' and cast(date_add('day',31,cast('{0}' as date)) as varchar) 
            ) dd 
            -- on c.jkx_userid=dd.userid and dd.dt3>c.mdt2 and dd.dt3<=date_add('day',15,c.mdt2)
            on c.jkx_userid=dd.userid and to_unixtime(dd.add_time)>=c.max_usetime 
            and to_unixtime(dd.add_time)<=to_unixtime(date_add('day',14,cast(c.max_user_day as date)))+ 86399.99
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_10 = operate_presto.query_data(sql_10)
        if not presto_data_list_10:
            df_10 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0],
                 'fifteen_fufei_user': [0]
                 }
            )
        else:
            result_columns_10 = operate_presto.query_data_index()
            df_10 = pd.DataFrame(presto_data_list_10)
            df_10.columns = result_columns_10
        time.sleep(1)
        end_time = time.time()
        print('sql_10', end_time - start_time)


        sql_21 = '''
                /*有效期内下单*/
                select a.part_day,a.type,a.issue,
                count(distinct b.userid) as youxiao_fufei_user,  --有效期内下单用户数
                count(distinct b.id) as youxiao_fufei_num, --有效期内下单量
                sum(b.pm) as youxiao_fufei_money --有效期内下单金额
                from 
                (
                    select part_day,type,issue,jkx_userid,recetime as dt1,outtime,recetime
                    from zhwdb.zhw_hongbao 
                    where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                    and (type in {1} and issue in {2})
                ) a 
                left join
                (
                    select *
                    from zhwdb.zhw_dingdan
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar)
                ) b 
                on a.jkx_userid=b.userid and to_unixtime(b.add_time) between a.recetime and a.outtime
                group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_21 = operate_presto.query_data(sql_21)
        if not presto_data_list_21:
            df_21 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'youxiao_fufei_user': [0],
                 'youxiao_fufei_num': [0], 'youxiao_fufei_money': [0]}
            )
        else:
            result_columns_21 = operate_presto.query_data_index()
            df_21 = pd.DataFrame(presto_data_list_21)
            df_21.columns = result_columns_21
        time.sleep(1)
        end_time = time.time()
        print('sql_21', end_time - start_time)

        start_time = time.time()
        sql_22 = '''
            /*有效期内红包下单*/
            select a.part_day,a.type,a.issue,
            count(distinct c.userid) as youxiao_hb_fufei_user,  --有效期内下单用户数
            count(distinct c.id) as youxiao_hb_fufei_num, --有效期内下单量
            sum(c.pm) as youxiao_hb_fufei_money --有效期内下单金额
            from 
            (
                select *
                from zhwdb.zhw_hongbao 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            join 
            (
                select *
                from zhwdb.zhw_hongbao_order 
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) b 
            on a.id = b.hb_id and b.usetime between a.recetime and a.outtime
            join
            (
                select *
                from zhwdb.zhw_dingdan
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar)
            ) c 
            on b.order_id = c.id and to_unixtime(c.add_time) between a.recetime and a.outtime
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_22 = operate_presto.query_data(sql_22)
        if not presto_data_list_22:
            df_22 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'youxiao_hb_fufei_user': [0],
                 'youxiao_hb_fufei_num': [0], 'youxiao_hb_fufei_money': [0]}
            )
        else:
            result_columns_22 = operate_presto.query_data_index()
            df_22 = pd.DataFrame(presto_data_list_22)
            df_22.columns = result_columns_22
        time.sleep(1)
        end_time = time.time()
        print('sql_22', end_time - start_time)

        operate_presto.close_conn()

        result = pd.merge(df_1, df_2, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_3, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_5, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_6, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_7, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_8, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_9, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_10, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_21, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_22, on=['part_day', 'type', 'issue'], how='left')

        result = result[
            ['part_day', 'type', 'issue', 'hb_rece_user', 'hb_rece_num', 'hb_rece_money', 'hb_use_user', 'hb_use_num',
             'hb_use_money', 'curr_user', 'youxiao_user', 'curr_fufei_user', 'curr_fufei_num', 'curr_fufei_money',
             'curr_hb_did_user', 'curr_hb_did_num', 'curr_hb_did_money', 'threeday_login_user', 'threeday_fufei_user',
             'threeday_fufei_num', 'threeday_fufei_money', 'threeday_hb_fufei_user', 'threeday_hb_fufei_num',
             'threeday_hb_fufei_money', 'sevenday_login_user', 'fifteen_login_user', 'sevenday_fufei_user',
             'fifteen_fufei_user', 'youxiao_fufei_user', 'youxiao_fufei_num', 'youxiao_fufei_money',
             'youxiao_hb_fufei_user', 'youxiao_hb_fufei_num', 'youxiao_hb_fufei_money']]
        result.fillna(0, inplace=True)
        presto_data_list = []
        for i in range(len(result)):
            presto_data_list.append(list(result.iloc[i, :]))

        # 实例化mysql2
        operate_mysql2 = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # presto_data_list 是以list为元素的list(批量插入)
        if presto_data_list:
            # 每次写入50条数据
            step = 50
            length_data = len(presto_data_list)
            r = math.ceil(length_data / step)
            for i in range(r):
                tmp_list = presto_data_list[i * step:(i + 1) * step]
                batch_sql = ','.join([str(tuple(a)) for a in tmp_list])
                insert_sql = "insert into zhw_dws_msg_recall_statistics(part_day ,type ,issue ,hb_rece_user ,hb_rece_num ,hb_rece_money ,hb_use_user ,hb_use_num ,hb_use_money ,curr_user ,youxiao_user ,curr_fufei_user ,curr_fufei_num ,curr_fufei_money ,curr_hb_did_user ,curr_hb_did_num ,curr_hb_did_money ,threeday_login_user ,threeday_fufei_user ,threeday_fufei_num ,threeday_fufei_money ,threeday_hb_fufei_user ,threeday_hb_fufei_num ,threeday_hb_fufei_money ,sevenday_login_user ,fifteen_login_user ,sevenday_fufei_user ,fifteen_fufei_user,youxiao_fufei_user ,youxiao_fufei_num ,youxiao_fufei_money ,youxiao_hb_fufei_user ,youxiao_hb_fufei_num ,youxiao_hb_fufei_money) values {0};".format(
                    batch_sql)
                operate_mysql2.insert_data(insert_sql)
        operate_mysql2.close_conn()

    def zhw_dws_red_packet_type_id_msg(self, part_day):
        """
        :param part_day: 日期参数
        :return: 短信红包发送量 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # msg 推送的红包类型和红包id
        msg_config_sql = """
            select packet_type,packet_id
            from red_packet_trigger_way 
            where way = 'msg'
        """
        msg_config_result = operate_mysql.query_data(msg_config_sql)
        msg_config_result_columns = operate_mysql.query_data_index()
        msg_config_df = pd.DataFrame(msg_config_result)
        msg_config_df.columns = msg_config_result_columns

        packet_type_tuple = tuple(set(msg_config_df.packet_type))
        packet_id_tuple = tuple(set(msg_config_df.packet_id))

        # 清空数据
        mysql_sql = "delete from zhw_dws_red_packet_type_id_msg"
        operate_mysql.update_data(mysql_sql)
        operate_mysql.close_conn()

        sql_task_record = """
            select task_id,red_packet_ids, red_packet_type
            from zhwhigh.zhw_task_record
            where red_packet_ids  is not null 
            and red_packet_type in {}
            group by 1,2,3
        """.format(packet_type_tuple)
        result_task_record = operate_presto.query_data(sql_task_record)
        result_task_record_columns = operate_presto.query_data_index()
        result_task_record = pd.DataFrame(result_task_record)
        result_task_record.columns = result_task_record_columns

        task_id_, red_packet_type_, red_packet_ids_ = [], [], []
        for idx, item in result_task_record.iterrows():
            task_id = item['task_id']
            red_packet_type = item['red_packet_type']
            red_packet_ids = [int(a) for a in item['red_packet_ids'].split(',')]

            tmp_len = len(red_packet_ids)

            task_id_.extend([task_id] * tmp_len)
            red_packet_type_.extend([red_packet_type] * tmp_len)
            red_packet_ids_.extend(red_packet_ids)

        df_task_record = pd.DataFrame(
            {'task_id_': task_id_, 'red_packet_type_': red_packet_type_, 'red_packet_ids_': red_packet_ids_})
        task_id_tuple = tuple(task_id_)

        df_task_record_2 = df_task_record[df_task_record.red_packet_ids_.isin(packet_id_tuple)]

        # 红包任务下的短信发送记录
        sql_send_sms = """
            select format_datetime(create_time,'yyyy-MM-dd') as create_dt,task_id,count(*) as send_sms_count
            from zhwhigh.zhw_log_send_sms
            where task_id in {}
            and format_datetime(create_time,'yyyy-MM')>='2022-03'
            group by 1,2
        """.format(task_id_tuple)

        presto_data_list = operate_presto.query_data(sql_send_sms)
        presto_data_list_columns = operate_presto.query_data_index()

        operate_presto.close_conn()

        result_send_sms = pd.DataFrame(presto_data_list)
        result_send_sms.columns = presto_data_list_columns

        df_ = pd.merge(result_send_sms, df_task_record_2, left_on=['task_id'], right_on=['task_id_'])

        df_ = df_[['create_dt', 'task_id', 'send_sms_count', 'task_id_', 'red_packet_type_', 'red_packet_ids_']]

        presto_data_list = []
        for i in range(len(df_)):
            presto_data_list.append(list(df_.iloc[i, :]))

        # 实例化mysql2
        operate_mysql2 = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        if presto_data_list:
            # 每次写入50条数据
            step = 50
            length_data = len(presto_data_list)
            r = math.ceil(length_data / step)
            for i in range(r):
                tmp_list = presto_data_list[i * step:(i + 1) * step]
                batch_sql = ','.join([str(tuple(a)) for a in tmp_list])
                insert_sql = "insert into zhw_dws_red_packet_type_id_msg(create_dt ,task_id ,send_sms_count ,task_id_ ,red_packet_type_ ,red_packet_ids_) values {0};".format(
                    batch_sql)
                operate_mysql2.insert_data(insert_sql)
        operate_mysql2.close_conn()

    def zhw_dws_push_recall_statistics(self, part_day):
        """
        :param part_day: 日期参数
        :return: PUSH 召回 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # push 推送的红包类型和红包id
        msg_config_sql = """
            select packet_type,packet_id
            from red_packet_trigger_way 
            where way = 'push'
        """

        msg_config_result = operate_mysql.query_data(msg_config_sql)
        msg_config_result_columns = operate_mysql.query_data_index()
        msg_config_df = pd.DataFrame(msg_config_result)
        msg_config_df.columns = msg_config_result_columns

        packet_type_tuple = tuple(set(msg_config_df.packet_type))
        packet_id_tuple = tuple(set(msg_config_df.packet_id))

        # 清空近8日数据
        mysql_sql = "delete from zhw_dws_push_recall_statistics where part_day between '{0}' and date_add(str_to_date('{0}','%Y-%m-%d'),interval 3 day)".format(
            part_day)
        operate_mysql.update_data(mysql_sql)
        operate_mysql.close_conn()

        start_time = time.time()
        # 红包领取 & 红包使用(sql_1 + sql_4) -- 金额不存在重复
        sql_1 = '''
            select
                a.part_day,
                a.type,
                a.issue,
                count(distinct a.jkx_userid)  as hb_rece_user,-- 红包领取人数
                count(a.id) as hb_rece_num, -- 红包领取数
                sum(a.money) as hb_rece_money, -- 红包领取金额
                count(distinct case when a.usemoney>0 then a.jkx_userid end ) as hb_use_user,--红包累积使用金额
                count(case when a.usemoney>0 then a.id end) as hb_use_num,
                sum(a.usemoney) as hb_use_money, -- 红包累积使用金额

                count(distinct c.userid)  curr_hb_did_user,-- 当日领取当日红包使用人数
                count(c.id) as curr_hb_did_num, -- 当日领取当日红包下单订单量
                sum(c.pm) as curr_hb_did_money  -- 当日领取当日红包下单金额
            from
            (
                select part_day,type,issue,jkx_userid,id,money,usemoney
                from zhwdb.zhw_hongbao
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                -- 满足以下条件的数据从2022年3月开始（2022-03-05）
                and (type in {1} and issue in {2})
            ) a

            left join 
            (
                select order_id,hb_id,part_day 
                from zhwdb.zhw_hongbao_order 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) b 
            on a.id=b.hb_id and a.part_day=b.part_day 
            left join 
            (
                select userid,id,pm,part_day 
                from zhwdb.zhw_dingdan 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
            ) c 
            on b.order_id=c.id  and b.part_day=c.part_day 
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_1 = operate_presto.query_data(sql_1)

        if not presto_data_list_1:
            return

        result_columns_1 = operate_presto.query_data_index()
        df_1 = pd.DataFrame(presto_data_list_1)
        df_1.columns = result_columns_1
        time.sleep(1)
        end_time = time.time()
        print('sql_1', end_time - start_time)

        start_time = time.time()
        # 领取红包且活跃，领取红包有效期内活跃 -- 不统计金额
        sql_2 = '''
            select
                a.part_day,
                a.type,
                a.issue,
                count(distinct b.userid) as curr_user,  -- 当日领取且活跃人数
                count(distinct c.userid) as youxiao_user  -- 当日领取且有效期内活跃人数
            from
            (
                select part_day,type,issue,jkx_userid,recetime,outtime
                from zhwdb.zhw_hongbao
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a
            left join
            (
                select part_day,userid,usertimer
                from zhwdb.zhw_user_login_log_extend
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                -- group by 1,2
            ) b ---当天的活跃情况
            on a.jkx_userid=b.userid and a.part_day=b.part_day and to_unixtime(b.usertimer) >= a.recetime
            left join
            (
                select part_day,userid,usertimer
                from zhwdb.zhw_user_login_log_extend
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar)
                -- group by 1,2,3
            ) c ---红包有效期内一般[1,10]的活跃情况
            on a.jkx_userid=c.userid and to_unixtime(c.usertimer) between a.recetime and a.outtime
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_2 = operate_presto.query_data(sql_2)
        if not presto_data_list_2:
            df_2 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'curr_user': [0], 'youxiao_user': [0]}
            )
        else:
            result_columns_2 = operate_presto.query_data_index()
            df_2 = pd.DataFrame(presto_data_list_2)
            df_2.columns = result_columns_2
        time.sleep(1)
        end_time = time.time()
        print('sql_2', end_time - start_time)

        start_time = time.time()
        # 当日领取当日下单  金额不存在重复
        sql_3 = '''
            select
                a.part_day,
                a.type,
                a.issue,
                count(distinct b.userid) as curr_fufei_user,--当日领取当日下单人数
                count(b.id) as curr_fufei_num,
                sum(b.pm) as curr_fufei_money
            from
            (
                select part_day,type,issue,jkx_userid,recetime,outtime
                from zhwdb.zhw_hongbao
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a
            left join
            (
                select part_day,userid,id,pm,add_time
                from zhwdb.zhw_dingdan
                where  part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
            ) b
            -- 当日领取当日下单(领取后下单)
            on a.part_day=b.part_day and a.jkx_userid=b.userid and to_unixtime(b.add_time) between a.recetime and a.outtime
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_3 = operate_presto.query_data(sql_3)
        if not presto_data_list_3:
            df_3 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'curr_fufei_user': [0], 'curr_fufei_num': [0],
                 'curr_fufei_money': [0]}
            )
        else:
            result_columns_3 = operate_presto.query_data_index()
            df_3 = pd.DataFrame(presto_data_list_3)
            df_3.columns = result_columns_3
        time.sleep(1)
        end_time = time.time()
        print('sql_3', end_time - start_time)

        start_time = time.time()
        # 3日内活跃用户数 不统计金额
        sql_5 = '''
            /*3日内活跃用户数*/
            select a.part_day,a.type,a.issue,
            count(distinct b.userid) as threeday_login_user  --当日领取且3日内活跃
            from 
            (
                select part_day,type,issue,jkx_userid,recetime as dt1 
                from zhwdb.zhw_hongbao 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            left join 
            (
                select part_day,userid,to_unixtime(usertimer) as dt2 
                from zhwdb.zhw_user_login_log_extend 
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
            ) b 
            -- 3日内活跃
            -- on b.dt2>=a.dt1 and b.dt2<=a.dt1+259200 and a.jkx_userid=b.userid 
            on b.dt2>=a.dt1 and b.dt2<=to_unixtime(date_add('day',2,cast(a.part_day as date)))+ 86399.99 and a.jkx_userid = b.userid
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_5 = operate_presto.query_data(sql_5)

        if not presto_data_list_5:
            df_5 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'threeday_login_user': [0]
                 }
            )
        else:
            result_columns_5 = operate_presto.query_data_index()
            df_5 = pd.DataFrame(presto_data_list_5)
            df_5.columns = result_columns_5
        time.sleep(1)
        end_time = time.time()
        print('sql_5', end_time - start_time)

        start_time = time.time()
        # 3日内付费 -- 3日下单，3日红包下单 分开
        sql_6 = '''
            select coalesce(t1.part_day,t2.part_day) as part_day,coalesce(t1.type,t2.type) as type,
            coalesce(t1.issue,t2.issue) as issue,
            max(threeday_fufei_user) as threeday_fufei_user,
            max(threeday_fufei_num) as threeday_fufei_num,
            max(threeday_fufei_money) as threeday_fufei_money,
            max(threeday_hb_fufei_user) as threeday_hb_fufei_user,
            max(threeday_hb_fufei_num) as threeday_hb_fufei_num,
            max(threeday_hb_fufei_money) as threeday_hb_fufei_money
            from 
            (
                select a.part_day,a.type,a.issue,
                count(distinct b.userid) as threeday_fufei_user,  --当日领取且3日内下单
                count(distinct b.id) as threeday_fufei_num, --当日领取且3日内下单量
                sum(b.pm) as threeday_fufei_money --当日领取且3日内下单金额
                from
                (
                    select part_day,type,issue,jkx_userid,recetime as dt1 
                    from zhwdb.zhw_hongbao 
                    where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                    and (type in {1} and issue in {2})
                ) a 
                left join 
                (
                    select part_day,userid,to_unixtime(add_time) as dt2,id,pm
                    from zhwdb.zhw_dingdan 
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
                ) b 
                -- 3日内下单
                -- on b.dt2>=a.dt1 and b.dt2<=a.dt1+259200 and a.jkx_userid=b.userid 
                on b.dt2>=a.dt1 and b.dt2<=to_unixtime(date_add('day',2,cast(a.part_day as date)))+ 86399.99 and a.jkx_userid=b.userid 
                group by 1,2,3
            ) t1 
            full join  
            (
                select a.part_day,a.type,a.issue,
                count(distinct c.userid) as threeday_hb_fufei_user,  --当日领取且3日内红包下单
                count(distinct c.id) as threeday_hb_fufei_num, --当日领取且3日内红包下单量
                sum(c.pm) as threeday_hb_fufei_money --当日领取且3日内红包下单金额
                from 
                (
                    select part_day,type,issue,jkx_userid,recetime as dt1,id
                    from zhwdb.zhw_hongbao 
                    where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                    and (type in {1} and issue in {2})
                ) a 
                join 
                ( 
                    select part_day,jkx_userid as userid,order_id,usetime,hb_id
                    from zhwdb.zhw_hongbao_order 
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
                    and (type in {1} and issue in {2})
                ) b 
                on a.id =b.hb_id
                join
                (
                    select part_day,userid,to_unixtime(add_time) as dt3,id,pm
                    from zhwdb.zhw_dingdan 
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
                ) c 
                -- 3日内红包下单
                -- on c.dt3>=a.dt1 and c.dt3<=a.dt1+259200 and a.jkx_userid=c.userid 
                on b.order_id = c.id and c.dt3>=a.dt1 and c.dt3<=to_unixtime(date_add('day',2,cast(a.part_day as date)))+ 86399.99
                group by 1,2,3
            ) t2 
            on t1.part_day = t2.part_day and t1.type = t2.type and t1.issue = t2.issue
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_6 = operate_presto.query_data(sql_6)

        if not presto_data_list_6:
            df_6 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'threeday_fufei_user': [0],
                 'threeday_fufei_num': [0], 'threeday_fufei_money': [0], 'threeday_hb_fufei_user': [0],
                 'threeday_hb_fufei_num': [0], 'threeday_hb_fufei_money': [0]
                 }
            )
        else:
            result_columns_6 = operate_presto.query_data_index()
            df_6 = pd.DataFrame(presto_data_list_6)
            df_6.columns = result_columns_6
        time.sleep(1)
        end_time = time.time()
        print('sql_6', end_time - start_time)

        start_time = time.time()
        # sql_7
        sql_7 = '''
            /*7日留存率*/
            select 
            a.part_day,
            a.type ,
            a.issue,
            count(distinct c.userid) sevenday_login_user
            from 
            (
                select part_day,type,issue,jkx_userid,recetime
                from zhwdb.zhw_hongbao  
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            left join 
            (
                select part_day,userid,usertimer
                from zhwdb.zhw_user_login_log_extend  
                where part_day between '{0}' and cast(date_add('day',14,cast('{0}' as date)) as varchar) 
                -- group by 1,2
            ) c  
            -- 7日活跃留存
            -- on a.jkx_userid=c.userid and date(c.part_day)>date(a.part_day)
            -- and date(c.part_day)<date_add('day',7,date(a.part_day))
            on a.jkx_userid=c.userid and to_unixtime(c.usertimer)>=a.recetime
            and to_unixtime(c.usertimer)<=to_unixtime(date_add('day',6,cast(a.part_day as date)))+ 86399.99
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_7 = operate_presto.query_data(sql_7)
        if not presto_data_list_7:
            df_7 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'sevenday_login_user': [0]
                 }
            )
        else:
            result_columns_7 = operate_presto.query_data_index()
            df_7 = pd.DataFrame(presto_data_list_7)
            df_7.columns = result_columns_7
        time.sleep(1)
        end_time = time.time()
        print('sql_7', end_time - start_time)


        start_time = time.time()
        # sql_8
        sql_8 = '''
            /*15日留存率*/
            select 
            a.part_day,
            a.type ,
            a.issue,
            count(distinct cc.userid)  fifteen_login_user
            from 
            (
                select part_day,type,issue,jkx_userid,recetime
                from zhwdb.zhw_hongbao  
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            left join 
            (
                select part_day,userid,usertimer
                from zhwdb.zhw_user_login_log_extend  
                where part_day between '{0}' and cast(date_add('day',22,cast('{0}' as date)) as varchar) 
                -- group by 1,2
            ) cc  
            -- 15日活跃留存
            -- on a.jkx_userid=cc.userid and date(cc.part_day)>date(a.part_day)
            -- and date(cc.part_day)<date_add('day',15,date(a.part_day))
            on a.jkx_userid=cc.userid and to_unixtime(cc.usertimer)>=a.recetime
            and to_unixtime(cc.usertimer)<=to_unixtime(date_add('day',14,cast(a.part_day as date)))+ 86399.99
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_8 = operate_presto.query_data(sql_8)
        if not presto_data_list_8:
            df_8 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'sevenday_login_user': [0],
                 'fifteen_login_user': [0]}
            )
        else:
            result_columns_8 = operate_presto.query_data_index()
            df_8 = pd.DataFrame(presto_data_list_8)
            df_8.columns = result_columns_8
        time.sleep(1)
        end_time = time.time()
        print('sql_8', end_time - start_time)


        start_time = time.time()
        # sql_q
        sql_9 = '''
            /*7日付费留存率   使用红包后7天内有下单 */
            select 
            c.part_day,
            c.type,
            c.issue,
            count(distinct d.userid)  as sevenday_fufei_user
            from 
            (   -- 领取日期，使用红包日期
                select a.part_day,a.type,a.issue,b.jkx_userid,max(b.usetime) as max_usetime,max(b.part_day) as max_user_day
                from 
                (
                    select part_day,type,issue,id,jkx_userid,recetime
                    from zhwdb.zhw_hongbao  
                    where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                    and (type in {1} and issue in {2})
                ) a  
                left join 
                (   -- 红包有效期[1,10]
                    select jkx_userid,hb_id,usetime,part_day
                    from zhwdb.zhw_hongbao_order 
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
                    and (type in {1} and issue in {2})
                ) b 
                -- 红包有效期 最长7日最短1日
                on a.id=b.hb_id 
                group by 1,2,3,4
            ) c
            left join 
            (
                select add_time,id,userid,pm 
                from zhwdb.zhw_dingdan 
                where part_day between '{0}' and cast(date_add('day',26,cast('{0}' as date)) as varchar) 
            ) d 
            -- 7日内付费
            -- on c.jkx_userid=d.userid and d.dt3>c.mdt2 and d.dt3<=date_add('day',7,c.mdt2)
            on c.jkx_userid=d.userid and to_unixtime(d.add_time)>=c.max_usetime 
            and to_unixtime(d.add_time)<=to_unixtime(date_add('day',6,cast(c.max_user_day as date)))+ 86399.99
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_9 = operate_presto.query_data(sql_9)
        if not presto_data_list_9:
            df_9 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'sevenday_fufei_user': [0]
                 }
            )
        else:
            result_columns_9 = operate_presto.query_data_index()
            df_9 = pd.DataFrame(presto_data_list_9)
            df_9.columns = result_columns_9
        time.sleep(1)
        end_time = time.time()
        print('sql_9', end_time - start_time)

        start_time = time.time()
        # sql_10
        sql_10 = '''
            /*15日付费留存率   使用红包后15天内有下单 */
            select 
            c.part_day,
            c.type,
            c.issue,
            count(distinct dd.userid) as fifteen_fufei_user
            from 
            (   -- 领取日期，使用红包日期
                select a.part_day,a.type,a.issue,b.jkx_userid,max(b.usetime) as max_usetime,max(b.part_day) as max_user_day
                from 
                (
                    select part_day,type,issue,id,jkx_userid,recetime
                    from zhwdb.zhw_hongbao  
                    where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                    and (type in {1} and issue in {2})
                ) a  
                left join 
                (   -- 红包有效期[1,10]
                    select jkx_userid,hb_id,usetime,part_day
                    from zhwdb.zhw_hongbao_order 
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar) 
                    and (type in {1} and issue in {2})
                ) b 
                -- 红包有效期 最长7日最短1日
                on a.id=b.hb_id 
                group by 1,2,3,4
            ) c
            -- 15日内付费
            left  join 
            (
                select add_time,id,userid,pm 
                from zhwdb.zhw_dingdan 
                where part_day between '{0}' and cast(date_add('day',31,cast('{0}' as date)) as varchar) 
            ) dd 
            -- on c.jkx_userid=dd.userid and dd.dt3>c.mdt2 and dd.dt3<=date_add('day',15,c.mdt2)
            on c.jkx_userid=dd.userid and to_unixtime(dd.add_time)>=c.max_usetime 
            and to_unixtime(dd.add_time)<=to_unixtime(date_add('day',14,cast(c.max_user_day as date)))+ 86399.99
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_10 = operate_presto.query_data(sql_10)
        if not presto_data_list_10:
            df_10 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0],
                 'fifteen_fufei_user': [0]
                 }
            )
        else:
            result_columns_10 = operate_presto.query_data_index()
            df_10 = pd.DataFrame(presto_data_list_10)
            df_10.columns = result_columns_10
        time.sleep(1)
        end_time = time.time()
        print('sql_10', end_time - start_time)


        sql_21 = '''
                /*有效期内下单*/
                select a.part_day,a.type,a.issue,
                count(distinct b.userid) as youxiao_fufei_user,  --有效期内下单用户数
                count(distinct b.id) as youxiao_fufei_num, --有效期内下单量
                sum(b.pm) as youxiao_fufei_money --有效期内下单金额
                from 
                (
                    select part_day,type,issue,jkx_userid,recetime as dt1,outtime,recetime
                    from zhwdb.zhw_hongbao 
                    where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                    and (type in {1} and issue in {2})
                ) a 
                left join
                (
                    select *
                    from zhwdb.zhw_dingdan
                    where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar)
                ) b 
                on a.jkx_userid=b.userid and to_unixtime(b.add_time) between a.recetime and a.outtime
                group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_21 = operate_presto.query_data(sql_21)
        if not presto_data_list_21:
            df_21 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'youxiao_fufei_user': [0],
                 'youxiao_fufei_num': [0], 'youxiao_fufei_money': [0]}
            )
        else:
            result_columns_21 = operate_presto.query_data_index()
            df_21 = pd.DataFrame(presto_data_list_21)
            df_21.columns = result_columns_21
        time.sleep(1)
        end_time = time.time()
        print('sql_21', end_time - start_time)

        start_time = time.time()
        sql_22 = '''
            /*有效期内红包下单*/
            select a.part_day,a.type,a.issue,
            count(distinct c.userid) as youxiao_hb_fufei_user,  --有效期内下单用户数
            count(distinct c.id) as youxiao_hb_fufei_num, --有效期内下单量
            sum(c.pm) as youxiao_hb_fufei_money --有效期内下单金额
            from 
            (
                select *
                from zhwdb.zhw_hongbao 
                where part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            join 
            (
                select *
                from zhwdb.zhw_hongbao_order 
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) b 
            on a.id = b.hb_id and b.usetime between a.recetime and a.outtime
            join
            (
                select *
                from zhwdb.zhw_dingdan
                where part_day between '{0}' and cast(date_add('day',13,cast('{0}' as date)) as varchar)
            ) c 
            on b.order_id = c.id and to_unixtime(c.add_time) between a.recetime and a.outtime
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list_22 = operate_presto.query_data(sql_22)
        if not presto_data_list_22:
            df_22 = pd.DataFrame(
                {'part_day': ['2099-01-01'], 'type': [0], 'issue': [0], 'youxiao_hb_fufei_user': [0],
                 'youxiao_hb_fufei_num': [0], 'youxiao_hb_fufei_money': [0]}
            )
        else:
            result_columns_22 = operate_presto.query_data_index()
            df_22 = pd.DataFrame(presto_data_list_22)
            df_22.columns = result_columns_22
        time.sleep(1)
        end_time = time.time()
        print('sql_22', end_time - start_time)

        operate_presto.close_conn()

        result = pd.merge(df_1, df_2, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_3, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_5, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_6, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_7, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_8, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_9, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_10, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_21, on=['part_day', 'type', 'issue'], how='left')
        result = pd.merge(result, df_22, on=['part_day', 'type', 'issue'], how='left')

        result = result[
            ['part_day', 'type', 'issue', 'hb_rece_user', 'hb_rece_num', 'hb_rece_money', 'hb_use_user', 'hb_use_num',
             'hb_use_money', 'curr_user', 'youxiao_user', 'curr_fufei_user', 'curr_fufei_num', 'curr_fufei_money',
             'curr_hb_did_user', 'curr_hb_did_num', 'curr_hb_did_money', 'threeday_login_user', 'threeday_fufei_user',
             'threeday_fufei_num', 'threeday_fufei_money', 'threeday_hb_fufei_user', 'threeday_hb_fufei_num',
             'threeday_hb_fufei_money', 'sevenday_login_user', 'fifteen_login_user', 'sevenday_fufei_user',
             'fifteen_fufei_user', 'youxiao_fufei_user', 'youxiao_fufei_num', 'youxiao_fufei_money',
             'youxiao_hb_fufei_user', 'youxiao_hb_fufei_num', 'youxiao_hb_fufei_money']]
        result.fillna(0, inplace=True)
        presto_data_list = []
        for i in range(len(result)):
            presto_data_list.append(list(result.iloc[i, :]))

        # 实例化mysql2
        operate_mysql2 = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # presto_data_list 是以list为元素的list(批量插入)
        if presto_data_list:
            # 每次写入50条数据
            step = 50
            length_data = len(presto_data_list)
            r = math.ceil(length_data / step)
            for i in range(r):
                tmp_list = presto_data_list[i * step:(i + 1) * step]
                batch_sql = ','.join([str(tuple(a)) for a in tmp_list])
                insert_sql = "insert into zhw_dws_push_recall_statistics(part_day ,type ,issue ,hb_rece_user ,hb_rece_num ,hb_rece_money ,hb_use_user ,hb_use_num ,hb_use_money ,curr_user ,youxiao_user ,curr_fufei_user ,curr_fufei_num ,curr_fufei_money ,curr_hb_did_user ,curr_hb_did_num ,curr_hb_did_money ,threeday_login_user ,threeday_fufei_user ,threeday_fufei_num ,threeday_fufei_money ,threeday_hb_fufei_user ,threeday_hb_fufei_num ,threeday_hb_fufei_money ,sevenday_login_user ,fifteen_login_user ,sevenday_fufei_user ,fifteen_fufei_user,youxiao_fufei_user ,youxiao_fufei_num ,youxiao_fufei_money ,youxiao_hb_fufei_user ,youxiao_hb_fufei_num ,youxiao_hb_fufei_money) values {0};".format(
                    batch_sql)
                operate_mysql2.insert_data(insert_sql)
        operate_mysql2.close_conn()

    def zhw_dws_msg_hb_expired_retention7(self, part_day):
        """
        :param part_day: 日期参数
        :return: 短信召回 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # msg 推送的红包类型和红包id
        msg_config_sql = """
            select packet_type,packet_id
            from red_packet_trigger_way 
            where way = 'msg'
        """
        msg_config_result = operate_mysql.query_data(msg_config_sql)
        msg_config_result_columns = operate_mysql.query_data_index()
        msg_config_df = pd.DataFrame(msg_config_result)
        msg_config_df.columns = msg_config_result_columns

        packet_type_tuple = tuple(set(msg_config_df.packet_type))
        packet_id_tuple = tuple(set(msg_config_df.packet_id))

        # -- 1 重点关注指标
        # 清空昨日数据
        delete_sql = "delete from zhw_dws_msg_hb_expired_retention7 where part_day between '{0}' and date_add(str_to_date('{0}','%Y-%m-%d'),interval 3 day)".format(
            part_day)

        operate_mysql.update_data(delete_sql)
        operate_mysql.close_conn()

        start_time = time.time()
        # 红包领取 & 红包使用(sql_1 + sql_4) 金额不存在重复
        sql_7d_retention = '''
            select a.part_day,a.type,a.issue,
                coalesce(count(distinct a.jkx_userid),0) as hb_orders_users,
                coalesce(count(distinct c.userid),0) as expired_7d_order_users,
                coalesce(count(c.id),0) as expired_7d_orders,
                coalesce(sum(c.pm),0.00) as expired_7d_order_pm
            from 
            (	-- 使用红包下单
                select part_day,type,issue,jkx_userid,hb_id
                from hive.zhwdb.zhw_hongbao_order
                where true 
                and part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            join 
            (
                -- 领取红包(使用红包前1个月内领取）
                select id,outtime
                from hive.zhwdb.zhw_hongbao 
                where part_day between cast(date_add('day',-33,cast('{0}' as date)) as varchar) and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) b 
            on a.hb_id = b.id
            left join 
            (
                -- 
                select part_day,id,pm,userid
                from hive.zhwdb.zhw_dingdan 
                where part_day between cast(date_add('day',0,cast('{0}' as date)) as varchar) and cast(date_add('day',33,cast('{0}' as date)) as varchar)
            ) c 
            on a.jkx_userid = c.userid 
            -- 有效期后7天内下单
            and c.part_day > format_datetime(from_unixtime(b.outtime),'yyyy-MM-dd') 
            and c.part_day<=format_datetime(date_add('day',7,from_unixtime(b.outtime)),'yyyy-MM-dd')
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list = operate_presto.query_data(sql_7d_retention)

        # 实例化mysql2
        operate_mysql2 = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # presto_data_list 是以list为元素的list(批量插入)
        if presto_data_list:
            # 每次写入50条数据
            step = 50
            length_data = len(presto_data_list)
            r = math.ceil(length_data / step)
            for i in range(r):
                tmp_list = presto_data_list[i * step:(i + 1) * step]
                batch_sql = ','.join([str(tuple(a)) for a in tmp_list])
                insert_sql = "insert into zhw_dws_msg_hb_expired_retention7(part_day,type,issue,hb_orders_users,expired_7d_order_users,expired_7d_orders,expired_7d_order_pm) values {0};".format(
                    batch_sql)
                operate_mysql2.insert_data(insert_sql)
        operate_mysql2.close_conn()



    def zhw_dws_msg_hb_expired_retention30(self, part_day):
        """
        :param part_day: 日期参数
        :return: 短信召回 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # msg 推送的红包类型和红包id
        msg_config_sql = """
            select packet_type,packet_id
            from red_packet_trigger_way 
            where way = 'msg'
        """
        msg_config_result = operate_mysql.query_data(msg_config_sql)
        msg_config_result_columns = operate_mysql.query_data_index()
        msg_config_df = pd.DataFrame(msg_config_result)
        msg_config_df.columns = msg_config_result_columns

        packet_type_tuple = tuple(set(msg_config_df.packet_type))
        packet_id_tuple = tuple(set(msg_config_df.packet_id))

        # -- 1 重点关注指标
        # 清空昨日数据
        delete_sql = "delete from zhw_dws_msg_hb_expired_retention30 where part_day between '{0}' and date_add(str_to_date('{0}','%Y-%m-%d'),interval 3 day)".format(
            part_day)

        operate_mysql.update_data(delete_sql)

        operate_mysql.close_conn()

        start_time = time.time()
        # 红包领取 & 红包使用(sql_1 + sql_4) 金额不存在重复
        sql_30d_retention = '''
            select a.part_day,a.type,a.issue,
                coalesce(count(distinct a.jkx_userid),0) as hb_orders_users,
                coalesce(count(distinct c.userid),0) as expired_30d_order_users,
                coalesce(count(c.id),0) as expired_30d_orders,
                coalesce(sum(c.pm),0.00) as expired_30d_order_pm
            from 
            (	-- 使用红包下单
                select part_day,type,issue,jkx_userid,hb_id
                from hive.zhwdb.zhw_hongbao_order
                where true 
                and part_day between '{0}' and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) a 
            join 
            (
                -- 领取红包(使用红包前1个月内领取）
                select id,outtime
                from hive.zhwdb.zhw_hongbao 
                where part_day between cast(date_add('day',-33,cast('{0}' as date)) as varchar) and cast(date_add('day',3,cast('{0}' as date)) as varchar)
                and (type in {1} and issue in {2})
            ) b 
            on a.hb_id = b.id
            left join 
            (
                -- 
                select part_day,id,pm,userid
                from hive.zhwdb.zhw_dingdan 
                where part_day between cast(date_add('day',0,cast('{0}' as date)) as varchar) and cast(date_add('day',63,cast('{0}' as date)) as varchar)
            ) c 
            on a.jkx_userid = c.userid 
            -- 有效期后30天内下单
            and c.part_day > format_datetime(from_unixtime(b.outtime),'yyyy-MM-dd') 
            and c.part_day<=format_datetime(date_add('day',30,from_unixtime(b.outtime)),'yyyy-MM-dd')
            group by 1,2,3
        '''.format(part_day, packet_type_tuple, packet_id_tuple)

        presto_data_list = operate_presto.query_data(sql_30d_retention)

        # 实例化mysql2
        operate_mysql2 = OperateMysql(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # presto_data_list 是以list为元素的list(批量插入)
        if presto_data_list:
            # 每次写入50条数据
            step = 50
            length_data = len(presto_data_list)
            r = math.ceil(length_data / step)
            for i in range(r):
                tmp_list = presto_data_list[i * step:(i + 1) * step]
                batch_sql = ','.join([str(tuple(a)) for a in tmp_list])
                insert_sql = "insert into zhw_dws_msg_hb_expired_retention30(part_day,type,issue,hb_orders_users,expired_30d_order_users,expired_30d_orders,expired_30d_order_pm) values {0};".format(
                    batch_sql)
                operate_mysql2.insert_data(insert_sql)
        operate_mysql2.close_conn()


if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1

    # # 短信召回效果——数据（每日 以4日为单位重跑） 领取红包后预估最长30天失效
    # # lst = [4 * i for i in range(1, 9)]
    # lst = [4]
    # lst.reverse()
    # for i in lst:
    #     start_time = time.time()
    #     day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
    #
    #     dws1 = Dws()
    #     dws1.zhw_dws_msg_recall_statistics(day_last_1)
    #
    #     end_time = time.time()
    #     print('执行完成：', day_last_1, '耗时:', end_time - start_time)
    #
    # # 短信召回效果——数据（重跑所有短信发送量）
    # day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    # dws2 = Dws()
    # dws2.zhw_dws_red_packet_type_id_msg(day_last_1)
    #
    # # PUSH召回效果——数据（每日 以4日为单位重跑） 领取红包后预估最长30天失效
    # lst = [4 * i for i in range(1, 9)]
    # lst = [4]
    # lst.reverse()
    # for i in lst:
    #     start_time = time.time()
    #
    #     day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
    #
    #     dws1 = Dws()
    #     dws1.zhw_dws_push_recall_statistics(day_last_1)
    #
    #     end_time = time.time()
    #     print('执行完成：', day_last_1, '耗时:', end_time - start_time)
    #
    #
    # # 使用红包下单人群红包过期后7日的留存
    # lst = [4 * i for i in range(1, 9)]
    # lst.reverse()
    # for i in lst:
    #     start_time = time.time()
    #
    #     day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
    #
    #     dws1 = Dws()
    #     dws1.zhw_dws_msg_hb_expired_retention7(day_last_1)
    #
    #     end_time = time.time()
    #     print('执行完成：', day_last_1, '耗时:', end_time - start_time)

    # 使用红包下单人群红包过期后30日的留存
    lst = [4 * i for i in range(1, 2)]
    lst.reverse()
    for i in lst:
        start_time = time.time()

        day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1

        dws1 = Dws()
        dws1.zhw_dws_msg_hb_expired_retention30(day_last_1)

        end_time = time.time()
        print('执行完成：', day_last_1, '耗时:', end_time - start_time)
