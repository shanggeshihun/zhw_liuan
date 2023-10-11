#!/usr/bin/python
# -*- coding: utf-8 -*-
'''小时级封装今日各游戏的撤单情况'''
import datetime
import pandas as pd
import numpy as np
from pyhive import presto
from sqlalchemy import create_engine
import jieba.analyse
import configparser
import redis
import yagmail
import pymysql
from collections import defaultdict
from dateutil.relativedelta import relativedelta

# ------------------------参数变量区----------------------------
# 配置运营数据库地址
cf = configparser.ConfigParser()
if cf.read("C:/Users/Administrator/Downloads/config.ini") == []:
    """服务器模式"""
    cf.read("/usr/model/zhw_product/config/config.ini")
else:
    """本地模式"""
    cf.read("C:/Users/Administrator/Downloads/config.ini")

redis_host = cf.get("product_redis", "host")
redis_port = cf.get("product_redis", "port")
redis_password = cf.get("product_redis", "password")
redis_db = cf.get("product_redis", "user_db")

host = cf.get("hive_presto", "host")
username = cf.get("hive_presto", "username")
port = cf.get("hive_presto", "port")
schema = cf.get("hive_presto", "schema")
catalog = cf.get("hive_presto", "catalog")
presto_db = presto.connect(host=host, port=port, username=username, schema=schema, catalog=catalog)

host = cf.get("Mysql-sjwj", "host")
user = cf.get("Mysql-sjwj", "user")
password = cf.get("Mysql-sjwj", "password")
DB = cf.get("Mysql-sjwj", "DB")
port = cf.get("Mysql-sjwj", "port")
cnx = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + DB, echo=False)
# ------------------------参数配置----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
day_now = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')  # t-1
day_now_new = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
day_last_db = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y%m%d')  # t-2
day_last_hb = (datetime.datetime.now() - datetime.timedelta(days=8)).strftime('%Y%m%d')  # t-8
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')  # t-30
day_last_15 = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')  # t-7
day_now_H = (datetime.datetime.now()).strftime('%Y%m%d%H')  # h
day_now_H_Last = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h-1
day_now_H_Last_2 = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime('%Y%m%d%H')  # h-2
day_now_H_Last_3 = (datetime.datetime.now() - datetime.timedelta(hours=3)).strftime('%Y%m%d%H')  # h-2
day_now_H_Last_4 = (datetime.datetime.now() - datetime.timedelta(hours=4)).strftime('%Y%m%d%H')  # h-4
day_now_H_Last_6 = (datetime.datetime.now() - datetime.timedelta(hours=6)).strftime('%Y%m%d%H')  # h-4
###先缩短到8个小时，观测下效果
day_now_H_Last_13 = (datetime.datetime.now() - datetime.timedelta(hours=8)).strftime('%Y%m%d%H')  # h-12

month = (datetime.date.today()).strftime('%Y-%m')  # t-1 #当月日期
last_day_month = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m')  # t-1 数字
last_month = (datetime.date.today() - relativedelta(months=+1)).strftime('%Y-%m')  # t-1 #上月日期
last_month_1 = (datetime.date.today() - relativedelta(months=+2)).strftime('%Y-%m')  # t-2
# print(day_now_H,day_now_H_Last,day_now_H_Last_2,day_now_H_Last_4,day_now_H_Last_13)
# 2019121713 2019121712 2019121711 2019121709 2019121704
# 12小时的货架撤单跟踪分析


##缩短到8个小时。
##缩短到8个小时。
##缩短到8个小时。

##目标用户群
Cd_H_sql = '''
select '{2}' as Date_H,s1.hid,huserid,s1.yxqu,zh,gameid,timelimit_hao,cd_cnt,dingdan_cnt,(cd_cnt*1.00)/(1.00*dingdan_cnt) as cd_bit
from 
(
    select t1.hid,t1.huserid,t1.yxqu,t2.zh,t1.gameid,case when timelimit_id > 0 then 1 else 0 end as timelimit_hao,COUNT(DISTINCT t1.id) as dingdan_cnt
    from 
    (
        select * 
        from kudu.zhwdb.zhw_dingdan
        where part_day >= '{4}'
        and DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN '{0}' and '{1}'
        and hid not in (37,38)
        and huserid not in ('15623468694') -- 盒子商户白名单
    ) t1 
    inner join 
    (select * from kudu.zhwdb.zhw_hao where zt  in (0,1)) t2 on t1.hid = t2.id
    inner join 
    (select * from kudu.zhwdb.zhw_dingdan where part_day >= '{4}' and DATE_FORMAT(add_time,'%Y%m%d%H')= '{1}' and zt=3) t3 on t1.hid = t3.hid 
    GROUP BY 1,2,3,4,5,6
) S1
inner join
(
    SELECT t2.hid,count(distinct t2.id) as cd_cnt from
    (
        select * from kudu.zhwdb.zhw_ts
        where part_day>= '{4}'
        and lx in 
        (
        '账号描述与实际不符','健康时间','设备锁','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封','裁决之廉','信誉积分不足',
        '账号禁赛','上号器自动投诉（封号）','上号器自动投诉（账号密码错误）','会员时间到期','TP检测16-2',
        'TP检测16-2/36-2','被挤号（顶号）了','上号器自动投诉（qq冻结）','账号密码错误',
        '手动协助失败（系统自动发起投诉）','游戏账号未实名认证','上号器自动投诉 (人脸识别)','账号准备失败','客服发起-无法登录'
        )
    ) t1 
    inner join 
    (
        select * from kudu.zhwdb.zhw_dingdan
        where part_day>='{4}' 
        and DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN '{0}' and '{1}'
        and zt=3 
        and hid <>38
    ) t2 
    on t1.did = t2.id
    group by 1 
) S2
on S1.hid = S2.hid
left join
(
    select hid from datawj.datawj.high_risk_hid_hour where catch_time between '2021062508' and '2021062513'
) s3  
on S1.hid=s3.hid
where s3.hid is null
group by 1,2,3,4,5,6,7,8,9,10
having  ((cd_cnt*1.00)/(1.00*dingdan_cnt) = 1.00 and dingdan_cnt = 2) or 
    ((cd_cnt*1.00)/(1.00*dingdan_cnt) >= 0.6 and dingdan_cnt  between 3 and  5) or 
    ((cd_cnt*1.00)/(1.00*dingdan_cnt) >= 0.5 and dingdan_cnt >= 6)
order by dingdan_cnt desc
'''.format(day_now_H_Last_13, day_now_H_Last, day_now_H, day_now_H_Last_6, day_now_new)

print(Cd_H_sql)

# 货架连续异常次数计算sql  hid_time
time_count_sql = '''
    select a.hid,b.add_time,b.zt 
    from 
    (
        SELECT t2.hid,count(distinct id)  from kudu.zhwdb.zhw_dingdan t2
        where part_day >= '{2}'
        and DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN '{0}' and '{1}'
        AND t2.zt in (3,5)
        and t2.hid not in (37,38)
        group by 1
        having count(distinct id)>=2
    ) a
    left join 
    (
        SELECT hid,DATE_FORMAT(add_time,'%Y%m%d%H%s')as add_time,zt from kudu.zhwdb.zhw_dingdan t2
        where part_day >= '{2}'
        and DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN '{0}' and '{1}'
        and t2.hid <> 38
        AND t2.zt in (2,3,5)
        group by 1,3,2
    ) B
    on a.hid=b.hid
    group by 1,2,3
    order by a.hid,b.add_time
'''.format(day_now_H_Last_13, day_now_H_Last, day_now_new)

# 高危货架撤单原因分析sql(所有的撤单包括主动撤单）
reason_sql = '''
    SELECT t2.hid,
        case 
            when lx in ('无法登录（wegame错误码：282）','人脸识别','不想玩了或其它理由不玩了',
            '健康时间','设备锁','租错号了','账号描述与实际不符','账号密码错误','QQ冻结（QQ暂时无法登陆）','号被封了','账号被封',
            '复活币不足','裁决之廉','安全问题错误','信誉积分不足','账号禁赛','上号器自动投诉（qq冻结）',
            '自己的号要撤销','自己要玩','上号器自动投诉（账号密码错误）','游戏账号未实名认证','因财产密码','steam客服已冻结该帐户',
            '会员时间到期','TP检测16-2','TP检测16-2/36-2','被挤号（顶号）了','使用外挂 By 上号器','通过篡改上号器文件恶意破解,错误代码：1008 By 上号器','租客违规操作','租方打排位','租方开外挂',
            '上号器自动投诉（使用外挂）','提示有外挂残留','250','ZD主动防御','无法登陆（非密码错误问题）','一直云检测','无法下载上号器',
            '不输入账号密码','安装不了上号器','客服仲裁错误','上号器自动投诉（qq冻结）','上号器自动投诉（封号）','上号器自动投诉（账号密码错误）',
            '手动协助失败（系统自动发起投诉）','上号器自动投诉 (人脸识别)','账号准备失败','QQ/微信冻结（QQ/微信暂时无法登陆）','客服发起-无法登录') 
            then lx else '其他' 
        end lx,
        re 
    from kudu.zhwdb.zhw_ts t1
    inner join  kudu.zhwdb.zhw_dingdan t2
    on t1.did = t2.id
    where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN '{0}' and '{1}'
    AND t2.zt=3
    and t2.hid in 
    (
        SELECT distinct hid from  kudu.zhwdb.zhw_dingdan t2
        where DATE_FORMAT(add_time,'%Y%m%d%H') = '{1}'
        AND t2.zt=3
    )
    and t2.hid <> 38
    GROUP BY 1,2,3
'''.format(day_now_H_Last_13, day_now_H_Last)


# 货架连续异常次数计算函数 返回hid_time
def chedan_times(cx_sql):
    hid_time = pd.read_sql(cx_sql, con=presto_db)
    df = np.array(hid_time).tolist()
    dict_data = defaultdict(dict)
    for line in df:
        dict_data.setdefault(line[0], []).append(line[2])

    list1 = []
    for i, v in dict_data.items():
        c = 0
        for j in list(v):
            if j in (3, 5):
                c = min(max(c + 0.3, 0), 1)
            else:
                c = min(max(c - 0.3, 0), 1)
        a = ['1' if i in [3, 5] else ' ' for i in list(v)]
        b = max([len(x) for x in ''.join(a).split()])
        list1.append([i, b, c])
    hid_time = pd.DataFrame(list1, columns=['hid', 'chedan_times', 'chedan_pra'])
    return hid_time


##结巴提取关键词
def get_words(data):
    words = jieba.analyse.extract_tags(data, topK=3, withWeight=False, allowPOS=())
    return words


##原因字典
reason_weight = {
    '250': ['250', '251'],
    'QQ冻结（QQ暂时无法登陆）': ['冻结', 'QQ', '登录', '啊啊啊', '账号', '无法', '不了', 'qq', '退款', '登陆'],  #
    'TP检测16-2/36-2': ['小黑', '检测', '啊啊啊', '不了', '游戏', '排位', 'TP', '退款', 'tp', '登录'],  #
    'ZD主动防御': ['主动防御', 'ZD', 'md5'],
    '排队': ['排队'],
    '维护': ['维护', '游戏'],
    'steam客服已冻结该帐户': ['不了', '游戏', '账号', '退款', '封禁', '登录', '啊啊啊', '帐号', '封号', '无法'],  #
    '一直云检测': ['检测', '啊啊啊', '不了', '一直', '游戏', '小黑', '登录', '退款', '上去', '进不去'],  #
    '上号器自动投诉（qq冻结）': ['QQ', '冻结', '暂时'],  #
    '上号器自动投诉（账号密码错误）': ['密码', '错误'],  #
    '不想玩了或其它理由不玩了': ['不想', '啊啊啊', '不了', '退款', '游戏', '不玩', '排位', '谢谢', '不好意思', '段位'],  #
    '不输入账号密码': ['输入', '密码', '啊啊啊', '登录', '账号密码', '不了', '帐号密码', '一直', '退款', '游戏'],  #
    '会员时间到期': ['会员', '到期', '啊啊啊', '没有', '时间', '不了', '退款', '账号', '过期', '游戏'],  #
    '使用外挂 By 上号器': ['MD5', 'exe', 'dll', 'Users', '路径', '浏览器', 'rkr', '外挂', 'Administrator', '360'],  #
    '信誉积分不足': ['不足', '信誉', '积分', '排位', '不了', '不足', '不够', '匹配', '退款', '人机', '80'],  #
    '其他': ['游戏', '维护', '不了', '啊啊啊', '小黑', '退款', '更新', '登录', '退钱', '小时'],  #
    '号被封了': ['小黑', '封号', '封', '被封', '被封号', '账号', '游戏', '封号', '登录', '帐号'],  #
    '因财产密码': ['密码', '财产', '错误', '1234', '啊啊啊', '游戏', '输入', '不了', 'F9', '无法'],  #
    '复活币不足': ['复活', '没有', '啊啊啊', '一个', '怎么', '猎场', '不足', '游戏', '不了', '退款'],  #
    '安全问题错误': ['安全', '啊啊啊', '问题', '错误', '登录', '答案', '进不去', '验证码', '无法', '游戏'],  #
    '安装不了上号器': ['不了', '号器', '啊啊啊', '游戏', '安装', '退款', '登录', '电脑', '下载', '无法'],  #
    '客服仲裁错误': ['撤单', '租客', '密码', '客服', '游戏', '恶意', '账号', '投诉', '登录', '账号密码'],  #
    '提示有外挂残留': ['外挂', '残留', '小黑', '提示', '啊啊啊', '不了', '游戏', '排位', '退款', '登录'],  #
    '无法下载上号器': ['号器', '不了', '下载', '啊啊啊', '退款', '游戏', '无法', '登录', '电脑', '登不上'],
    '无法登陆（非密码错误问题）': ['登录', '游戏', '啊啊啊', '不了', '无法', '登陆', '退款', '上去', '登不上', '不上'],
    '游戏账号未实名认证': ['实名', '认证', '不了', '游戏', '验证', '啊啊啊', '防沉迷', '时间', '退款', '邮箱'],
    '租客违规操作': ['租客', '挂机', '排位', '信誉', '扣分', '恶意', 'QQ', '客服', '账号', '游戏'],
    '租方开外挂': ['租客', '挂机', '恶意', '撤单', '信誉', '账号', '游戏', '密码', '排位', '扣分'],
    '租方打排位': ['排位', '租客', '挂机', '租方', '信誉', '恶意', '排位赛', '扣分', '撤单', '违规'],
    '租错号了': ['错号', '租错', '啊啊啊', '不好意思', '排位', '退款', '组错', '谢谢', '段位', '对不起'],
    '自己的号要撤销': ['测试', '租客', '下架', '排位', '密码', '账号', '挂机', '冻结', '自己', '撤单'],
    '自己要玩': ['自己', '不好意思', '下架', '租客', '玩要', '抱歉', '号主', '撤单', '我要', '谢谢'],
    '被挤号（顶号）了': ['挤号', '顶号', '被顶', '挤', '游戏', '有人', '退款', '啊啊啊', '一直', '退钱', '不了', '登录', '账号'],
    '裁决之廉': ['排位', '啊啊啊', '裁决', '不了', '信誉', '竞技', '冷却', '游戏', '退款', '匹配'],
    '账号密码错误': ['密码', '错误', '账号密码', '登录', '啊啊啊', '退款', '登不上', '上去', '帐号密码', '不了'],
    '账号描述/段位不符': ['段位', '排位', '描述', '账号', '没有', '不符', '符合'],
    '账号禁赛': ['禁赛', '账号', '啊啊啊', '不了', '封号', '10', '游戏', '下线', '时间', '健康'],
    '账号被封': ['挂机', '租客', '信誉', '账号', '扣分', '恶意', '积分', '导致', '游戏', '禁赛'],
    '通过篡改上号器文件恶意破解,错误代码：1008 By 上号器': ['错误代码', '号器', '1008', '篡改', '破解', '恶意', '文件', '通过'],
    '健康时间': ['健康', '时间', '系统', '强制', '休息'],
    '设备锁': ['设备锁', '锁', '验证码', '验证'],
    '人脸识别': ['人脸识别', '人脸', '人脸验证', '要人脸', '验证', '识别', '刷脸'],
    '无法登录（wegame错误码：282）': ['wegame', '啊啊啊', '登陆', '不上', '不了', '登录', '上不去'],
    '手动协助失败（系统自动发起投诉）': ['未及时协助', '无法协助'],
    '上号器自动投诉 (人脸识别)': ['协助解除人脸', '上号失败', '无法协助', '失败'],
    '账号准备失败': ['投诉', '人脸', '登陆', '不上'],
    'QQ/微信冻结（QQ/微信暂时无法登陆）': ['登不上', '玩不了', '冻结了', '被冻结了', '进不去', '上不去'],
    '客服发起-无法登录': ['无法正常游戏', '人脸', '上不去'],
    '上号器自动投诉（qq冻结）': ['QQ暂时冻结', '身份验证', '账号冻结', '授权码无效', '授权码不正确', '还原码无效', '无法登录']
}


def reason_type(reason_weight):
    A = []
    B = 1
    for i in reason_weight.keys():
        A.append([B, i])
        B = B + 1
    df = pd.DataFrame(A, columns=['reason_type_id', 'reason_type_name'])
    ##df.to_sql(name='reason_type', con=cnx, if_exists = 'append', index=False)
    return df


# reason_type =
##句子分词，返回向量
def get_word_vector(s1, s2):
    """
    :param s1: 句子1
    :param s2: 句子2
    :return: 返回句子的余弦相似度
    """
    # 分词
    s1 = str(s1)
    s2 = str(s2)
    cut1 = jieba.cut(s1)
    cut2 = jieba.cut(s2)
    list_word1 = (','.join(cut1)).split(',')
    list_word2 = (','.join(cut2)).split(',')

    # 列出所有的词,取并集
    key_word = list(set(list_word1 + list_word2))
    # 给定形状和类型的用0填充的矩阵存储向量
    word_vector1 = np.zeros(len(key_word))
    word_vector2 = np.zeros(len(key_word))

    # 计算词频
    # 依次确定向量的每个位置的值
    for i in range(len(key_word)):
        # 遍历key_word中每个词在句子中的出现次数
        for j in range(len(list_word1)):
            if key_word[i] == list_word1[j]:
                word_vector1[i] += 1
        for k in range(len(list_word2)):
            if key_word[i] == list_word2[k]:
                word_vector2[i] += 1

    # 输出向量
    # print(word_vector1)
    # print(word_vector2)
    return word_vector1, word_vector2


###余弦相似度计算
def cos_dist(vec1, vec2):
    """
    :param vec1: 向量1
    :param vec2: 向量2
    :return: 返回两个向量的余弦相似度
    """
    dist1 = float(np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2)))
    return dist1


###撤单原因划分
def jud_reason_yx(word2):
    word2 = str(word2)
    result = pd.DataFrame()
    for i in reason_weight.keys():
        word1 = reason_weight[i]
        vec1, vec2 = get_word_vector(word1, word2)
        sorce = cos_dist(vec1, vec2)
        if sorce >= 0:
            data = pd.DataFrame({'id_name': [i],
                                 'score': [sorce]})
            result = pd.concat([data, result], axis=0)
    reason = result[result['score'] == result['score'].max()]
    reason_name = str(reason.id_name[0])
    # reason_score = str(reason.score[0])
    return reason_name


##未分类的原因处理
def jug_data(data):
    if len(data) > 40:
        data = '原因待跟踪'
    else:
        data = data
    return data


def data_merage(data1, data2):
    """

    Args:
        data1: Now_Cd_Data
        data2:  chedan_reason_2

    Returns:

    """
    chedan_reason_2 = pd.merge(data1, data2, how='inner', left_on='货架号', right_on='hid')

    re = chedan_reason_2['re'].groupby(chedan_reason_2.hid).aggregate(lambda x: ','.join(x))
    lx = chedan_reason_2['lx'].groupby(chedan_reason_2.hid).aggregate(lambda x: ','.join(x))

    hid = pd.DataFrame(re.index)
    re_list = pd.DataFrame(re.values)
    lx_list = pd.DataFrame(lx.values)

    list_data = pd.concat([hid, lx_list, re_list], axis=1)
    list_data.columns = ['hid', 'lx_list', 're_list']

    chedan_reason_2.drop_duplicates(subset=['hid'], keep='first', inplace=True)

    chedan_reason_2 = pd.merge(chedan_reason_2, list_data, how='inner', left_on='货架号', right_on='hid')

    chedan_reason_2['re_keywords'] = chedan_reason_2['re_list'].apply(lambda x: get_words(x))
    chedan_reason_2['lx_keywords'] = chedan_reason_2['lx_list'].apply(lambda x: get_words(x))

    chedan_reason_2['lx_reasons'] = chedan_reason_2['lx_list'].apply(lambda x: jud_reason_yx(x))
    chedan_reason_2['re_reasons'] = chedan_reason_2['re_keywords'].apply(lambda x: jud_reason_yx(x))

    chedan_reason_2['lx_reasons'] = chedan_reason_2['lx_reasons'].apply(lambda x: jug_data(x))
    chedan_reason_2['re_reasons'] = chedan_reason_2['re_reasons'].apply(lambda x: jug_data(x))

    return chedan_reason_2


# 同一用户多条投诉（恶意用户多条投诉）客服处理的投诉订单
ey_people_sql = '''
    SELECT hid,t1.userid,t1.huserid,count(*) as cnt from kudu.zhwdb.zhw_ts t1
    inner join kudu.zhwdb.zhw_dingdan t2
    on t1.did = t2.id
    where DATE_FORMAT(add_time,'%Y%m%d%H') BETWEEN '{0}' and '{1}'
    and DATE_FORMAT(t,'%Y%m%d%H') BETWEEN '{0}' and '{1}'
    and t1.zt = 2
    AND t2.zt=3
    and t2.hid <> 38
    and t1.userid <> t1.huserid 
    GROUP BY 1,2,3
    HAVING count(*) >= 2
'''.format(day_now_H_Last_13, day_now_H_Last)


def report_log():
    report = '''
        SELECT t1.`catch_time`,t1.`game_name`,t1.`main_reason` from 
        (
            SELECT `catch_time`,`game_name`,`main_reason`,count(*) as now_cnt 
            from high_risk_Hid_hour
            where `catch_time` = {}
            GROUP BY 1,2,3
        )t1
        left join
        (
            SELECT `catch_time`,`game_name`,`main_reason`,count(*) as last_cnt 
            from high_risk_Hid_hour
            where `catch_time` = {}
            GROUP BY 1,2,3
        )t2
        on t1.`game_name`=t2.`game_name` and t1.`main_reason` = t2.`main_reason`
        where now_cnt >10
        and now_cnt / last_cnt >= 2
    '''.format(day_now_H_Last, day_now_H)
    report = pd.read_sql(report, con=cnx)
    print("report", report)
    report.to_sql(name='high_risk_Hid_hour_log', con=cnx, if_exists='append', index=False)
    print('预警日志已经写入')


now_H = (datetime.datetime.now()).strftime('%H')  # h


# 钉钉邮件提醒

def send_email(A):
    if now_H == '04' or now_H == '12' or now_H == '19' or len(A) > 600:
        yag = yagmail.SMTP(user='1154769147@qq.com', password='jplzsikpvomxjbie', host='smtp.qq.com')
        subject = '{}点高危撤单用户通报(共计{}条)'.format(day_now_H, len(A))
        user = ['zsc4538@163.com']
        # user = ['j3n_h3lxlpb1h@dingtalk.com', 'lizhengyuan7703@dingtalk.com', 'sjjd10756@dingtalk.com','zhengpengwei1508@dingtalk.com']
        yag.send(to=user, subject=subject, contents='当前时间段高危撤单货架提醒，请及时处理',
                 attachments=["D:/{}点高危货架.csv".format(day_now_H)])
    print('钉邮推送判断完毕')


def recall_cate_by_lol_hidtohid(result, host, port, dict, pas):
    host = host
    port = port
    cnt = 0
    # 建立redis 连接池
    pool = redis.ConnectionPool(host=host, port=port, db=5, password=pas)
    # 建立redis客户端
    client = redis.Redis(connection_pool=pool)
    pipe = client.pipeline()
    for i in range(len(result)):
        print(i)
        pipe.hset(dict, str(result[i]), str(result.推荐货架.values[i]))
        cnt += 1
        if cnt % 50000 == 0:
            pipe.execute()
    pipe.execute()


target_dict = "high_risk_hid"


# recall_cate_by_lol_hidtohid(result,redis_host,redis_port,target_dict,redis_password)


def main(sql0, sql1, sql2, sql3):
    """
    Args:
        sql0: Cd_H_sql    ##12小时的货架撤单跟踪分析 返回 Now_Cd_Data
        sql1: ey_people_sql #同一用户多条投诉（恶意用户多条投诉）客服处理的投诉订单
        sql2: reason_sql #高危货架撤单原因分析sql(所有的撤单包括主动撤单）
        sql3: time_count_sql #货架连续异常次数计算sql

    Returns:
    """
    ey_people = pd.read_sql(sql1, con=presto_db)  ##同一用户多条投诉（恶意用户多条投诉）客服处理的投诉订单
    print('ey_people', ey_people)
    ey_people.columns = ['投诉货架', '投诉用户', '号主用户', '连续次数']

    ey_people_cnt = ey_people['投诉用户'].groupby(ey_people['投诉货架']).aggregate(lambda x: ','.join(x))
    hid = pd.DataFrame(ey_people_cnt.index)
    userid = pd.DataFrame(ey_people_cnt.values)
    data = pd.concat([hid, userid], axis=1)
    data.columns = ['投诉货架', '投诉用户']

    # 结果整合
    chedan_reason_2 = pd.read_sql(sql2, con=presto_db)  # 高危货架撤单原因分析sql(所有的撤单包括主动撤单）

    print(sql0)
    Now_Cd_Data = pd.read_sql(sql0, con=presto_db)
    print(sql0)
    Now_Cd_Data.columns = ['捕获时段', '货架号', '用户名', '游戏区服', '游戏账号', 'gameid', '是否限时货架', '撤单量（12小时）', '订单量（12小时）', '真实撤单比']
    print('Now_Cd_Data', Now_Cd_Data)
    chedan_reason_2 = data_merage(Now_Cd_Data, chedan_reason_2)
    print('chedan_reason_2', chedan_reason_2)



    chedan_reason_2 = pd.merge(chedan_reason_2, data, how='left', left_on='货架号', right_on='投诉货架')
    # chedan_reason_2['联系电话'] = chedan_reason_2['联系电话'].fillna(value='无')
    chedan_reason_2 = chedan_reason_2.fillna(value=0)
    print('chedan_reason_22', chedan_reason_2['lx_reasons'])

    main_reason_name = pd.read_sql('''SELECT * from reason_type''', con=cnx)
    main_reason_name.columns = ['reason_type_id', 'reason_type_name']
    chedan_reason_2 = pd.merge(chedan_reason_2, main_reason_name, how='left', left_on='lx_reasons',
                               right_on='reason_type_name')

    # chedan_reason_2['lx_reasons'] = chedan_reason_2.apply(lambda x:str('用户{}发起多次撤单').format(x['投诉用户']) if x.lx_reasons == '账号描述/段位不符' and x['投诉用户'] != 0 else x.lx_reasons,axis=1)
    chedan_reason_2['lx_reasons'] = chedan_reason_2.apply(
        lambda x: str('用户发起多次撤单') if x.lx_reasons == '账号描述/段位不符' and x['投诉用户'] != 0 else x.lx_reasons, axis=1)
    chedan_reason_2['lx_reasons'] = chedan_reason_2.apply(
        lambda x: str('描述不符可能原因:') + x.re_reasons if x.lx_reasons == '账号描述/段位不符' and x['投诉用户'] != 0 else x.lx_reasons,
        axis=1)

    hid_time = chedan_times(sql3)
    print('hid_time', hid_time)
    chedan_reason_2 = pd.merge(chedan_reason_2, hid_time, how='inner', left_on='货架号', right_on='hid')
    print('chedan_reason_23:', chedan_reason_2)

    # 游戏名称
    game_name = pd.read_sql('''SELECT id as gameid,title as game_name from kudu.zhwdb.zhw_game_info''', con=presto_db)
    game_name.columns = ['gameid', 'game_name']
    chedan_reason_2 = pd.merge(chedan_reason_2, game_name, how='left', left_on='gameid', right_on='gameid')

    # sql4 = '''select hid,max(chedan_times) as chixu_zq from high_risk_hid_hour where catch_time between {0} and {1} group by hid'''.format(
    #     day_now_H_Last_3, day_now_H_Last_2)
    # chixu_times = pd.read_sql(sql4, con=cnx)
    # chixu_times['chixu_zq'] = chixu_times['chixu_zq'].map(lambda x: x + 1)
    # chedan_reason_2 = pd.merge(chedan_reason_2, chixu_times, how='left',  left_on='货架号', right_on='hid')
    # chedan_reason_2['chixu_zq'] = chedan_reason_2['chixu_zq'].fillna(value=1)

    def get_chedan_times(data):
        if (int(data.chixu_zq) == 0) & (data['撤单量（12小时）'] >= 4):
            times = 8
        elif ((int(data.chixu_zq) != 0) & (data['撤单量（12小时）'] >= 8)):
            times = int(data.chixu_zq) + int(data['撤单量（12小时）'])
        else:
            times = int(data.chixu_zq) + 1
        return times

    def get_chedan_hid(hid, data, bit):
        '''长期撤单高危货架，给与次数加成'''
        if data['货架号'] in hid.values:
            times = data.chixu_zq * bit
        else:
            times = data.chixu_zq
        return times

    sql1 = '''select hid 
        from 
        (
            select hid,t4.title,t4.categoryid,t3.userid,t3.pn,t3.ft,
            count(distinct t1.did) as "描述不符投诉次数",count(t5.userid) as "恶意用户投诉占比" 
            from 
            (
                select userid,did,re 
                from kudu.zhwdb.zhw_ts
                where lx like '%不符%'
                and part_day >= '{0}'
            ) t1
            inner join
            (
                select * 
                from kudu.zhwdb.zhw_dingdan
                where part_day >= '{0}'
                and gameid in (select id from kudu.zhwdb.zhw_game_info															
                where categoryid in (1,2,3)
                )
            )t2
            on t1.did = t2.id
            inner join
            (
                select * from kudu.zhwdb.zhw_hao
                where zt in (0,1)
            ) t3
            on t2.hid = t3.id
            inner join kudu.zhwdb.zhw_game_info t4
            on t3.gid = t4.id
            left join
            (select userid,count(distinct did) from kudu.zhwdb.zhw_ts
            where lx like '%不符%'
            and part_day >= '{0}'
            group by 1
            having count(distinct did) >= 5)t5
            on t1.userid = t5.userid
            group by 1,2,3,4,5,6
            having count(distinct t1.did) >= 10 and count(t5.userid) <= 20
            order by 7 desc
        ) a'''.format(day_last_30)
    cq_hid = pd.read_sql(sql1, con=presto_db)

    sql2 = '''
        select hid 
        from 
        (
            select hid,t4.title,t4.categoryid,t3.userid,t3.pn,t3.ft,count(distinct t1.did) as "设备锁投诉次数",count(t5.userid) as "恶意用户投诉占比" 
            from 
            (
                select userid,did,re 
                from kudu.zhwdb.zhw_ts
                where (re like '%验证%' or re like '%设备锁%')
                and part_day >= '{0}'
            )t1
            inner join
            (
                select * 
                from kudu.zhwdb.zhw_dingdan
                where part_day >= '{0}'
                and gameid in (select id from kudu.zhwdb.zhw_game_info															
                where categoryid in (1,2,3))
            )t2
            on t1.did = t2.id
            inner join
            (
                select * from kudu.zhwdb.zhw_hao
                where zt in (0,1)
            )t3
            on t2.hid = t3.id
            inner join kudu.zhwdb.zhw_game_info t4
            on t3.gid = t4.id
            left join
            (
                select userid,count(distinct did) 
                from kudu.zhwdb.zhw_ts
                where (re like '%验证%' or re like '%设备锁%')
                and part_day >= '{0}'
                group by 1
                having count(distinct did) >= 3
            )t5
            on t1.userid = t5.userid
            group by 1,2,3,4,5,6
            having count(distinct t1.did) >= 3 and count(t5.userid) <= 10
            order by 7 desc
    )'''.format(day_last_db)
    yz_hid = pd.read_sql(sql2, con=presto_db)

    sql4 = '''select  hid,chedan_times as chixu_zq from high_risk_hid_hour where catch_time = {0} group by hid'''.format(
        day_now_H_Last_2)
    chixu_times = pd.read_sql(sql4, con=cnx)

    chedan_reason_2 = pd.merge(chedan_reason_2, chixu_times, how='left', left_on='货架号', right_on='hid')
    chedan_reason_2['chixu_zq'] = chedan_reason_2['chixu_zq'].fillna(value=0)
    chedan_reason_2['chixu_zq'] = chedan_reason_2.apply(lambda x: get_chedan_times(x), axis=1)
    chedan_reason_2['chixu_zq'] = chedan_reason_2.apply(lambda x: get_chedan_hid(cq_hid, x, 4), axis=1)
    chedan_reason_2['chixu_zq'] = chedan_reason_2.apply(lambda x: get_chedan_hid(yz_hid, x, 4), axis=1)
    # chedan_reason_2['持续异常次数'] = chedan_reason_2['chedan_times']
    # chedan_reason_2['异常概率'] = chedan_reason_2['chedan_pra']

    # chedan_reason_2['持续异常周期（小时）'] = chedan_reason_2.apply(lambda x:int(x['持续异常周期'])+1 if x['持续异常周期'] >= 0 else 0,axis=1)
    print(chedan_reason_2.head(5))

    chedan_reason_2.drop_duplicates(subset=['货架号'], keep='first', inplace=True)
    print(chedan_reason_2)

    A = chedan_reason_2[
        ['捕获时段', '货架号', '游戏账号', 'gameid', 'game_name', '游戏区服', '用户名', '是否限时货架', '订单量（12小时）', '撤单量（12小时）', '真实撤单比',
         'reason_type_id', 'lx_reasons', 'chixu_zq', 'chedan_pra']]
    print("统计周期：", day_now_H_Last)
    print('数据提取完毕')
    A.columns = ['catch_time', 'hid', 'game_account', 'gameid', 'game_name', 'yxq', 'owner_name', 'limit_H_flag',
                 'dingdan_cnt', 'chedan_cnt', 'chedan_rate', 'main_reason_id', 'main_reason', 'chedan_times',
                 'chedan_pra']
    # A.to_csv("/usr/model/zhw_product/anay/{}点高危货架.csv".format(day_now_H), index=False, encoding='utf_8_sig')
    # try:
    #     A.to_csv("D:/{}点高危货架.csv".format(day_now_H), index=False, encoding='utf_8_sig')
    #     print('csv文件保存完毕')
    # except :
    #     pass
    pd.set_option('max_colwidth', 100)
    pd.set_option('display.max_columns', None)
    # print(A)

    A.to_sql(name='high_risk_hid_hour', con=cnx, if_exists='append', index=False)
    print('SQL写入完毕')
    # report_log()
    # send_email(A)


##已经封禁的异常周期归0
def kill_data_update():
    kill_list_sql = """select hid from kudu.zhwdb.zhw_hao_kill 
    where DATE_FORMAT(create_time,'%Y%m%d%H') between '{0}' and '{1}'  and status in (1,2) group by 1""".format(
        day_now_H_Last_13, day_now_H_Last)
    print(kill_list_sql)
    kill_list_df = pd.read_sql(kill_list_sql, con=presto_db)
    kill_list_nd = np.array(kill_list_df)

    conn = pymysql.connect(host=host, port=int(port), database=DB, user=user,
                           password=password, charset='utf8')
    # 获取cursor对象
    cs1 = conn.cursor()
    # 执行sql语句
    for i in kill_list_nd:
        query = "update datawj.high_risk_hid_hour set chedan_times = 0 where id = {0} ".format(int(i))
        print(query)
        cs1.execute(query)

    # 提交之前的操作，如果之前已经执行多次的execute，那么就都进行提交
    conn.commit()

    # 关闭cursor对象
    cs1.close()
    # 关闭connection对象
    conn.close()


# 和平精英非云高撤单货架下架6小时
def high_risk_hid_hp():
    sql = """
    select date_format(now(),'%Y%m%d%H') catch_time,
           t1.hid,
           zh game_account,
           683 gameid,
           '和平精英' game_name,
           yxqu yxq,
           huserid owner_name,
           case when hao_id is not null then 1 else 0 end limit_h_flag,
           dingdan_cnt,
           chedan_cnt,
           chedan_rate,
           48 main_reason_id,
           '和平精英非云高撤单货架' main_reason,
           6 chedan_times,
           1 chedan_pra
    from
    (
    select hid,huserid,
           count(id) dingdan_cnt,
           count(case when zt = 3 then id end) chedan_cnt,
           count(case when zt = 3 then id end)*1.00/count(id) chedan_rate
    from kudu.zhwdb.zhw_dingdan a
    inner join (select order_id from kudu.zhwdb.zhw_quick_vpn_log where part_day >= date_format(current_date-interval '3' day,'%Y-%m-%d') and vpn_type = 'north' group by 1) b
    on a.id = b.order_id
    where part_day >= date_format(current_date-interval '3' day,'%Y-%m-%d') and gameid = 683
    and date_format(add_time,'%Y-%m-%d %H') >= date_format(now()-interval '48' hour,'%Y-%m-%d %H')
    group by 1,2
    having count(case when zt = 3 then id end) >= 5 and count(case when zt = 3 then id end)*1.00/count(id) >= 0.6
    ) t1
    left join kudu.zhwdb.zhw_hao t2 on t1.hid = t2.id
    left join (select hao_id from kudu.zhwlog.zhw_sh_service where service_type = 3 and start_time <= now() and end_time >= now() group by 1) t3
    on t1.hid = t3.hao_id
    left join (select hid from kudu.zhwdb.zhw_quick_hao_cloud where status = 1) t4
    on t1.hid = t4.hid
    where t4.hid is null
    """
    data = pd.read_sql(sql, con=presto_db)
    data.to_sql(name='high_risk_hid_hour', con=cnx, if_exists='append', index=False)


def high_risk_hid_formal():
    sql = '''select catch_time,hid,gameid ,game_name ,owner_name,limit_h_flag ,dingdan_cnt ,chedan_cnt,chedan_rate,main_reason_id,
    reason_type_name as main_reason,chedan_times,kill_times,chedan_pra 
    from 
    (
        select 
            max(catch_time) as catch_time,hid,gameid,game_name,owner_name,max(limit_h_flag) as limit_h_flag,
            sum(dingdan_cnt) as dingdan_cnt,
            sum(chedan_cnt) as chedan_cnt,sum(chedan_cnt)*1.000/sum(dingdan_cnt) as chedan_rate,
            max(main_reason_id) as main_reason_id,max(chedan_times) as chedan_times,
            count(*) as kill_times ,max(chedan_pra) as chedan_pra 
        from datawj.high_risk_hid_hour 
        where catch_time between '{0}' and '{1}' 
        group by 2,3,4
    ) a
    left join datawj.reason_type b
    on a.main_reason_id=b.reason_type_id
    '''.format(day_now_H_Last_13, day_now_H_Last)

    report = pd.read_sql(sql, con=cnx)
    print(sql)
    print("report", report)
    report.columns = ['catch_time', 'hid', 'gameid', 'game_name', 'owner_name', 'limit_h_flag', 'dingdan_cnt',
                      'chedan_cnt', 'chedan_rate', 'main_reason_id', 'main_reason', 'chedan_times', 'kill_times',
                      'chedan_pra']
    report.to_sql(name='high_risk_hid_list_hour', con=cnx, if_exists='append', index=False)
    return print('预警日志已经写入')


if __name__ == "__main__":
    kill_data_update()
    main(Cd_H_sql, ey_people_sql, reason_sql, time_count_sql)
    high_risk_hid_formal()
    high_risk_hid_hp()
    # PHP检测Redis缓存，建立redis 连接池
    pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=5, password=redis_password)
    # 建立redis客户端
    client = redis.Redis(connection_pool=pool)
    client.lpush("high_risk_hid", day_now_H)
    client.lpush("high_risk_hid_list", day_now_H)
    print('写入完毕')