# _*_coding:utf-8 _*_
# @Time     :2023/11/9 16:27
# @Author   : anliu
# @File     :tmp_analaysis_schedul.py
# @Theme    :PyCharm
import sys

import schedule
import time, datetime, functools

from tmp_day import tmp_cb_multi_dims_retention_1di
from tmp_day import tmp_refund_multi_dims_retention_1di
from tmp_day import tmp_dws_iccmm_acq_1di
from tmp_day import tmp_dws_iccmm_refund_1di
from tmp_day import tmp_dws_iccmm_chargeback_1di


last_365d = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
last_180d = (datetime.datetime.now() - datetime.timedelta(days=180)).strftime('%Y-%m-%d')
last_0d = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')
last_7d = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

def timer_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"函数 {func.__name__} 运行时长：{end_time - start_time} 秒")
        return result
    return wrapper

@timer_decorator
def job_tmp_cb_multi_dims_retention_1di():
    '''
    :return:支付成功后第R日发起拒付，一般R最大180、特殊达到365以内
    '''
    start_end_day_list = [
        (last_365d, last_0d)
    ]
    for idx, start_end_tuple in enumerate(start_end_day_list):

        start_day, end_day = start_end_tuple[0], start_end_tuple[1]
        tmp = tmp_cb_multi_dims_retention_1di.TmpCbMultiDimsRetention1di()
        tmp.run(start_day, end_day)

        time.sleep(1)

@timer_decorator
def job_tmp_refund_multi_dims_retention_1di():
    '''
    :return:支付成功后第R日发起退款，一般R最大180
    '''
    start_end_day_list = [
        (last_180d, last_0d)
    ]
    for idx, start_end_tuple in enumerate(start_end_day_list):

        start_day, end_day = start_end_tuple[0], start_end_tuple[1]
        tmp = tmp_refund_multi_dims_retention_1di.TmpRefundMultiDimsRetention1di()
        tmp.run(start_day, end_day)

        time.sleep(1)


@timer_decorator
def job_tmp_refund_multi_dims_retention_1di():
    '''
    :return:支付成功后第R日发起退款，一般R最大180
    '''
    start_end_day_list = [
        (last_180d, last_0d)
    ]
    for idx, start_end_tuple in enumerate(start_end_day_list):

        start_day, end_day = start_end_tuple[0], start_end_tuple[1]
        tmp = tmp_refund_multi_dims_retention_1di.TmpRefundMultiDimsRetention1di()
        tmp.run(start_day, end_day)

        time.sleep(1)


@timer_decorator
def job_tmp_dws_iccmm_acq_1di():
    '''
    :return:收单支付宽表-交易日期
    '''
    start_end_day_list = [
        (last_7d, last_0d)
    ]
    for idx, start_end_tuple in enumerate(start_end_day_list):

        start_day, end_day = start_end_tuple[0], start_end_tuple[1]
        tmp = tmp_dws_iccmm_acq_1di.TmpDwsIccmmAcq1di()
        tmp.run(start_day, end_day)

        time.sleep(1)


@timer_decorator
def job_tmp_dws_iccmm_chargeback_1di():
    '''
    :return:收单拒付宽表-拒付日期
    '''
    start_end_day_list = [
        (last_7d, last_0d)
    ]
    for idx, start_end_tuple in enumerate(start_end_day_list):

        start_day, end_day = start_end_tuple[0], start_end_tuple[1]
        tmp = tmp_dws_iccmm_chargeback_1di.TmpDwsIccmmChargeback1di()
        tmp.run(start_day, end_day)

        time.sleep(1)


@timer_decorator
def job_tmp_dws_iccmm_refund_1di():
    '''
    :return:收单退款宽表-退款日期
    '''
    start_end_day_list = [
        (last_7d, last_0d)
    ]
    for idx, start_end_tuple in enumerate(start_end_day_list):

        start_day, end_day = start_end_tuple[0], start_end_tuple[1]
        tmp = tmp_dws_iccmm_refund_1di.TmpDwsIccmmRefund1di()
        tmp.run(start_day, end_day)

        time.sleep(1)

# 每天的10:30执行任务
schedule.every().day.at("10:39:00").do(job_tmp_cb_multi_dims_retention_1di)

schedule.every().day.at("10:39:00").do(job_tmp_refund_multi_dims_retention_1di)

schedule.every().day.at("10:39:00").do(job_tmp_dws_iccmm_acq_1di)
schedule.every().day.at("10:39:00").do(job_tmp_dws_iccmm_chargeback_1di)
schedule.every().day.at("10:39:00").do(job_tmp_dws_iccmm_refund_1di)

while True:
    schedule.run_pending()
    time.sleep(1)