# -*- coding: UTF-8 -*-
import fun_model
import constant
import pandas as pd
import os
from datetime import datetime
from datetime import timedelta
import imei_modification
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor


def main():
    file_list_15 = fun_model.get_folder_file(constant.FOLDER_15)
    file_list_15 = file_list_15[0: 40]
    for file_path in file_list_15:
        # imei_modification.deal_imei(file_path['file_path'], file_path['e_time'], 1)  # imei变化处理(现)
        # imei_modification.deal_imei(file_path['file_path'], file_path['e_time'], 2)  # imei变化处理(专)
        # 15分钟文件
        df = pd.read_csv(file_path['file_path'], header=None, error_bad_lines=False,
                         dtype={0: 'object', 1: 'object', 5: 'object', 9: 'object', 12: 'object'})
        error_df, df = fun_model.filter_df(df, 1)
        if not error_df.empty:
            fun_model.save_error_csv(constant.FOLDER_ERROR, error_df, file_path['s_time'])  # 存取不合格的行
        request_sum_df_15 = sig_rate(df)

        xian_zhuan_request(file_path, request_sum_df_15, 'wl_c_pdp_make_i15', 'wl_c_success_rate_make_i15', 1)  # 现网处理
        xian_zhuan_request(file_path, request_sum_df_15, 'wl_c_pdp_make_i15', 'wl_c_success_rate_make_i15', 2)  # 专网

        chk_f_path = file_path['file_path'][0:-4] + '.chk'  # chk文件

        fun_model.move_file(constant.FOLDER_1H, file_path['file_path'], chk_f_path)  # 移动文件
        print (file_path['file_path']+'is ok 15 minute')

        if file_path['e_time'].minute == 00:
            # 判断是否整点，然后操作1h文件夹
            file_list_1h = fun_model.get_folder_file_1h(constant.FOLDER_1H)
            for f_list in file_list_1h:
                df_con = read_concat(f_list['file_path'])
                request_sum_df_1h = sig_rate(df_con)
                xian_zhuan_request(f_list, request_sum_df_1h, 'wl_c_pdp_make_h1', 'wl_c_success_rate_make_h1', 1)
                xian_zhuan_request(f_list, request_sum_df_1h, 'wl_c_pdp_make_h1', 'wl_c_success_rate_make_h1', 2)
                fun_model.move_file(constant.FOLDER_1D, f_list['file_path'])  # 移动文件
                print (f_list['e_time'], '1 hour')

                if f_list['e_time'].hour == 00 & f_list['e_time'].minute == 00 & f_list['e_time'].second == 00:
                    # 判断是否是00：00:00，处理一天文件
                    file_list_1d = fun_model.get_folder_file_1d(constant.FOLDER_1D)
                    for f_list_1d in file_list_1d:
                        df_con_1d = read_concat(f_list_1d['file_path'])
                        request_sum_df_1d = sig_rate(df_con_1d)
                        xian_zhuan_request(f_list_1d, request_sum_df_1d, 'wl_c_pdp_make_d1', 'wl_c_success_rate_make_d1', 1)
                        xian_zhuan_request(f_list_1d, request_sum_df_1d, 'wl_c_pdp_make_d1', 'wl_c_success_rate_make_d1', 2)
                        fun_model.move_file(constant.FOLDER_OK, f_list_1d['file_path'])  # 移动文件
                        print (f_list['e_time'], '1 day')
    # timing()  # 开启下一个任务


def sig_rate(df):
    # 请求次数
    count_df = fun_model.df_column_build_apply(df, [5, 9, 12, 21],
                                               ['apn', 'gtp_cause', 'region_code', 'request_count'],
                                               ['apn', 'gtp_cause', 'region_code'], ['request_count'])
    sum_df = fun_model.df_apply(count_df, ['apn', 'region_code'], 'request_count')  # 总的请求次数
    sum_df.rename(columns={'request_count': 'sum_count'}, inplace=True)
    request_sum_df = pd.merge(count_df, sum_df, how='left', on=['apn', 'region_code'])  # how='left', on='apn'
    return request_sum_df


def xian_zhuan_request(file_dic, request_sum_df, db_table_make, db_table_success, type):
    # 请求次数（所有的结果码）的比率（apn，gtp_cause，region_code）
    if type == 1:
        request_df = request_sum_df[request_sum_df['apn'].str.contains(r'^cmiot|^cmmtm', na=False)==False]  # 获取现网数据
    else:
        request_df = request_sum_df[request_sum_df['apn'].str.contains(r'^cmiot|^cmmtm', na=False)]  # 获取专网数据

    request_df['request_rate'] = request_df['request_count'] / request_df['sum_count']
    request_df['time_end'] = file_dic['e_time']
    request_df['time_start'] = file_dic['s_time']
    all_result_code = fun_model.region_id_code(request_df)

    any_two_df = any_two_rate(all_result_code, file_dic['e_time'], file_dic['s_time'])  # todo

    # 请求次数按照(apn, gtp_cause)分组，统计比率
    # gtp_df = fun_model.df_apply(request_df, ['apn', 'gtp_cause'], 'request_count')

    # 请求次数（成功的结果码）的比率（apn，gtp_cause，region_code）
    if type == 1:
        success_code_df = request_sum_df[request_sum_df.gtp_cause.isin(constant.XIAN_SUCCESS_CAUSE)]
    else:
        success_code_df = request_sum_df[request_sum_df.gtp_cause.isin(constant.ZHUAN_SUCCESS_CAUSE)]
    success_group_df = fun_model.df_apply(success_code_df, ['apn', 'region_code'], 'request_count')
    xian_df = all_result_code.drop(['request_count', 'request_rate', 'gtp_cause'], axis=1)
    uniq_xian_df = xian_df.drop_duplicates(['apn', 'region_code'], keep='last')  # 去重合并 注意右边是否重复数据
    suc_df = pd.merge(success_group_df, uniq_xian_df, how='left', on=['apn', 'region_code'])
    suc_df['request_rate'] = suc_df['request_count'] / suc_df['sum_count']
    # todo
    if type == 1:
        suc_df['gtp_cause'] = 128
    else:
        suc_df['gtp_cause'] = 0
    any_two_suc_df = any_two_rate(suc_df, file_dic['e_time'], file_dic['s_time'])
    all_df = pd.concat([all_result_code, any_two_df], axis=0, ignore_index=True)
    success_df = pd.concat([suc_df, any_two_suc_df], axis=0, ignore_index=True)

    if type == 1:
        fun_model.save_data_db(all_df, db_table_make, constant.DATABASE)  # 现网数据库
        fun_model.save_data_db(success_df, db_table_success, constant.DATABASE)
    else:
        fun_model.save_data_db(all_df, db_table_make, constant.ZHUAN_DATABASE)  # 专网数据库
        fun_model.save_data_db(success_df, db_table_success, constant.ZHUAN_DATABASE)


def any_two_rate(rate_df, e_time, s_time):
    # todo apn, gtp_cause
    apn_gtp = fun_model.df_two_agg(rate_df, ['apn', 'gtp_cause'], 'request_count', 'sum_count')
    apn_gtp['request_rate'] = apn_gtp['request_count'] / apn_gtp['sum_count']
    apn_gtp['time_end'] = e_time
    apn_gtp['time_start'] = s_time
    apn_gtp['region_id'] = 1
    apn_gtp['region_code'] = '0000'
    # region gtp_cause
    region_gtp = fun_model.df_two_agg(rate_df, ['region_code', 'gtp_cause'], 'request_count', 'sum_count')
    region_gtp['request_rate'] = region_gtp['request_count'] / region_gtp['sum_count']
    region_gtp['time_end'] = e_time
    region_gtp['time_start'] = s_time
    region_gtp['apn'] = 'all'
    region_gtp_df = fun_model.region_id_code(region_gtp)
    # gtp_cause
    gtp = fun_model.df_two_agg(rate_df, 'gtp_cause', 'request_count', 'sum_count')
    gtp['request_rate'] = gtp['request_count'] / gtp['sum_count']
    gtp['time_end'] = e_time
    gtp['time_start'] = s_time
    gtp['apn'] = 'all'
    gtp['region_id'] = 1
    gtp['region_code'] = '0000'

    con_df = pd.concat([apn_gtp, region_gtp_df, gtp], axis=0, ignore_index=True)
    return con_df


def read_concat(f_list):
    df_list = []
    for f in f_list:
        try:
            df = pd.read_csv(f, header=None, error_bad_lines=False,
                             dtype={0: 'object', 1: 'object', 5: 'object', 9: 'object', 12: 'object'})
            right_df = fun_model.filter_df(df, 2)
            df_list.append(right_df)
        except Exception as e:
            print (e)
    df_con = pd.concat(df_list, ignore_index=True)
    return df_con


def timing():
    abspath = os.path.abspath('.')
    dir_path = os.path.join(abspath, constant.SQLITE_DB)
    url = r'sqlite:///%s' % dir_path
    job_stores = {
        'default': SQLAlchemyJobStore(url=url)
    }
    executors = {  # 执行器
        'default': ThreadPoolExecutor(20),  # 线程池的最大线程数为20
        'processpool': ProcessPoolExecutor(5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3  # 设置允许并发执行任务的个数。
    }
    # 调度器
    scheduler = BlockingScheduler(jobstores=job_stores, executors=executors, job_defaults=job_defaults)
    # scheduler.add_job(tick, 'interval', seconds=5, id='sig_timing', replace_existing=True)  # 间隔3S执行
    scheduler.add_job(main, next_run_time=datetime.now() + timedelta(seconds=10), id='sig_timing',
                      replace_existing=True)  # 下一次5分钟执行
    # scheduler.start()  # 这里的调度任务是独立的一个线程
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()    # 采用的是阻塞的方式，只有一个线程专职做调度的任务
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The Job!')


if __name__ == '__main__':
    main()

