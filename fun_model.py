# -*- coding: UTF-8 -*-
import constant
import os
import shutil
import datetime
import time
import logging
import re
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector


def time_me(fn):
    """计算耗时"""
    def _wrapper(*args, **kwargs):
        start = time.clock()
        fn(*args, **kwargs)
        # print "%s cost %s second" % (fn.__name__, time.clock() - start)
        return time.clock() - start
    return _wrapper


def region_id_code(df):
    """匹配region_id和region_code"""
    re_list = [[1, '0000'], [2, '028'], [3, '0816'], [4, '0813'], [5, '0812'], [6, '0830'], [12, '0838'], [13, '0839'], [14, '0825'], [15, '0832'],
               [16, '0833'], [17, '0901'], [18, '0831'], [19, '0817'], [20, '0818'], [21, '0835'], [22, '0837'], [23, '0836'], [24, '0834'],
               [25, '0826'], [26, '0827'], [27, '0902'], [28, '0281'], [9999, '-1']]

    re_df = pd.DataFrame(re_list, columns=['region_id', 'region_code'])
    result_df = pd.merge(df, re_df, how='left', on='region_code')
    return result_df


def get_folder_file(folder_path):
    """
    获取目录下文件   判断.chk文件
    :param folder_path 处理文件夹的相对路径
    :return file_list 返回处理csv文件列表
    """
    file_list = []
    handle_file_type = 'chk'
    abspath = os.path.abspath('.')  # 获取当前绝对路径
    dir_path = os.path.join(abspath, folder_path)  # data文件夹的路径  'data\lixin\data_test'
    dir_file = os.listdir(dir_path)  # data文件夹下的文件名称
    dir_file = list(set(dir_file))  # 去掉重复的文件
    dir_file.sort()  # 升序排列
    if not dir_file:  # 一个空 list 本身等同于 False
        print("15 minute not files can handle")
    else:
        for f_name in dir_file:
            file_type = f_name.split('.')[1]  # 文件类型 chk或csv
            # if f_name.endswith('.chk')  # 此函数判断一个文本是否以某个或几个字符结束，结果以True或者False返回
            if file_type == handle_file_type:  # 判断是否为'chk' 文件
                standard = f_name.split('.')[0]+'.csv'
                for f_file in dir_file:
                    if f_file == standard:  # 获取另外一个时间相同文件
                        f_path = os.path.join(dir_path, f_file)  # 绝对路径
                        start_time, end_time = get_time(f_file, 900, 1)
                        f_t_list = {'file_path': f_path, 's_time': start_time, 'e_time': end_time, 'file_name': f_file}
                        file_list.append(f_t_list)  # file_list包含找到的csv全路径、时间、csv文件名字

            else:
                continue
    return file_list


def get_folder_file_1h(folder_path):
    """处理小时文件夹"""
    file_list = []
    handle_file_type = '45.csv'  # 判断45.csv
    abspath = os.path.abspath('.')  # 获取当前绝对路径
    dir_path = os.path.join(abspath, folder_path)  # data文件夹的路径  'data\lixin\data_test'
    dir_file = os.listdir(dir_path)  # data文件夹下的文件名称
    dir_file_set = set(dir_file)
    if not dir_file:  # 一个空 list 本身等同于 False
        print("1 hour not files can handle")
    else:
        for f_name in dir_file:
            file_list_1h = []
            if f_name.endswith(handle_file_type):
                file_list_1h.append(f_name.replace(handle_file_type, '00.csv'))
                file_list_1h.append(f_name.replace(handle_file_type, '15.csv'))
                file_list_1h.append(f_name.replace(handle_file_type, '30.csv'))
                file_list_1h.append(f_name)
                file_set_1h = set(file_list_1h)
                if dir_file_set.issuperset(file_set_1h):  # dir_file_set是否包含file_set_1h中4个文件
                    f_path_list = []
                    start_time, end_time = get_time(file_list_1h[0], 3600, 2)  # 时间
                    for file_1h in file_list_1h:
                        f_path = os.path.join(dir_path, file_1h)  # 合成绝对路径
                        f_path_list.append(f_path)
                    f_path_dic = {'file_path': f_path_list, 'e_time': end_time, 's_time': start_time}
                    file_list.append(f_path_dic)
    return file_list


def get_folder_file_1d(folder_path):
    """处理一天的文件"""
    file_list = []
    handle_file_type = '2345.csv'  # 判断2345.csv
    abspath = os.path.abspath('.')  # 获取当前绝对路径
    dir_path = os.path.join(abspath, folder_path)
    dir_file = os.listdir(dir_path)  # data文件夹下的文件名称
    if len(dir_file) < 96:  # 一个空 list 本身等同于 False
        print("1 day not files can handle")
    else:
        for f_name in dir_file:
            if f_name.endswith(handle_file_type):
                standard_time = f_name.split('_')[4][0:8]  # 获取 20170325 类似的时间
                start_time, end_time = get_time(f_name, 900, 3)  # 时间
                tmp_list = []
                for aim_name in dir_file:
                    if standard_time == aim_name.split('_')[4][0:8]:
                        aim_path = os.path.join(dir_path, aim_name)  # 合成路径
                        tmp_list.append(aim_path)
                un_list = list(set(tmp_list))  # 先转为set(不重复的集合)
                if len(un_list) == 96:
                    aim_path_dic = {'file_path': un_list, 'e_time': end_time, 's_time': start_time}
                    file_list.append(aim_path_dic)
    return file_list


def get_time(file_path, add_s, flag):
    """获取时间"""
    time_str = file_path.split('_')[4]
    if flag == 3:   # 处理一天的时间
        time_str = time_str.split('.')[0] + '00'  # 获取时间
        s_time = datetime.datetime.strptime(time_str, "%Y%m%d%H%M%S")
        add_t = datetime.timedelta(seconds=add_s)  # 在begin_time加秒数
        end_time = s_time + add_t
        sub_time = datetime.timedelta(days=-1)
        start_time = end_time + sub_time
    else:
        time_str = time_str.split('.')[0] + '00'  # 获取时间
        start_time = datetime.datetime.strptime(time_str, "%Y%m%d%H%M%S")
        add_t = datetime.timedelta(seconds=add_s)  # 在begin_time加秒数
        end_time = start_time + add_t
    return start_time, end_time


def get_file_list_by_period(dir_file, dir_path, time_args):
    """
    根据时间段处理文件
    :param dir_file 目录文件列表
    :param dir_path  目录路径
    :param time_args 开始、结束时间
    :return file_list 返回处理csv文件列表
    """
    file_list = []
    begin_time = datetime.datetime.strptime(time_args[0], "%Y-%m-%d %H:%M")  # 格式化字符串日期
    end_time = datetime.datetime.strptime(time_args[1], "%Y-%m-%d %H:%M")
    for f_name in dir_file:
        file_type = f_name[-4:]
        if file_type == '.chk':  # 判断是否为'.chk' 文件
            time_str = f_name.split('_')[4]
            time_str = time_str.split('.')[0]+'00'
            time = datetime.datetime.strptime(time_str, "%Y%m%d%H%M%S")
            if begin_time <= time <= end_time:    # 判断是否该文件是否在该时间段
                standard = f_name.split('.')[0] + '.csv'
                for f_file in dir_file:
                    if f_file == standard:  # 获取另外一个时间相同文件
                        f_path = os.path.join(dir_path, f_file)  # 绝对路径
                        f_t_list = {'file_path': f_path, 'time': time}
                        file_list.append(f_t_list)  # file_list包含找到的csv文件及时间
            else:
                continue
    return file_list


def move_file(folder_path, *file_args):
    """
    移动文件
    :param file_args csv文件名字
    :param folder_path 目的文件夹
    """
    abspath = os.path.abspath('.')  # 获取当前绝对路径
    data_path = os.path.join(abspath, folder_path)  # 移动的目的文件夹
    is_exist = os.path.exists(data_path)
    if is_exist is False:  # 判断目的文件夹是否存在
            os.mkdir(folder_path)
    if len(file_args) == 2:
        shutil.copy(file_args[0], data_path)  # 复制文件，存在则覆盖
        for f_path in file_args:
            os.remove(f_path)  # 删除当前文件
    elif len(file_args) == 1:
        for f_path in file_args[0]:
            shutil.copy(f_path, data_path)
            os.remove(f_path)


def df_column_build_apply(df, col_num, col_name, group_key, agg_key):
    """
    组建新的df,分组，求和
    :param df
    :param col_num 原数据的列
    :param col_name 新的df的列名
    :param group_key 分组的索引
    :param agg_key 运算的列名 <list>
    :return grouped_df 分组之后的df
    """
    new_df = pd.DataFrame(df.iloc[:, col_num])
    new_df.columns = col_name
    grouped_df = new_df.groupby(group_key, as_index=False)[agg_key].agg((lambda arr: arr.sum()))  # 分组
    # grouped = grouped[apply_key].apply((lambda arr: arr.sum()))
    # grouped_df = grouped.reset_index()
    return grouped_df


def df_column_build_two_agg(df, col_num, col_name, group_key, agg_key1, agg_key2):
    """
    组建新的df,分组，两列求和
    :param df
    :param col_num 原数据的列
    :param col_name 新的df的列名
    :param group_key 分组的索引
    :param agg_key1 运算的列名
    :param agg_key2 运算的列名
    :return grouped_df 分组之后的df
    """
    new_df = pd.DataFrame(df.iloc[:, col_num])
    new_df.columns = col_name
    grouped_df = new_df.groupby(group_key, as_index=False).agg({agg_key1: 'sum', agg_key2: 'sum'})  # 分组
    return grouped_df


def df_column_build_count(df, col_num, col_name, group_key):
    """
    组建新的df,分组，求和
    :param df
    :param col_num 原数据的列
    :param col_name 新的df的列名
    :param group_key 分组的索引
    :return grouped_df 分组之后的df
    """
    new_df = pd.DataFrame(df.iloc[:, col_num])
    new_df.columns = col_name
    grouped_df = new_df.groupby(group_key, as_index=False).count()  # 分组
    return grouped_df


def df_apply(df, group_key, agg_key):
    """
    df分组，某些列求和
    :param df
    :param group_key分组的索引
    :param agg_key运算的列名 <list>
    :return grouped_df
    """
    grouped = df.groupby(group_key, as_index=False)  # 分组
    # grouped_df = grouped[agg_key].agg((lambda arr: arr.sum()))
    grouped_df = grouped[agg_key].sum()
    # grouped_df = grouped.reset_index()
    return grouped_df


def df_two_agg(df, group_key, agg_key1, agg_key2):
    grouped = df.groupby(group_key, as_index=False)  # 分组
    grouped_df = grouped.agg({agg_key1: 'sum', agg_key2: 'sum'})
    return grouped_df


def save_data_db(df_data, db_table, db_base):
    """
    保存数据到数据库
    :param df_data数据
    :param db_table表名
    :param db_base
    """
    y_connect = create_engine(constant.ENGINE_STR)
    pd.io.sql.to_sql(df_data, db_table, y_connect, schema=db_base, if_exists='append', index=False)
    # chunksize


def read_sql(sql, engine):
    #  sql = 'select ii_msisdn,ii_imei from imei_current'
    y_connect = create_engine(engine)
    df = pd.read_sql(sql, y_connect)
    return df


def initlog():
    # 生成一个日志对象
    logger = logging.getLogger()
    # 生成一个Handler。logging支持许多Handler，例如FileHandler, SocketHandler, SMTPHandler等，
    # log_file是一个全局变量，它就是一个文件名
    log_file = constant.SIG_LOG_FILE
    log_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s %(filename)s %(levelname)s %(message)s')
    # 将格式器设置到处理器上
    log_handler.setFormatter(formatter)
    # 将处理器加到日志对象上
    logger.addHandler(log_handler)
    logger.setLevel(logging.NOTSET)
    return logger


def filter_df(df, flags):
    # 筛选不合格的数据
    msisdn_null_df = df[df.iloc[:, 0].str.contains(r'^nan$', flags=re.I, na=True)]  # msisdn为空或者数字
    apn_null_int_df = df[df.iloc[:, 5].str.contains(r'^nan$|^\d$', flags=re.I, na=True)]  # apn为空或者数字
    region_code_null_df = df[df.iloc[:, 12].str.contains(r'^nan$', flags=re.I, na=True)]  # region_code为空
    gtp_cause_null_df = df[df.iloc[:, 9].str.contains(r'^nan$', flags=re.I, na=True)]  # gtp_cause为空
    error_df = pd.concat([msisdn_null_df, apn_null_int_df, region_code_null_df, gtp_cause_null_df])
    err_index = error_df.index
    df.drop(err_index, axis=0, inplace=True)  # 删除不合格的行
    if flags == 1:
        return error_df, df
    else:
        return df


def save_error_csv(folder_path, df, s_time):
    # 将不合格的数据存为csv文件
    abspath = os.path.abspath('.')
    dir_path = os.path.join(abspath, folder_path)
    time = s_time.strftime('%Y%m%d%H%M%S')
    f_path = os.path.join(dir_path, time)
    df.to_csv(f_path+'.csv', encoding='utf-8', header=None)



