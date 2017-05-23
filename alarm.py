# -*- coding: UTF-8 -*-
import fun_model
import constant
import pandas as pd
import numpy as np
import os
import stat
import logging
import re
from datetime import datetime
from datetime import timedelta
import mysql.connector
from apscheduler.schedulers.background import BlockingScheduler


def request_alarm_read(now_t):
    """分类： 1-激活成功率告警 2-核心网告警 3-大客户告警 4-专线告警"""
    # now_t = datetime.now()
    # now_t = '2017-04-27 11:00:00'
    # now_t = datetime.strptime(now_t, "%Y-%m-%d %H:%M:%S")
    s_time = now_t + timedelta(minutes=-53)
    s_time = s_time.strftime('%Y-%m-%d %H:%M:%S')
    e_time = now_t + timedelta(minutes=-47)
    e_time = e_time.strftime('%Y-%m-%d %H:%M:%S')
    # sql = "select time_end, apn, region_id,request_rate from wl_c_success_rate_make_i15 where " \
    #       "request_rate <= 0.8 and region_code >1 and apn != 'all' and time_end > '{:s}' and time_end < '{:s}'".\
    #     format(s_time, e_time)  # todo region_id

    sql = "select time_end, apn, region_id,request_rate from wl_c_success_rate_make_i15 where " \
          "request_rate <= 0.8 and region_code >1 and apn != 'all' and time_end = '{}'".format(now_t)
    df = fun_model.read_sql(sql, constant.ENGINE_STR)
    print (df)
    region_name_id = region_code_id()
    name_df = pd.merge(df, region_name_id.loc[:, ['region_id', 'region_name']], how='left', on='region_id')
    name_df['alarm_level'] = map(lambda x: 3 if 0.8 >= x > 0.75 else (2 if 0.75 >= x > 0.7 else (1 if x <= 0.7 else 0)),
                                 name_df['request_rate'])
    name_df['are_tag'] = map(lambda x, y, z: '1_%s_%d_%d' % (x, y, z), name_df['apn'], name_df['region_id'],
                             name_df['alarm_level'])
    lt_df = request_alarm_level_title()
    title_df = pd.merge(name_df, lt_df, how='left', on='alarm_level')
    title_df['content'] = map(lambda x, y, z, level: '<{}>APN<{}> 激活成功率{:.2%}，一级门限阀值70%，主要激活失败原因<>'.format(x, y, z)
    if level == 1 else ('<{}>APN<{}> 激活成功率{:.2%}，二级门限阀值75%，主要激活失败原因<>'.format(x, y, z)
                        if level == 2 else ('<{}>APN<{}> 激活成功率{:.2%}，三级门限阀值80%，主要激活失败原因<>'.format(x, y, z)
                                            if level == 3 else ''))
                              , title_df['region_name'], title_df['apn'], title_df['request_rate'], title_df['alarm_level'])

    title_df.columns = ['time_end', 'are_lines', 'are_region_id', 'are_val', 'are_region', 'are_status_level',
                        'are_tag', 'are_title', 'are_explain']
    sql = "select are_lines, are_region_id, are_class, are_drop, are_status_level, are_tag from wl_alarm_record " \
          "where are_drop = 1 and are_class = 1"
    record_df = fun_model.read_sql(sql, constant.ANALYSIS_ENGINE_STR)
    alarm_df = title_df.drop('time_end', axis=1)
    alarm_df['are_val'] *= 100
    alarm_df['are_tag'] = alarm_df['are_tag'].astype('object')
    alarm_record(alarm_df, record_df, 1)
    print (alarm_df)
    # csv文件的df
    csv_df = title_df.drop(['are_region_id', 'are_val'], axis=1)  # 删除
    return csv_df, now_t


def boce_alarm(now_t):
    # now_t = datetime.now()
    # now_t = '2017-05-16 02:00:00'
    # e_time = datetime.strptime(now_t, "%Y-%m-%d %H:%M:%S")

    e_time = now_t.strftime('%Y-%m-%d %H:%M:%S')
    s_time = now_t + timedelta(minutes=-60)
    s_time = s_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = "select apn, part_no, rate_success from wl_source_dialtest where " \
          "time_test_start > '{}' and time_test_start <= '{}'". \
        format(s_time, e_time)  # todo wl_source_dialtest
    bc_df = fun_model.read_sql(sql, constant.TOTAL_ENGINE_STR)
    bc_df['rate_success'] = bc_df['rate_success'].astype('float64')
    grouped = bc_df.groupby(['apn', 'part_no'], as_index=False).agg({'rate_success': np.mean})
    # 获取告警的记录
    d1_d2_df = grouped[grouped.part_no.isin(['D1', 'D2'])]
    d3_df = grouped[grouped.part_no.isin(['D3'])]
    d1_d2_df = d1_d2_df[d1_d2_df['rate_success'] < 90]
    d3_df = d3_df[d3_df['rate_success'] < 80]
    # 拼接基础信息
    bc_alarm = pd.concat([d1_d2_df, d3_df], axis=0, ignore_index=True)
    bc_alarm['are_status_level'] = map(bc_part_level, bc_alarm['part_no'], bc_alarm['rate_success'])
    ria_df = region_id_apn()
    bc_al = pd.merge(bc_alarm, ria_df, how='left', on='apn')
    bc_al['are_tag'] = map(lambda x, y, z, c: '2_%s_%d_%d' % (x, y, z) if c == 'D1' else (
            '3_%s_%d_%d' % (x, y, z) if c == 'D2' else ('4_%s_%d_%d' % (x, y, z) if c == 'D3' else '')),
                           bc_al['apn'], bc_al['region_id'], bc_al['are_status_level'], bc_al['part_no'])
    title_df = zhuan_alarm_level_title()
    bc_title = pd.merge(bc_al, title_df, how='left', on=['are_status_level', 'part_no'])
    bc_title['are_explain'] = map(bc_level_explain, bc_title['part_no'], bc_title['region_name'], bc_title['apn'],
                                  bc_title['rate_success'], bc_title['are_status_level'])

    bc_title.columns = ['are_lines', 'part_no', 'are_val', 'are_status_level', 'are_region', 'are_region_id',
                           'region_code', 'are_tag', 'are_title', 'are_explain']
    # 分组D1，D2，D3分别处理
    part_d1 = bc_title[bc_title.part_no.isin(['D1'])]
    part_d2 = bc_title[bc_title.part_no.isin(['D2'])]
    part_d3 = bc_title[bc_title.part_no.isin(['D3'])]
    part_d1 = part_d1.drop(['part_no', 'region_code'], axis=1)
    sql = "select are_lines, are_region_id, are_class, are_drop, are_status_level, are_tag from wl_alarm_record " \
          "where are_drop = 1 and are_class = 2"
    record_df = fun_model.read_sql(sql, constant.ANALYSIS_ENGINE_STR)
    alarm_record(part_d1, record_df, 2)
    part_d2 = part_d2.drop(['part_no', 'region_code'], axis=1)
    sql = "select are_lines, are_region_id, are_class, are_drop, are_status_level, are_tag from wl_alarm_record " \
          "where are_drop = 1 and are_class = 3"
    record_df = fun_model.read_sql(sql, constant.ANALYSIS_ENGINE_STR)
    alarm_record(part_d2, record_df, 3)
    part_d3 = part_d3.drop(['part_no', 'region_code'], axis=1)
    sql = "select are_lines, are_region_id, are_class, are_drop, are_status_level, are_tag from wl_alarm_record " \
          "where are_drop = 1 and are_class = 4"
    record_df = fun_model.read_sql(sql, constant.ANALYSIS_ENGINE_STR)
    alarm_record(part_d3, record_df, 4)

    print (bc_title)

    # csv文件
    csv_df = bc_title.drop(['are_region_id', 'are_val', 'part_no', 'region_code'], axis=1)  # 删除
    csv_df['time_end'] = now_t
    return csv_df


def save_csv(request_csv, now_t, boce_csv):
    # 生成告警csv文件
    csv_df = pd.concat([request_csv, boce_csv], axis=0, ignore_index=True)
    abspath = r'E:\PythonWork\tmp_file'
    f_name = now_t.strftime('%Y%m%d%H%M')
    f_name += '00'
    file_path = os.path.join(abspath, f_name)
    columns = ['time_end', 'are_region', 'are_lines', 'are_tag', 'are_title', 'are_status_level', 'are_explain']
    csv_df.to_csv(file_path + '.csv', encoding='utf-8', index=False, header=None, quoting=1, columns=columns)
    os.chmod(file_path + '.csv', stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  # 更改文件权限mode:777


def alarm_record(ar_df, record_df, are_class):
    dif_df = pd.merge(ar_df, record_df, how='left', on=['are_lines', 'are_region_id'])
    new_df = dif_df[dif_df.loc[:, 'are_tag_y'].str.contains(r'^nan$', flags=re.I, na=True)]
    if not new_df.empty:
        insert_alarm(ar_df, new_df.loc[:, 'are_tag_x'], are_class)  # 现有记录不在数据库中直接插入

    diff_df = pd.merge(record_df, ar_df, how='left', on=['are_lines', 'are_region_id'])  # 拼接
    up_df = diff_df[diff_df.loc[:, 'are_tag_y'].str.contains(r'^nan$', flags=re.I, na=True)]  # 以are_tag判断
    if not up_df.empty:
        up_uniq_df = up_df.drop_duplicates(['are_lines', 'are_region_id', 'are_class', 'are_status_level_x'])
        up_uniq_df.reset_index(drop=True, inplace=True)  # 删除原来的索引，重排
        drop_time = datetime.now()
        are_drop_time = drop_time.strftime('%Y-%m-%d %H:%M:%S')
        update_alarm(constant.ANALYSIS_DATABASE, up_uniq_df, are_drop_time)  # 数据库中记录在现有的记录不存在，则更新

    yes_df = diff_df[diff_df.loc[:, 'are_tag_x'].str.contains(r'^nan$', flags=re.I, na=True) == False]  # 以are_tag判断
    # 获取升级或降级的记录
    insert_df = yes_df[yes_df['are_status_level_x'] > yes_df['are_status_level_y']]
    drop_in_df = yes_df[yes_df['are_status_level_x'] < yes_df['are_status_level_y']]

    i_df = insert_df[insert_df.are_tag_y.isin(record_df.loc[:, 'are_tag']) == False]
    if not i_df.empty:  # 数据库的记录告警级别大于现有记录，则插入
        insert_alarm(ar_df, i_df.loc[:, 'are_tag_y'], are_class)
    if not drop_in_df.empty:  # 反之，消除数据库的告警，添加新的级别告警
        judge_in_up(ar_df, drop_in_df, are_class)


def judge_in_up(ar_df, drop_in_df, are_class):
    # 判断降级升级（更新，插入）
    drop_time = datetime.now()
    are_drop_time = drop_time.strftime('%Y-%m-%d %H:%M:%S')
    drop_in_df.reset_index(drop=True, inplace=True)
    update_alarm(constant.ANALYSIS_DATABASE, drop_in_df, are_drop_time)  # 更新
    # 判断是否数据库记录已经存在
    sql = "select are_lines, are_region_id, are_class, are_drop, are_status_level, are_tag from wl_alarm_record where " \
          "are_drop = 1 and are_class = {}".format(are_class)
    record_df = fun_model.read_sql(sql, constant.ANALYSIS_ENGINE_STR)
    in_df = drop_in_df[drop_in_df.are_tag_y.isin(record_df.loc[:, 'are_tag']) == False]
    insert_alarm(ar_df, in_df.loc[:, 'are_tag_y'], are_class)


def insert_alarm(ar_df, isin_key, are_class):
    # 添加告警的记录
    ins_df = ar_df[ar_df.are_tag.isin(isin_key)]
    # alarm_df = ins_df.drop('time_end', axis=1)
    ins_df['are_class'] = are_class
    # ins_df['are_val'] *= 100
    alarm_df = ins_df.round(2)
    fun_model.save_data_db(alarm_df, 'wl_alarm_record', constant.ANALYSIS_DATABASE)  # 告警记录保存到数据库


def update_alarm(dbase, df, drop_time):
    # 更新记录
    try:
        global db
        db = mysql.connector.connect(host=constant.DB_HOST, user=constant.DB_USER_NAME, password=constant.DB_PASSWORD,
                                     database=dbase, autocommit=False)
        # mysql.connector默认不会自动提交事务，插入，更新，需要手动commit，特别注意数据表是否支持事务ENGINE=InnoDB
        cursor = db.cursor()
        db.start_transaction()  # 可省略 开始事务
        length = len(df)
        for i in range(length):
            up_sql = "update wl_alarm_record set are_drop=0,are_drop_time='{}' where are_tag= '{}' and are_drop = 1".\
                format(drop_time, df.loc[i, 'are_tag_x'])
            cursor.execute(up_sql)  # 变化表
            db.commit()

    except mysql.connector.Error as e:
        db.rollback()
        print ("Error %d: %s" % (e.args[0], e.args[1]))
    else:
        cursor.close()
        db.close()


def region_code_id():
    re_list = [[1, '0000', '四川'], [2, '028', '成都'], [3, '0816', '绵阳'], [4, '0813', '自贡'], [5, '0812', '攀枝花'],
               [6, '0830', '泸州'], [12, '0838', '德阳'], [13, '0839', '广元'], [14, '0825', '遂宁'], [15, '0832', '内江'],
               [16, '0833', '乐山'], [17, '0901', '资阳'], [18, '0831', '宜宾'], [19, '0817', '南充'], [20, '0818', '达州'],
               [21, '0835', '雅安'], [22, '0837', '阿坝'], [23, '0836', '甘孜'], [24, '0834', '凉山'], [25, '0826', '广安'],
               [26, '0827', '巴中'], [27, '0902', '眉山'], [28, '0281', '天府新区'], [9999, '-1']]
    re_df = pd.DataFrame(re_list, columns=['region_id', 'region_code', 'region_name'])
    return re_df


def region_id_apn():
    ria_list = [['阿坝', 'abncxys.sc'],
                ['巴中', 'bzsfxt.sc'],
                ['巴中', 'bzsgs.sc'],
                ['广安', 'gadlxj.sc'],
                ['广安', 'gahycgt.sc'],
                ['广安', 'hysfdl.sc'],
                ['广安', 'scgaaz.sc'],
                ['凉山', 'lszychy.sc'],
                ['凉山', 'nndlck.sc'],
                ['凉山', 'lsjdlte.sc'],
                ['凉山', 'hlxys.sc'],
                ['泸州', 'lsjjjwt.sc'],
                ['绵阳', 'jyhfjx.sc'],
                ['绵阳', 'mycgj.sc'],
                ['绵阳', 'myddb.sc'],
                ['绵阳', 'mydlxt.sc'],
                ['绵阳', 'jslg.sc'],
                ['绵阳', 'shelljq.sc'],
                ['绵阳', 'styadl.sc'],
                ['绵阳', 'myrqgs.sc'],
                ['广元', 'gygaj.sc'],
                ['德阳', 'dyhbj.sc'],
                ['德阳', 'dysyx.sc'],
                ['南充', 'rqgs.sc'],
                ['达州', 'dzdfdl.sc'],
                ['遂宁', 'pxwsj.sc'],
                ['遂宁', 'snrsj.sc'],
                ['攀枝花', 'panmydl.sc'],
                ['攀枝花', 'pzhswjt.sc'],
                ['攀枝花', 'panpx.sc'],
                ['德阳', 'dymzld.sc'],
                ['达州', 'dzqxdl.sc'],
                ['乐山', 'lsdlgf.sc'],
                ['乐山', 'lscqgdj.sc'],
                ['南充', 'nbpd.sc'],
                ['南充', 'ysscdl.sc'],
                ['乐山', 'lsgasplte.sc'],
                ['遂宁', 'slmzdl.sc'],
                ['遂宁', 'snpxgd.sc'],
                ['达州', 'dzdbzz.sc'],
                ['德阳', 'zjzfj.sc'],
                ['广元', 'gajtc.sc'],
                ['南充', 'ncssh.sc'],
                ['内江', 'njdkhce.sc'],
                ['广安', 'gawsxgd.sc'],
                ['南充', 'ncpahy.sc'],
                ['乐山', 'lsdyj.sc'],
                ['成都', 'shebaoju.sc'],
                ['乐山', 'lsjjjwt.sc'],
                ['雅安', 'yayjgtj.sc'],
                ['巴中', 'bzsgtj.sc'],
                ['成都', 'cdscdllte.sc'],
                ['巴中', 'bzszy.sc'],
                ['巴中', 'bztjzf.sc'],
                ['达州', 'dzdlcb.sc'],
                ['广安', 'gajcs.sc'],
                ['简阳', 'jysqwggl.sc'],
                ['遂宁', 'mxdl.sc'],
                ['遂宁', 'mxdlzk.sc'],
                ['攀枝花', 'pzhszxyy.sc'],
                ['甘孜', 'scgzga.sc'],
                ['遂宁', 'scsndydl.sc'],
                ['广安', 'scwsgd.sc'],
                ['遂宁', 'shmzdl.sc'],
                ['遂宁', 'sncyt.sc'],
                ['遂宁', 'sndydlnd.sc'],
                ['遂宁', 'sndydlyx.sc'],
                ['广元', 'gymenglinranqi.sc'],
                ['成都', 'cdcsidc.sc'],
                ['成都', 'cdcszm.sc'],
                ['成都', 'cdczc.sc'],
                ['成都', 'cdfucai.sc'],
                ['成都', 'cdgat.sc'],
                ['成都', 'cdgatzazd.sc'],
                ['成都', 'cdgjjt.sc'],
                ['成都', 'cdgjjtlte.sc'],
                ['成都', 'cdgongan.sc'],
                ['成都', 'cdjwtlte.sc'],
                ['成都', 'cdnonghang.sc'],
                ['成都', 'cdnsyh.sc'],
                ['成都', 'cdpflte.sc'],
                ['成都', 'cdpost.sc'],
                ['成都', 'cdxd.sc'],
                ['成都', 'cdylsw.sc'],
                ['成都', 'cdyzgs.sc'],
                ['成都', 'cdzh.sc'],
                ['成都', 'cdzhlte.sc'],
                ['成都', 'cgtlte.sc'],
                ['成都', 'ciblte.sc'],
                ['成都', 'cmblteatm.sc'],
                ['成都', 'ecitic.sc'],
                ['成都', 'scgylte.sc'],
                ['成都', 'scjpxm.sc'],
                ['成都', 'scmjsd.sc'],
                ['成都', 'scsjt.sc'],
                ['成都', 'scsygljtxgs.sc'],
                ['成都', 'scttlte.sc'],
                ['成都', 'shxnyqj.sc'],
                ['成都', 'cdslrt.sc'],
                ['成都', 'ccblteatm.sc'],
                ['成都', 'cdairchina.sc'],
                ['成都', 'cdajlte.sc'],
                ['成都', 'cdbocd.sc'],
                ['成都', 'cdczclte.sc'],
                ['成都', 'cddgj.sc'],
                ['成都', 'cddlgs.sc'],
                ['成都', 'cdghyyb.sc'],
                ['成都', 'cdgtdcj.sc'],
                ['成都', 'cdjgkj.sc'],
                ['成都', 'cdjyrq.sc'],
                ['成都', 'cdmpjdtx.sc'],
                ['成都', 'cdmptx.sc'],
                ['成都', 'cdschqgaj.sc'],
                ['成都', 'cdscjrlhh.sc'],
                ['成都', 'cdwhgalte.sc'],
                ['成都', 'cdxnyq.sc'],
                ['成都', 'cdyxbz.sc'],
                ['成都', 'cebbanklteatm.sc'],
                ['成都', 'cxdsjlte.sc'],
                ['成都', 'dljxgs.sc'],
                ['成都', 'gxjclte.sc'],
                ['成都', 'huanbaozhuanwang.sc'],
                ['泸州', 'lzjtjt.sc'],
                ['成都', 'scdllte.sc'],
                ['成都', 'scjcjt.sc'],
                ['成都', 'scxfzd.sc'],
                ['成都', 'sczwbdkjyxgs.sc'],
                ['成都', 'slxnrq.sc'],
                ['成都', 'sqcxj.sc'],
                ['成都', 'whgawrjlte.sc'],
                ['成都', 'yjxny.sc'],
                ['成都', 'zshlte.sc'],
                ['成都', 'cdsjlte.sc'],
                ['宜宾', 'jldl.sc'],
                ['泸州', 'lzgajz.sc'],
                ['泸州', 'lzjjd.sc'],
                ['泸州', 'lzqckj.sc'],
                ['泸州', 'lzzjjqh.sc'],
                ['内江', 'njgadb.sc'],
                ['宜宾', 'ybcndl.sc'],
                ['宜宾', 'ybdlpd.sc'],
                ['宜宾', 'ybgaox.sc'],
                ['宜宾', 'ybgtzy.sc'],
                ['宜宾', 'ybgxdl.sc'],
                ['宜宾', 'ybjjzf.sc'],
                ['宜宾', 'ybpsdl.sc'],
                ['宜宾', 'sccyt.sc'],
                ['宜宾', 'ybgtj.sc'],
                ['宜宾', 'ybgabd.sc'],
                ['宜宾', 'yhrrq.sc'],
                ['宜宾', 'ybxwdl.sc'],
                ['自贡', 'zgcgt.sc'],
                ['自贡', 'zgpwqx.sc'],
                ['内江', 'lcjjd.sc'],
                ['成都', 'scryylte.sc'],
                ['泸州', 'lzgajbd.sc'],
                ['泸州', 'lzgajbjb.sc'],
                ['资阳', 'zyyjdl.sc'],
                ['资阳', 'zylzdl.sc'],
                ['资阳', 'sczyga.sc'],
                ['资阳', 'zyaydl.sc'],
                ['泸州', 'lzrsj.sc'],
                ['泸州', 'lzgaj.sc'],
                ['内江', 'njga.sc'],
                ['泸州', 'glgac.sc'],
                ['资阳', 'zygajwtlte.sc'],
                ['资阳', 'zyscdl.sc'],
                ['资阳', 'sczygajz.sc'],
                ['资阳', 'zygakt.sc'],
                ['资阳', 'zylzlb.sc'],
                ['资阳', 'yjnbrzpt.sc']]
    ria_df = pd.DataFrame(ria_list, columns=['region_name', 'apn'])
    rci_df = region_code_id()
    df = pd.merge(ria_df, rci_df, how='left', on='region_name')
    return df


def request_alarm_level_title():
    lt_list = [[3, 'M2M APN激活成功率低于三级门限'], [2, 'M2M APN激活成功率低于二级门限'], [1, 'M2M APN激活成功率低于一级门限']]
    lt_df = pd.DataFrame(lt_list, columns=['alarm_level', 'title'])
    return lt_df


def zhuan_alarm_level_title():
    alt_list = [[3, 'D3', 'M2M APN专线拨测成功率低于三级门限'], [2, 'D3', 'M2M APN专线拨测成功率低于二级门限'], [1, 'D3', 'M2M APN专线拨测成功率低于一级门限'],
                [3, 'D2', 'M2M APN大客户平台拨测成功率低于三级门限'], [2, 'D2', 'M2M APN大客户平台拨测成功率低于二级门限'], [1, 'D2', 'M2M APN大客户平台拨测成功率低于一级门限'],
                [3, 'D1', 'M2M APN核心网拨测成功率低于三级门限'], [2, 'D1', 'M2M APN核心网拨测成功率低于二级门限'], [1, 'D1', 'M2M APN核心网拨测成功率低于一级门限']]
    alt_df = pd.DataFrame(alt_list, columns=['are_status_level', 'part_no', 'are_title'])
    return alt_df


def bc_part_level(part, rate_succ):
    flag = 0
    if part == 'D3':
        if rate_succ < 50:
            flag = 1
        elif 50 <= rate_succ < 70:
            flag = 2
        else:
            flag = 3
    else:
        if rate_succ < 70:
            flag = 1
        elif 70 <= rate_succ < 80:
            flag = 2
        else:
            flag = 3
    return flag


def bc_level_explain(part, region_name, apn, rate, level):
    explain = ''
    if part == 'D3':
        if level == 1:
            explain = '<{}>APN<{}>一小时内专线拨测成功率{:.2f}%，一级门限阀值50%'.format(region_name, apn, rate)
        elif level == 2:
            explain = '<{}>APN<{}>一小时内专线拨测成功率{:.2f}%，二级门限阀值70%'.format(region_name, apn, rate)
        elif level == 3:
            explain = '<{}>APN<{}>一小时内专线拨测成功率{:.2f}%，三级门限阀值80%'.format(region_name, apn, rate)
    if part == 'D2':
        if level == 1:
            explain = '<{}>APN<{}>一小时内大客户平台拨测成功率{:.2f}%，一级门限阀值70%'.format(region_name, apn, rate)
        elif level == 2:
            explain = '<{}>APN<{}>一小时内大客户平台拨测成功率{:.2f}%，二级门限阀值80%'.format(region_name, apn, rate)
        elif level == 3:
            explain = '<{}>APN<{}>一小时内大客户平台拨测成功率{:.2f}%，三级门限阀值90%'.format(region_name, apn, rate)
    if part == 'D1':
        if level == 1:
            explain = '<{}>APN<{}>一小时内核心网拨测成功率{:.2f}%，一级门限阀值70%'.format(region_name, apn, rate)
        elif level == 2:
            explain = '<{}>APN<{}>一小时内核心网拨测成功率{:.2f}%，二级门限阀值80%'.format(region_name, apn, rate)
        elif level == 3:
            explain = '<{}>APN<{}>一小时内核心网拨测成功率{:.2f}%，三级门限阀值90%'.format(region_name, apn, rate)
    return explain


def main():
    b_now_t = '2017-05-16 04:10:00'
    b_now_t = datetime.strptime(b_now_t, "%Y-%m-%d %H:%M:%S")
    r_now_t = '2017-04-27 11:00:00'
    r_now_t = datetime.strptime(r_now_t, "%Y-%m-%d %H:%M:%S")
    # r_now_t = b_now_t = datetime.now().replace(second=0, microsecond=0)
    # print (type(r_now_t))
    bc_csv = pd.DataFrame()
    if b_now_t.minute == 00:
        bc_csv = boce_alarm(b_now_t)
    re_csv, now_t = request_alarm_read(r_now_t)
    save_csv(re_csv, now_t, bc_csv)


if __name__ == '__main__':
    main()
    # logging.basicConfig()
    # scheduler = BlockingScheduler()
    # # 如果超时start_date，也会执行。下一次15分钟执行
    # scheduler.add_job(main, 'interval', start_date='2017-05-08 17:29:00', seconds=2)  # todo 时间
    # print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    # try:
    #     scheduler.start()  # 采用的是阻塞的方式，只有一个线程专职做调度的任务
    # except (KeyboardInterrupt, SystemExit):
    #     scheduler.shutdown()
    #     print('Exit The Job!')

