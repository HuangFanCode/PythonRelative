# -*- coding: UTF-8 -*-
import fun_model
import constant
import pandas as pd
import mysql.connector


@fun_model.time_me
def deal_imei(file_path, time, flags):
    # imei变化, 存取当前imei
    try:
        df = pd.read_csv(file_path, error_bad_lines=False, header=None,
                         dtype={0: 'object', 1: 'object', 5: 'object', 9: 'object', 12: 'object'})
    except Exception as e:
        logger.error("read csv Error %d: %s" % (e.args[0], e.args[1]))
    else:
        true_df = fun_model.filter_df(df, 2)
        im_df = pd.DataFrame(true_df.iloc[:, [0, 1, 5]])
        im_df.columns = ['ii_msisdn', 'ii_imei', 'ii_apn']
        if flags == 1:
            classify_df = im_df[im_df['ii_apn'].str.contains(r'^cmiot|^cmmtm', na=False) == False]  # 现网
        else:
            classify_df = im_df[im_df['ii_apn'].str.contains(r'^cmiot|^cmmtm', na=False)]  # 专网
        # 去重 'last'保留重复数据中的最后一条inplace=True
        no_repeat_df = classify_df.drop_duplicates(['ii_msisdn', 'ii_imei'], keep='last')

        sql = 'select ii_msisdn,ii_imei from imei_current'
        if flags == 1:
            current_df = fun_model.read_sql(sql, constant.ENGINE_STR)  # 数据库读取上次的号码的IMEI(现)
        else:
            current_df = fun_model.read_sql(sql, constant.ZHUAN_ENGINE_STR)  # 专网
        curr_con_df = pd.merge(no_repeat_df, current_df, how='left', on='ii_msisdn')  # 当前的df 与 上次df 合并
        curr_con_df['is_change'] = curr_con_df['ii_imei_x'] == curr_con_df['ii_imei_y']
        # 获取对比is_change为false的数据，即是IMEI变化的号码
        change_df = curr_con_df[curr_con_df.is_change.isin([False])]  # 变化的imei表
        change_df = change_df.rename(columns={'ii_imei_x': 'ii_imei_now', 'ii_imei_y': 'ii_imei_last'})  # 更改列名
        del change_df['is_change']  # 删除is_change列
        change_df.fillna(-1, inplace=True)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S")
        change_df['ii_time_create'] = time_str
        chan_ndarray = change_df.as_matrix()
        chan_list = chan_ndarray.tolist()
        print ('imei change %s is succeed' % file_path)

        # 更新当前IMEI表
        cu_change_df = change_df.drop(['ii_imei_last'], axis=1)   # 去除上次的IMEI列
        cu_change_df = cu_change_df.rename(columns={'ii_imei_now': 'ii_imei'})
        uniq_cu_df = cu_change_df.drop_duplicates(['ii_msisdn'], keep='last')  # 当前表 以ii_msisdn去重，获取最后变化的一行
        curr_ndarray = uniq_cu_df.as_matrix()    # as_matrix 将DataFrame转为ndarray
        curr_list = curr_ndarray.tolist()   # tolist 将ndarray转为list
        if flags == 1:
            write_replace_db(chan_list, curr_list, constant.DATABASE)  # sql语句
        else:
            write_replace_db(chan_list, curr_list, constant.ZHUAN_DATABASE)
        print ('current imei %s is succeed' % file_path)


def write_replace_db(chan_list, curr_list, db_base):
    """批量执行sql语句  replace into替换插入"""
    chan_sql = "insert into imei_inconsistent (ii_msisdn, ii_imei_now, ii_apn, ii_imei_last, ii_time_create) " \
               "values (%s, %s, %s, %s, %s)"
    curr_sql = "replace into imei_current (ii_msisdn, ii_imei, ii_apn, ii_time_create) values (%s, %s, %s, %s)"
    length = len(chan_list)/10000+1
    try:
        global db
        db = mysql.connector.connect(host=constant.DB_HOST, user=constant.DB_USER_NAME, password=constant.DB_PASSWORD,
                                     database=db_base, autocommit=False)
        # mysql.connector默认不会自动提交事务，插入，更新，需要手动commit，特别注意数据表是否支持事务ENGINE=InnoDB
        cursor = db.cursor()
        db.start_transaction()  # 可省略 开始事务
        index = 0
        for i in range(length):
            chan_item = chan_list[index: index+10000]  # 分10000条插入数据
            curr_item = curr_list[index: index+10000]
            cursor.executemany(chan_sql, chan_item)  # 变化表
            cursor.executemany(curr_sql, curr_item)  # 当前表
            db.commit()
            index += 10000
    except mysql.connector.Error as e:
        db.rollback()
        print ("Error %d: %s" % (e.args[0], e.args[1]))
        logger.error("Error %d: %s" % (e.args[0], e.args[1]))
    else:
        cursor.close()
        db.close()


def main():
    global logger
    logger = fun_model.initlog()
    """获取文件列表，数据处理"""
    handle_folder = constant.FOLDER_15
    # treated_folder = constant.TREAT_FOLDER  # 完成处理后的数据移动文件夹
    file_list = fun_model.get_folder_file(handle_folder)
    for f in file_list:
        t = deal_imei(f['file_path'], f['e_time'], 1)    # imei变化  t 耗时
        logger.info(f['file_name']+' deal complete time:'+str(t))  # 日志
        # chk_f_path = f['file_path'].split('.')[0] + '.chk'  # chk文件
        # fun_model.move_file(treated_folder, f['file_path'], chk_f_path)   # 移动文件
        logger.info(f['file_name'] + ' move complete')
    logger.info('================= complete a folder traverse =================')  # 遍历一次文件夹处理完成


if __name__ == "__main__":
    main()
