# -*- coding: utf-8 -*-
"""
Created on Thu Jun  7 08:48:43 2018

@author: wengmgna
"""
from functools import reduce  # py3
import xlwt
import pandas as pd
import numpy as np
import os
import datetime as date
from pandas import Series, DataFrame

import urllib
import re
import requests
from bs4 import BeautifulSoup
import json
from lxml import etree
import time
from sklearn.preprocessing import Imputer
from sklearn.feature_selection import VarianceThreshold


to_zero_columns = ['BUSINESS_ADD_ZONE', 'BUSINESS_ADD_CITY', 'BUSINESS_ADDR_PROVINCE', 'UNIT_SCALE',
                    'EMP_ZONE', 'EMP_CITY', 'EMP_PROVINCE', 'YEARS_OF_WORK', 'SOCIAL_PAY_NUM',
                    'SOCIAL_PAY_AMT', 'SOCIAL_PAY_MONTH', 'PRODUCT_CD6'
                   ]

drop_columns = ['BUSINESS_ADD_ZONE','BUSINESS_ADD_CITY','BUSINESS_ADDR_PROVINCE',
                'OTHER_LOAN', 'MATE_ID_TYPE', 'ID_TYPE',  # 方差=0
                'EMP_TITLE',# 缺失数据
                'NAME', 'REGISTER_DETAIL', 'ABODE_DETAIL', 'UNIT_NAME', 'MATE_NAME', 'EMP_ADD',
                'BUSINESS_ADDRESS', 'BUSINESS_NAME', 'MAIN_BUSINESS', 'MATE_UNIT_ADDRESS',  #中文信息
                'BANK_CARD_NO', 'BANK_PROVINCE_CODE', 'BANK_CITY_CODE', 'BANK_ZONE_CODE',  #银行信息
                'PRODUCT_CD1','BUSINESS_REGIST_NUM', 'mate_constellation', 'PRODUCT_CD', 'MATE_ID_NO', 'ID_NO', 'SETUP_DATE','TOTAL_WORK_LIFE',  # 信息已转换
                'app_YYYYMM','MONTHLY_TURNOVER'
                ]

one_hot_columns = ['QUALIFICATION', 'PRODUCT_CD2', 'PRODUCT_CD3', 'EMP_POST', 'EMP_TYPE', 'EMP_STRUCTURE',
                    'APPLICANT_STATUS', 'MARITAL_STATUS', 'PAY_STATUS', 'LOAN_PURPOSE', 'constellation',
                    'app_month', 'PRODUCT_CD4','LOAN_ORG','OCCUPATION','HOUSE_CONDITION','REGISTER_CITY'
                   ]

APP_NO =[]
value_dicts ={'从未通话':2,'偶尔通话':3,'频繁通话':4,
             '使用时间不足6个月':2,'使用时间6个月到2年':3,'使用时间2年到5年':4,'使用时间大于5年':5,
             '数量稀少':2,'数量正常':3,'数量众多':4,
             '很少活动':2,'正常活动':3,'频繁活动':4,
             '从未静默':2,'偶尔静默':3,'正常静默':4,'频繁静默':5,
             '无数据':1,
             '否':1,'是':2,
             '不匹配':2,'模糊匹配':3,'完全匹配':4,
              '正常':2,'未知':3,'欠费':4,'停机':5,
              'Y': 1, 'N': 0,
              'MCEI': 1, 'MCEP': 0
             }

map_trans_cols = ['call_110_analysis_6month','call_120_analysis_6month','call_macau_analysis_6month','call_lawyer_analysis_6month','call_court_analysis_6month','loan_contact_analysis_6month','collection_contact_analysis_6month','mobile_net_age_analysis','mutual_number_analysis_6month','late_night_analysis_6month','mobile_silence_analysis_6month','emergency_contact1_analysis_6month','emergency_contact2_analysis_6month','emergency_contact3_analysis_6month','emergency_contact4_analysis_6month','emergency_contact5_analysis_6month','is_call_data_complete_1month','is_call_data_complete_3month','is_call_data_complete_6month','is_msg_data_complete_1month','is_msg_data_complete_3month','is_msg_data_complete_6month','is_consume_data_complete_1month','is_consume_data_complete_3month','is_consume_data_complete_6month','real_name_check_yys','identity_code_check_yys','home_addr_check_yys','email_check_yys','account_status']


col_value = ['contact_count_6month','call_time_active_6month','call_time_passive_6month','msg_count_6month']

drop_cols_dict ={'t_rsp_loanee.xls':['SEQ','UUIDSEQ','INP_DATE','STRANSNO','REASONCODE','INDUSTRY','ORG_CODE','NAME','ID_NO','BUSIDATE'],
                 't_rsp_freloanbox.xls':['SEQ','UUIDSEQ','INP_DATE','STRANSNO','ORG_CODE','TRANS_CODE','NAME','MOBILENO','IDENTITY_CODE','QUERY_TYPE','MESSAGE','TASK_ID'],
                 't_rsp_blackperson.xls':['SEQ','UUIDSEQ','INP_DATE','STRANSNO','org_code','name','id_no','OCCURREDTIME'],
                 'mobile_info.xls':['mobile_net_time','user_mobile','mobile_net_addr','mobile_carrier','real_name','identity_code','email','contact_addr']
                 }

expand_file_dict ={'finance_contact_stats.xls':'contact_type',
                   'risk_contact_stats.xls':'risk_type'
                   }

def train_test_split(df, split_ratio=0.8, shuffle=False):
    if shuffle:
        df = df.sample(frac=1).reset_index(drop=True)

    df_train = df.iloc[:(int(len(df) * split_ratio)), :]
    df_test = df.iloc[(int(len(df) * split_ratio)):, :]
    return df_train, df_test

def merge_xlsx(file, key_col, unwanted_sheet=[],user_list =[]):

    xls = pd.ExcelFile(file)
    dfs = []
    for sn in xls.sheet_names:
        if sn in unwanted_sheet:
            continue
        df = pd.read_excel(xls, sn)
        print(sn)
        df = df.drop_duplicates()
        df.drop(columns=df.loc[:, df.isnull().mean() > 0.95].columns, inplace=True)
        if sn == 'cust_job_info':
            df = df.groupby(['APP_NO'], as_index=False).max()

        if sn == 'cust_apply_info':
            df = df.drop_duplicates(['APP_NO'], keep='first')

        if len(user_list) !=0:
            df = df[df[key_col].isin(user_list)]

        dfs.append(df)
    df_final = reduce(lambda left, right: pd.merge(left, right, on=key_col, how='outer'), dfs)

    return df_final

def drop_nan(data,data_bak):
    nan_columns = []
    data_desc = data.describe()

    for i in data_desc.columns:
        mean_value = (data.describe())[i]['mean']
        if str(mean_value) =='nan':
            data_bak[i]=data[i]
            nan_columns.append(i)

    fp = open("./data/nan_columns.txt", 'a+')
    fp.write('\n'.join(nan_columns))
    fp.close()

    data.dropna(axis=1, how='all',inplace=True)

def check_two_cols_equal(df, a, b):
    print(df.loc[:, a].equals(df.loc[:, b]))

def crawl_number_value_from_web():
    '''
    爬取手机号码估价，存储到"tel_value.txt"
    '''
    requests.packages.urllib3.disable_warnings()
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'}
    f = open("phone_number.txt")
    lines = f.readlines()
    lines = list(set(lines))
    print(len(lines))
    f.close()
    result = []
    i = 0  # 计数，每爬取50个号码，sleep一下，友好访问
    for line in lines:
        tel = line.strip()
        url = 'https://www.haomagujia.com/' + tel
        try:
            res = requests.get(url, headers=header, timeout=60, verify=False)
        except (
        requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, requests.Timeout, requests.HTTPError,
        requests.exceptions.ChunkedEncodingError):
            print('error or timeout')
        else:
            price = re.findall('.font class="font1".(\d*?)..font.', res.text, re.S)[1]
            result_i = str(tel) + '--' + str(price)
            #            print(result_i)
            result.append(result_i)
        i = i + 1
        if i % 50 == 0:  # 每爬取五十个号码，sleep 2s
            time.sleep(2)
    fp = open("tel_value.txt", 'a+')
    fp.write('\n'.join(result))
    fp.close()

def get_constellation(month, date):
    """
    return str
    """
    dates = (20, 19, 21, 20, 21, 22, 23, 23, 23, 24, 23, 22)
    constellations = ("摩羯", "水瓶", "双鱼", "白羊", "金牛", "双子", "巨蟹", "狮子", "处女", "天秤", "天蝎", "射手", "摩羯")
    if month < 0 or month > 12: return 'nan'
    if date < dates[month - 1]:
        return constellations[month - 1]
    else:
        return constellations[month]

def map_x(df):
    '''
    二值的离散型特征map转换
    'IS_LOCAL_ESTATE'，'IS_OWN_CAR'，'IS_LOCAL_REGIST'，'APP_TYPE'
    '''
    # 定义转换规则
    YN_mapping = {'Y': 1, 'N': 0}
    APP_TYPE_mapping = {'MCEI': 1, 'MCEP': 0}  # APP_TYPE ['MCEI', 'MCEP']

    # 转换 'IS_LOCAL_ESTATE','IS_OWN_CAR','IS_LOCAL_REGIST','APP_TYPE'
    df['IS_LOCAL_ESTATE'] = df['IS_LOCAL_ESTATE'].map(YN_mapping)
    df['IS_OWN_CAR'] = df['IS_OWN_CAR'].map(YN_mapping)
    df['IS_LOCAL_REGIST'] = df['IS_LOCAL_REGIST'].map(YN_mapping)
    df['APP_TYPE'] = df['APP_TYPE'].map(APP_TYPE_mapping)

def map_value(df,map_cols,value_dict):
    for col in map_cols:
        if col in df.columns:
            df[col] = df[col].map(value_dict)

def one_hot_transrow(df, column):
    '''
    df  :dataframe类型数据输入
    row :为需要one-hot转换的的列名 ，如：'EMP_POST'
    '''
    v_c = df[column].value_counts()
    # column_list = list(set(list(df[column])))  # 当前列所有可能出现的值
    column_list =v_c.index
    print(column_list)
    print(v_c)
    columns = []
    unwish_columns =[]
    for i in column_list:
        if str(i) == 'nan': continue
        if v_c[i] <50:
            unwish_columns.append(i)
            continue
        columns.append(column + '_' + str(i))
    print(columns)
    for r_i in columns:  # 在需要one-hot转换的列前 插入拆分后的列
        df.insert(df.columns.get_loc(column), r_i, 0)

    for idx, item in df.iterrows():
        if str(item[column]) == 'nan': continue
        if item[column] in unwish_columns:continue
        p = str(item[column])
        c = column + '_' + p  # 拼接列名
        df.loc[idx, c] = 1

def get_ID_NUMBER_info(df):
   age_col_name = 'AGE'
   id_col_name = 'ID_NO'
   df.insert(df.columns.get_loc('ID_NO'), 'constellation', '0')
   df.insert(df.columns.get_loc('MATE_ID_NO'), 'MATE_AGE', '0')
   df.insert(df.columns.get_loc('MATE_ID_NO'), 'mate_constellation', '0')
   today = date.datetime.now()

   df[age_col_name] = df[id_col_name].apply(lambda x: today.year - int(str(x)[6:10]) - (
               (today.month, today.day) < (int(str(x)[10:12]), int(str(x)[12:14]))) if str(x) != 'nan' else 0)
   df['MATE_AGE'] = df['MATE_ID_NO'].apply(lambda x: today.year - int(str(x)[6:10]) - (
               (today.month, today.day) < (int(str(x)[10:12]), int(str(x)[12:14]))) if str(x) != 'nan' else 0)
   df['MATE_AGE'] = df['MATE_AGE'].apply(lambda x: x if x>0 else 0)

   df['SETUP_DATE'] = df['SETUP_DATE'].apply(lambda x: today.year - int(str(x).split('-')[0]) - (today.month < (int(str(x).split('-')[1]))) if str(x) != 'nan' or str(x) != 'NaT' else 0)
   # df['SETUP_DATE'] = df['SETUP_DATE'].apply(lambda x: today.year - int(str(x).split('-')[0]) - (today.month < (int(str(x).split('-')[1]))) if str(x) != 'nan'  else 0)

   df['constellation'] = df[id_col_name].apply(
       lambda x: get_constellation(int(str(x)[10:12]), int(str(x)[12:14])) if str(x) != 'nan' else 0)
   df['mate_constellation'] = df['MATE_ID_NO'].apply(
       lambda x: get_constellation(int(str(x)[10:12]), int(str(x)[12:14])) if str(x) != 'nan' else 0)

def get_PHONE_NUMBER_info(data):
    f = open("./data/tel_value.txt")
    lines = f.readlines()
    lines = list(set(lines))
    f.close()
    tel_val = {}
    # 'CELLPHONE' 'MATE_PHONE'
    for line in lines:
        str_i = line.strip()
        tel_num = str_i[:11]
        value = str_i[13:]
        if tel_num not in tel_val:
            tel_val[tel_num] = value
    tel_val['nan'] = 0

    data['CELLPHONE'] = data['CELLPHONE'].apply(lambda x: tel_val[str(int(x))] if str(x) != 'nan' else 0)
    data['MATE_PHONE'] = data['MATE_PHONE'].apply(lambda x: tel_val[str(int(x))] if str(x) != 'nan' else 0)

def combine_feature(data):
    data['repay_per_month'] = data.apply(lambda x:x['APP_LMT']/x['LONG_TERM'],axis=1)
    data['available_balance'] = data.apply(lambda x:x['TOTAL_INCOME'] - x['ROOM_LOAN_REPAY_MONTH'],axis=1)

def product_code(data_1):
    for i in range(1,7):
        data_1.insert(data_1.columns.get_loc('PRODUCT_CD'), ('PRODUCT_CD'+str(i)), '0')

    for idx, item in data_1.iterrows():
        PRODUCT_CD = item['PRODUCT_CD']
        if str(PRODUCT_CD) == 'nan': continue
        PRODUCT_CD_list = PRODUCT_CD.split('-')
        for i in range(1,7):
            r_i =   'PRODUCT_CD'+str(i)
            data_1.loc[idx, r_i] = PRODUCT_CD_list[i-1]
    data_1['PRODUCT_CD6'] = data_1['PRODUCT_CD6'].apply(lambda x: x if str(x) != 'null' else 0)

def merge_data(data,column_A,column_B):
    for idx, item in data.iterrows():
        value_a = item[column_A]
        value_b = item[column_B]
        if value_a ==0 and  value_b ==0:
            print("merge_data", idx)
            continue
        if value_a !=0 and value_b !=0:continue
        if value_a ==0 and value_b !=0:
            data.loc[idx, column_A] = value_b

def mark_label(data):
    data.insert(0, 'label', 0)
    overdue_data =  pd.read_excel("./data_all/0_overdue.xls",sheet_name = "Sheet2")
    overdue_data_app_no = list(set(list(overdue_data['APP_NO'])))

    for idx, item in data.iterrows():
        if item['APP_NO'] in overdue_data_app_no:
            data.loc[idx, 'label'] = 1

def find_month_info(data):
    data.insert(0, 'app_month', '0')
    data['app_month'] = data['APP_NO'].apply(lambda x: (str(x))[4:6])

def fillna_with_mean(data):
    df_float = data.select_dtypes(include='float')
    df_object = data.select_dtypes(include='object')


    float_fill_NaN = Imputer(missing_values=np.nan, strategy='mean', axis=0)
    imputed_f = pd.DataFrame(float_fill_NaN.fit_transform(df_float))
    imputed_f.columns = df_float.columns
    imputed_f.index = df_float.index
    data.loc[:, imputed_f.columns] = imputed_f

    for col in df_object.columns:
        data[col].fillna(data[col].describe()['top'],inplace = True)

    # data.loc[:, imputed_o.columns] = imputed_o

def select_part_features(train_data, train_cols):
    train_data_out = train_data.loc[:, train_cols]
    df_float = train_data_out.select_dtypes(exclude='object')
    df_else = train_data_out.select_dtypes(include='object')


    train_cols  = df_float.columns
    # 删除方差为0的列（数值型数据）
    selector = VarianceThreshold()
    new_values = selector.fit_transform(df_float)#去除后低方差特征后，返回array数据

    selected_cols = selector.get_support() #selected_cols中方差为0的列的value为False

    new_cols = [train_cols[i] for i in range(len(train_cols)) if selected_cols[i]]#重构特征列名
    print(len(selected_cols), len(train_cols),len(new_cols))

    # 拼接 去除低方差数据后的数值型特征 和 object型特征
    values = np.hstack((new_values, df_else.values))
    cols = np.hstack((new_cols, df_else.columns))
    final_data = pd.DataFrame(values, columns=cols)

    #去除object类型数据的低方差特征，describe()['unique']==1表示取值唯一，则方差为0
    for col in df_else.columns:
        print(col,final_data[col].describe()['unique'])
        if final_data[col].describe()['unique']==1:
            final_data.drop(col, axis=1,inplace=True)

    #去除相关系数>0.75 的其一列
    print(final_data.select_dtypes(include='object').dtypes)

    corr_df = final_data.corr().abs()
    del_cols = []
    for index, r in corr_df.iterrows():
        for c in corr_df.columns:
            if np.where(corr_df.columns.values == c) <= np.where(corr_df.columns.values == index): continue
            if r[c] > 0.75:
                print(index + "  &&  " + c + ": " + str(r[c]))
                del_cols.append(c)

    del_cols = list(set(del_cols))

    final_data.drop(del_cols, axis=1,inplace=True)


    final_data.insert(0, 'result', 0)
    final_data['result'] = train_data['result']

    final_data.insert(0, 'APP_NO', 0)
    final_data['APP_NO'] = train_data['APP_NO']

    final_data.insert(0, 'label', 0)
    final_data['label'] = train_data['label']

    print(final_data.shape)

    return final_data

def delete_unwished_features(train_data):
    # train_data = pd.read_csv("./data/data_out/train_output_fillna.csv")
    # relabel_from_result_data(train_data)
    pre_cols = train_data.columns
    unwish_cloumn = ['label','result','APP_NO','Unnamed: 0','Unnamed: 0.1','REGISTER_STATE','REGISTER_ZONE','ABODE_STATE','ABODE_CITY','ABODE_ZONE','EMP_POST_bak','EMP_PROVINCE','EMP_CITY','EMP_ZONE','BANK_CODE','BANK_NAME','申请时间','node','最新节点时间','result','授信金额','是否拒绝','是否策略优化','call_area_city','package_type']

    train_cols  = list(set(pre_cols).difference(set(unwish_cloumn)))#将原始的特征与 unwish_cloumn特征求差集
    train_cols  = list(set(train_cols).difference(set(drop_columns)))#将原始的特征与 unwish_cloumn特征求差集

    data  = select_part_features(train_data,train_cols)
    return data

def select_wanted_features(train_data,wanted_cols):

    wanted_cols = ['score','recharge_amount_3month','CELLPHONE','account_balance','APP_LMT','mobile_net_age','max_continue_active_day_1call_6month_y','YEARS_OF_WORK','available_balance','gap_day_last_silence_day_0call_active_6month','average_consume_amount_6month','AGE','银行_call_time_active_6month','max_continue_silence_day_0call_active_3month','MATE_PHONE','consume_amount_3month','小贷_contact_count_6month','澳门电话_call_time_passive_6month','LOAN_ORG_10001','银行_call_time_passive_6month','110_call_time_active_6month','call_count_passive_3month','app_month_03','ROOM_LOAN_REPAY_MONTH','保险_call_time_passive_6month','emergency_contact1_analysis_6month','小贷_call_time_passive_6month','IS_LOCAL_ESTATE','continue_active_day_over3_1call_6month','EMP_TYPE_N','max_continue_active_day_1call_3month_x','emergency_contact3_analysis_6month','保险_call_time_active_6month','银行_contact_count_6month','PRODUCT_CD6','MONTHLY_TURNOVER','投资理财_call_time_passive_6month','小贷_call_time_active_6month','LOAN_ORG_30007','max_continue_active_day_1call_1month','HOUSE_CONDITION_30','EMP_STRUCTURE_G','EMP_STRUCTURE_B','EMP_STRUCTURE_F','EMP_TYPE_Z','投资理财_contact_count_6month','EMP_TYPE_H','call_lawyer_analysis_6month','APP_TYPE','LOAN_PURPOSE_DEC','MARITAL_STATUS_40','小贷_contact_count_6month','发卡拒绝','PRODUCT_CD2_受薪客户(月利率)','EMP_TYPE_F','PRODUCT_CD2_自雇人士(月利率)','MARITAL_STATUS_20','MARITAL_STATUS_10','is_call_data_complete_6month','PRODUCT_CD2_保单客户(月利率)','EMP_POST_E3','证券_contact_count_6month','无','EMP_STRUCTURE_Z','EMP_POST_E02','is_consume_data_complete_6month','PRODUCT_CD5','LOAN_ORG_30002','逾期91-180天','QUALIFICATION_Q04','110_msg_count_6month','EMP_TYPE_P','app_month_03','email_check_yys','IS_LOCAL_ESTATE','逾期61-90天','LOAN_ORG_10001','LOAN_ORG_30007','EMP_TYPE_N','CNSSAMOUNT','典当拍卖_contact_count_6month','2W-10W','澳门电话_msg_count_6month','逾期31-60天','LOAN_ORG_10001','IS_LOCAL_ESTATE','LOAN_ORG_30002','PRODUCT_CD3_D','emergency_contact1_analysis_6month','QUALIFICATION_Q02','emergency_contact3_analysis_6month','continue_silence_day_over3_0call_3month','app_month_04','PRODUCT_CD2_房贷客户(月利率)','emergency_contact2_analysis_6month','保险_contact_count_6month','LOAN_ORG_30007','constellation_水瓶','constellation_巨蟹','EMP_TYPE_H','EMP_TYPE_Z','EMP_POST_E04','continue_active_day_over3_1call_6month','YEARS_OF_WORK','max_continue_active_day_1call_3month_x','催收_call_time_passive_6month','SHAREHOLD_RATIO','AGE','基金_call_time_active_6month','基金_call_time_passive_6month','QUALIFICATION_Q01','GENDER','identity_code_check_yys','银行_contact_count_6month','call_110_analysis_6month','EMP_STRUCTURE_F','EMP_TYPE_E','PRODUCT_CD6','QUALIFICATION_Q0','continue_silence_day_over15_0call_6month','PRODUCT_CD2_受薪客户(月利率)','证券_contact_count_6month','LOAN_PURPOSE_DEC','EMP_STRUCTURE_B','score','EMP_TYPE_N','澳门电话_contact_count_6month','证券_msg_count_6month','constellation_摩羯','EMP_TYPE_J','late_night_analysis_6month','EMP_POST_E3','小贷_contact_count_6month','EMP_POST_E02','app_month_03']
    train_cols = list(set(wanted_cols))#去重

    data = select_part_features(train_data, train_cols)
    return data

def relabel_from_result_data(data):
    result_data = pd.read_excel('./data_all/0_results.xlsx')
    cancel_data = result_data[result_data['node'].isin(['合同签订']) & result_data['result'].isin(['Cancel','Pass'])]['APP_NO'].values

    data.loc[data[data['APP_NO'].isin(cancel_data)].index,'label'] =1


    df = pd.read_excel('./data_all/0_overdue.xls',sheet_name = 'Sheet2')
    l = []
    for i, r in df.iterrows():
        # print(r[2:])
        if 'Y' in r[2:].values:
            l.append(r.APP_NO)

        data.loc[data[data['APP_NO'].isin(l)].index, 'label'] = 0

    print("succccc")

def train_data_preprocessing(begin_step,end_step,save_intermediate =True,file_in ='',file_out =''):
    '''
    :param begin_step:
    :param end_step:
    :param save_intermediate: 是否保存中间文件，值为False时仅保存最后一步的文件输出
    :return:
    '''
    data = pd.DataFrame()
    data_bak = pd.DataFrame()


    for step in range(begin_step, end_step + 1):
        global one_hot_columns
        global to_zero_columns
        global drop_columns

        if step == 1:continue

        out_file =''
        save_data_bak = False
        if step == 2:  # 2.去除全nan数据列
            print("step2:")
            data = pd.read_excel("./data/data_out/data.xlsx", sheet_name="Sheet1")
            data_bak = pd.read_excel("./data/data_bak.xlsx", sheet_name="Sheet1")
            save_data_bak = True
            out_file = './data/output2_dropna.xlsx'

            drop_nan(data, data_bak)



        if step == 3:  # 3.身份证信息转换 年龄星座 + 电话号码 转换 估值
            print("step3:")
            out_file = './data/output3_trans_info.xlsx'
            if step == begin_step:  # load data
                data = pd.read_excel("./data/output2_dropna.xlsx", sheet_name="Sheet1")
            #转换"APP_NO"中的月份信息
            find_month_info(data)
            #拆分PRODUCT_CD列为6列 PRODUCT_CD1、PRODUCRT_CD2、...、PRODUCT_CD6
            product_code(data)
            #从身份证号码中 转换出年龄和星座等信息
            get_ID_NUMBER_info(data)
            #将手机号码数值化
            get_PHONE_NUMBER_info(data)

            combine_feature(data)


        if step == 4:  # 4.离散型特征数值化
            print("step4:")
            out_file = './data/output4_trans_categories.xlsx'
            if step == begin_step:  # load data
                data = pd.read_excel("./data/output3_trans_info.xlsx", sheet_name="Sheet1")
            #二值的离散特征 做map转换
            map_x(data)

            #多分类的离散特征做 one-hot转换
            for c in one_hot_columns:
                one_hot_transrow(data, c)


        if step == 5:
            print("step5:")
            out_file = './data/output5_fillna_with_zero.xlsx'
            if step == begin_step:
                data = pd.read_excel("./data/output4_trans_categories.xlsx", sheet_name="Sheet1")

            #缺失数据先补0
            for to_zero_column in to_zero_columns:
                data[to_zero_column] = data[to_zero_column].fillna(0)

            #补全'YEARS_OF_WORK'列
            data['YEARS_OF_WORK'] = data['SETUP_DATE'] + data['YEARS_OF_WORK']


        if step == 6:
            print("step6:")
            out_file = './data/output6_drop_useless_feature.xlsx'
            save_data_bak = True
            if step == begin_step:
                data = pd.read_excel("./data/output5_fillna_with_zero.xlsx", sheet_name="Sheet1")
                data_bak = pd.read_excel("./data/data_bak.xlsx", sheet_name="Sheet1")

            #合并工作位置信息
            merge_data(data, 'EMP_PROVINCE', 'BUSINESS_ADDR_PROVINCE')
            merge_data(data, 'EMP_CITY', 'BUSINESS_ADD_CITY')
            merge_data(data, 'EMP_ZONE', 'BUSINESS_ADD_ZONE')
            useless_columns = []
            useless_columns.extend(drop_columns)
            useless_columns.extend(one_hot_columns)

            #备份删除的列到data_bak.xlsx
            for c in useless_columns:
                data_bak.insert(0, c, '0')
                data_bak[c] = data[c]

            data = data.drop(useless_columns, axis=1)




        # if step == 8:
        #     print("step8:")
        #     save_data_bak = True
        #     out_file = './data/output8_deal_code_data.xlsx'
        #     if step == begin_step:
        #         data = pd.read_excel("./data/output7_mark_label.xlsx", sheet_name="Sheet1")
        #         data_bak = pd.read_excel("./data/data_bak.xlsx", sheet_name="Sheet1")
        #
        #     code_prob(data, 0.4, need_save_to_excel=True)


        if save_intermediate:
            data.to_excel(out_file)
            if save_data_bak:
                data_bak.to_excel('./data/data_bak.xlsx')

        if step == end_step:
            data.to_csv("./data/data_out/train_output.csv", encoding="utf_8_sig")

def test_data_preprocessing():
    global one_hot_columns
    global to_zero_columns
    global drop_columns
    train_data = pd.read_csv("./data/train_output.csv")
    data =  pd.read_excel("./data/test_data.xlsx")
    train_columns = train_data.columns
    # print(train_columns)

    find_month_info(data)
    product_code(data)
    get_ID_NUMBER_info(data)
    get_PHONE_NUMBER_info(data)
    mark_label(data)
    columns = data.columns

    for c in train_columns:
        if c =='Unnamed: 0': continue
        if c not in columns:
            data.insert(0, c, 0)

    map_x(data)

    #保证one-hot列和 train_data一致
    for idx, item in data.iterrows():
        for c in one_hot_columns:
            c_value = item[c]
            if str(c_value)!='nan':
                c_name = c+'_'+str(c_value)
                if c_name in train_columns:
                    data.loc[idx, c_name] = 1


    for c in to_zero_columns:
        data[c] = data[c].fillna(0)
    data['YEARS_OF_WORK'] = data['SETUP_DATE'] + data['YEARS_OF_WORK']

    merge_data(data, 'EMP_PROVINCE', 'BUSINESS_ADDR_PROVINCE')
    merge_data(data, 'EMP_CITY', 'BUSINESS_ADD_CITY')
    merge_data(data, 'EMP_ZONE', 'BUSINESS_ADD_ZONE')


    nan_columns =['ID_LAST_DATE','ID_LONG_EFFECTIVE','CERTIFICATE_LOCATION','REGISTER_ADDRESS','SOCIAL_IDENTITY','OLD_CUST_FLAG','DEGREE','EMP_STAND_FROM','BUS_ENTITY_TYPE','IS_MORTGAGE_ESTATE','ROOM_LOAN_REPAY_NUM','ROOM_LOAN_SETTLE','ROOM_VALUE','IS_MORTGAGE_CAR_LOAN','CAR_LOAN_REPAY_MONTH','CAR_LOAN_REPAY_NUM','CAR_LOAN_SETTLE','CAR_VALUE','YEAR_INCOME','MAIN_INCOME_SOURCE','FAMILY_MONTH_INCOME','FAMILY_MONTH_EXPENSE','SUPPORT_NUM','BANK_PROVINCE_NAME','BANK_CITY_NAME','IS_HAS_CREDIT_CARD','IS_HAS_LOAN','PERSONAL_CREDIR_RECORD','CREDIT_CARD_AQ','JION_LIFE_INSURANCE','APPLY_FROM_TYPE']

    useless_columns = []
    useless_columns.extend(drop_columns)
    useless_columns.extend(one_hot_columns)
    useless_columns.extend(nan_columns)

    data = data.drop(useless_columns, axis=1)

    #保证train_data 和 test_data的特征维度顺序相同
    data_out = pd.DataFrame()
    for i in range(len(train_columns)):
        c = train_columns[-(i+1)]
        if c =='Unnamed: 0': continue
        data_out.insert(0,c,0)
        data_out[c] = data[c]

    # data_out.to_excel('./data/test_output.xlsx')
    data_out.to_csv("./data/test_output.csv", encoding="utf_8_sig")

def del_noisy_data(data):
    noise_job_info = ['网商金融','网商小贷','网商金服','广州市','挺好吃刚才顾','无语','多喝点呵呵']
    for idx, item in data.iterrows():

        if (item['APP_NO'])[:8] >'20180530':
            data.drop(idx,inplace=True)
            continue
        # if (item['SCHOOL_NATURE'] =='1'):
        #     data.drop(idx, inplace=True)
        #     continue
        if (item['UNIT_NAME'] in noise_job_info):
            data.drop(idx, inplace=True)
            continue
        if (item['BUSINESS_NAME'] in noise_job_info):
            data.drop(idx, inplace=True)
            continue

def change_excel_to_csv():
    train_data = pd.read_csv("./data/train_output.csv")
    test_data = pd.read_csv("./data/test_output.csv")

    r = train_data.columns
    unwish_cloumn = ['APP_NO', 'ABODE_ZONE', 'ABODE_CITY', 'ABODE_STATE', 'REGISTER_ZONE', 'REGISTER_CITY',
                     'REGISTER_STATE']
    selected_features = ['label', 'EMP_ZONE', 'APP_LMT', 'AGE', 'MONTHLY_TURNOVER', 'YEARS_OF_WORK', 'MONTH_INCOME',
                         'PRODUCT_CD5', 'MATE_PHONE', 'LOAN_ORG_30001', 'CELLPHONE', 'ROOM_LOAN_REPAY_MONTH',
                         'MONTH_INCOME', 'HOUSE_NET_ASSET', 'MONTHLY_TURNOVER', 'APP_LMT', 'ROOM_LOAN_REPAY_MONTH',
                         'OTHER_INCOME', 'EMP_ZONE', 'EMP_CITY', 'EMP_PROVINCE', 'SALARY_INCOME', 'TOTAL_INCOME',
                         'LOAN_ORG_30003', 'PRODUCT_CD5', 'LOAN_ORG_10001', 'LOAN_ORG_30002', 'LOAN_ORG_30007',
                         'PRODUCT_CD4_等本等息', 'LOAN_PURPOSE_TRA', 'APP_TYPE', 'OCCUPATION_10', 'OCCUPATION_20',
                         'PRODUCT_CD2_自雇客户(月利率)', 'LOAN_ORG_10001', 'LOAN_ORG_30007', 'LOAN_ORG_30002',
                         'IS_LOCAL_ESTATE', 'OCCUPATION_20', 'LOAN_PURPOSE_DEC', 'LOAN_PURPOSE_BUS', '', 'EMP_POST_9.0',
                         'LOAN_PURPOSE_TRA', 'app_month_03', 'PRODUCT_CD5', 'app_month_06', 'PRODUCT_CD2_自雇客户(月利率)']
    app_features = ['label', 'app_month_05', 'app_month_11', 'app_month_09', 'app_month_12', 'app_month_10',
                    'app_month_04', 'app_month_08', 'app_month_01', 'app_month_06', 'app_month_03', 'app_month_07',
                    'app_month_02']
    print(r)

    train_data_out = pd.DataFrame()
    test_data_out = pd.DataFrame()

    for i in range(len(r)):
        r_name = r[-(i + 1)]
        if r_name not in app_features: continue
        train_data_out.insert(0, r_name, 0)
        train_data_out[r_name] = train_data[r_name]

        test_data_out.insert(0, r_name, 0)
        test_data_out[r_name] = test_data[r_name]

    train_data_out.to_csv("./preprocessed_data/train_appmon.csv", encoding="utf_8_sig")
    test_data_out.to_csv("./preprocessed_data/test_appmon.csv", encoding="utf_8_sig")

def expand_data(data, expand_col, col_value):
    col_value = ['contact_count_6month', 'call_time_active_6month', 'call_time_passive_6month', 'msg_count_6month']
    new_cols = []
    new_cols.append('APP_NO')
    new_values = np.zeros(
        ((len(data['APP_NO'].value_counts())), (len(data[expand_col].value_counts())) * len(col_value) + 1))
    for idx in data[expand_col].value_counts().index:
        for col in col_value:
            new_col = idx + '_' + col
            new_cols.append(new_col)
            print(new_col)
    df_new = pd.DataFrame(new_values, columns=new_cols)
    df_by_group = data.groupby("APP_NO")
    v_c = df_by_group.size()
    for i in range(len(v_c)):
        app_v = v_c.index[i]
        indexs = df_by_group.get_group(app_v).index
        df_new.loc[i, 'APP_NO'] = app_v
        for idx in indexs:
            expand_v = data.loc[idx, expand_col]
            for col in col_value:
                df_new.loc[i, expand_v + '_' + col] = data.loc[idx, col]

    return df_new

def merge_value(data_a, value_dict):
    data_g = data_a.groupby("APP_NO")
    cols = data_a.columns
    v_c = data_g.size()  # 类似于value_counts

    for i in range(len(v_c)):
        if v_c[i] > 1:
            app_v = v_c.index[i]
            indexs = data_g.get_group(app_v).index
            #            print(app_v, data_g.size()[app_v], indexs)
            for col in cols:  # 对于每一列的出现可能值 找出value最大的值
                max_value = ""
                max_score = 0
                if col == 'APP_NO': continue

                for idx in indexs:  # 找出value最大的值 value定义见value_dict
                    v_i = data_a.loc[idx, col]
                    if v_i in value_dict:
                        score = value_dict[v_i]
                        if score > max_score:
                            max_score = score
                            max_value = v_i

                if max_score > 0:
                    data_a.loc[indexs[0], col] = max_value  # 把最优的值赋给第一个行，后续做去重

    data_a.drop_duplicates(['APP_NO'], keep='first', inplace=True)  # keep first

def gen_new_data_file(file_out='./data/new_data.xlsx'):
    '''

    :param file_out: 保存输出的文件路径和文件名
    :return:
    '''
    pre_data = pd.read_excel("./data_all/0_results.xlsx", sheet_name="Sheet1")
    pre_APP_NO = pre_data['APP_NO'].values
    path = "./data/new_data/"
    writer = pd.ExcelWriter(file_out)
    for file in os.listdir(path):
        excel_name = path + file
        print(excel_name)

        data_i = pd.read_excel(excel_name)
        print(data_i.shape)

        data_i = data_i[data_i['APP_NO'].isin(pre_APP_NO)]

        if 'id' in data_i.columns:
            data_i.drop(['id'], axis=1, inplace=True)

        if file in drop_cols_dict:
            data_i.drop(drop_cols_dict[file], axis=1, inplace=True)

        data_i.drop(columns=data_i.loc[:, data_i.isnull().mean() > 0.85].columns, inplace=True)

        data_i = data_i.drop_duplicates()

        if file == '同盾.xlsx':
            print(data_i.columns)
            data_i = data_i.drop_duplicates(['APP_NO'], keep='first')

        if file == 'active_silence_stats.xls':
            data_i = data_i.drop_duplicates(['APP_NO'], keep='first')

        if file == 'all_contact_stats_per_month.xls':
            data_i = data_i.fillna(0).groupby(['APP_NO'], as_index=False).mean().drop(columns='month')
        if file == 'carrier_consumption_stats.xls':
            data_i = data_i.fillna(0).groupby(['APP_NO'], as_index=False).max()

        if file in ['behavior_analysis.xls', 'data_completeness.xls', 'info_match.xls']:
            merge_value(data_i, value_dicts)

        if file == 'mobile_info.xls':
            data_i = data_i[data_i.groupby(['APP_NO'])['mobile_net_age'].transform(max) == data_i[
                'mobile_net_age']].drop_duplicates(['APP_NO'], keep='last')

        if file == 't_rsp_loanee.xls':
            data_i = data_i.groupby('APP_NO', as_index=False).max()

        if file == 't_rsp_freloanbox.xls':
            data_i = data_i[data_i.groupby(['APP_NO'])['CODE'].transform(min) == data_i['CODE']]

        if file == 't_rsp_blackperson.xls':
            black_person_cols = ['APP_NO', '逾期31-60天', '逾期61-90天', '逾期91-180天', '违约', '发卡拒绝', '被执行人', '0-1000',
                                 '1000-5000',
                                 '10W以上', '2W-10W', '5000-20000', '无', 'IP存在欺诈风险']

            black_person_dict = {'逾期31-60天': 1, '逾期61-90天': 2, '逾期91-180天': 3, '违约': 4, '发卡拒绝': 5, '被执行人': 6,
                                 '0-1000': 7, '1000-5000': 8,
                                 '10W以上': 9, '2W-10W': 10, '5000-20000': 11, '无': 12}

            black_person_values = []
            new_values = np.zeros(((len(data_i['APP_NO'].value_counts())), len(black_person_cols)))

            df_new = pd.DataFrame(new_values, columns=black_person_cols)

            df_by_group = data_i.groupby("APP_NO")
            v_c = df_by_group.size()
            for i in range(len(v_c)):
                app_v = v_c.index[i]
                indexs = df_by_group.get_group(app_v).index
                df_new.loc[i, 'APP_NO'] = app_v
                for idx in indexs:
                    bal_v = data_i.loc[idx, 'bal']
                    if str(bal_v) != 'nan':
                        df_new.loc[i, bal_v] = +1

                    des_v = data_i.loc[idx, 'des']
                    if str(des_v) != 'nan':
                        df_new.loc[i, des_v] = +1

            data_i = df_new

        if file == 'city2.xlsx':
            # 保留最频繁通话城市数据
            data_temp = data_i[
                data_i.groupby(['APP_NO'])['call_area_seq_no'].transform(min) == data_i['call_area_seq_no']]
            # 存在重复查询数据，保留各列最大取值数据
            data_temp = data_temp.groupby('APP_NO', as_index=False).max()
            # 新增列 存储用户频繁通话城市数量
            data_temp.insert(0, 'city_counts', 0)
            data_temp['city_counts'] = (data_i.groupby(['APP_NO'])['call_area_seq_no'].max()).values

            data_i = data_temp

        if file in expand_file_dict:
            data_i = data_i.fillna(0).groupby(['APP_NO', expand_file_dict[file]], as_index=False).max()
            data_i = expand_data(data_i, expand_file_dict[file], col_value)

        print(data_i.shape)
        data_i.to_excel(writer, file[:-4], index=False)

        APP_NO.extend(data_i['APP_NO'])
    writer.save()
    print(len(APP_NO))
    new_APP_NO = list(set(APP_NO))
    print(len(new_APP_NO))

'''
steps:
    1.对所有sheet页做全连接，去除噪声数据，划分训练集和测试集
    2.去除全nan数据列
    3.拆分PRODUCT_CD列，身份证信息转换 年龄星座,电话号码 转换 估值
    4.离散型特征数值化
    5.补充缺失值
    6.删除部分无用信息特征
    7.从逾期数据中补充标签
    8.地理编码分箱
    9.数值型数据处理
'''


