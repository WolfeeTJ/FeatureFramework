# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 15:17:54 2018

@author: Guang Du
"""

import CreditLife_FeatureFramework.ff_main_checkfile as ff
import pandas as pd

# 读取配置信息和元数据、数据文件
configfile = pd.read_table("conf/config.txt")
configfile
metafile = pd.read_table("conf/meta.txt")
metafile

df_month_str_stat = pd.DataFrame([])
df_num_check_result = pd.DataFrame([])
df_char_check_result = pd.DataFrame([])
for i in range(0, len(configfile)):
    filebasename = configfile.iloc[i]["datasource"]
    month_start = configfile.iloc[i]["start_month"]
    month_end = configfile.iloc[i]["end_month"]
    customer_col = configfile.iloc[i]["key_col"]
    month_col = configfile.iloc[i]["month_col"]

    datasource_metadata = metafile.query("filename=='" + filebasename + "'")
    num_vars = datasource_metadata.query("var_type=='Numbers'")["var_name"]
    char_vars = datasource_metadata.query("var_type=='String'")["var_name"]

    df_month_str_stat_tmp, df_num_check_result_tmp, df_char_check_result_tmp = ff.ff_check_file(filebasename,
                                                                                                customer_col, month_col,
                                                                                                month_start, month_end,
                                                                                                num_vars, char_vars)
    df_month_str_stat = pd.concat([df_month_str_stat, df_month_str_stat_tmp], ignore_index=True)
    df_num_check_result = pd.concat([df_num_check_result, df_num_check_result_tmp], ignore_index=True)
    df_char_check_result = pd.concat([df_char_check_result, df_char_check_result_tmp], ignore_index=True)

df_month_str_stat = df_month_str_stat.reindex(['filename', 'month_str', 'month_str_count'], axis="columns")
df_num_check_result = df_num_check_result.reindex(
    ['filename', 'var_name', 'cnt', 'cnt_isna', 'pct_isna', 'sum', 'min', 'max', 'avg', 'med', 'P25_num', 'P50_num',
     'P75_num', 'P90_num', 'P95_num', 'P25_pct', 'P50_pct', 'P75_pct', 'P90_pct', 'P95_pct'], axis="columns")
df_char_check_result = df_char_check_result.reindex(["filename", "var_name", "var_value", "cnt", "pct"], axis="columns")

print(df_month_str_stat)
print(df_num_check_result)
print(df_char_check_result)
