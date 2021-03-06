# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 15:44:42 2018

@author: Guang Du
"""
import FeatureFrameworkLib.ff_funcs as ff
import pandas as pd
import numpy as np
import datetime
import ast


def ff_check_file(filebasename, customer_col, month_col, month_start, month_end, num_vars, char_vars):
    datafile = pd.read_table(filebasename + ".txt")

    # 检查数据缺失情况
    file_customers = datafile[customer_col].drop_duplicates()
    df_customers = pd.DataFrame({"dummy": 1, customer_col: file_customers})

    df_month = pd.DataFrame({"dummy": 1, "all_months": range(month_start, month_end + 1)})

    df_valid_cus_month = df_customers.merge(df_month, on="dummy")

    df_joined = df_valid_cus_month.merge(datafile[[customer_col, month_col]], how="left",
                                         left_on=[customer_col, "all_months"], right_on=[customer_col, month_col],
                                         validate="one_to_one")

    df_joined[month_col + "_isna"] = df_joined.isna()[month_col].astype(int).astype(str)
    df_month_str = df_joined.groupby(customer_col)[month_col + "_isna"].agg(lambda x: x.str.cat())
    s_month_str_stat = df_month_str.value_counts()
    df_month_str_stat = pd.DataFrame(
        {"month_str": s_month_str_stat.index.values, "month_str_count": s_month_str_stat, "filename": filebasename})
    #    print(df_month_str_stat)

    # 数据分布情况

    # 数据类型字段检查
    df_num_check_result = pd.DataFrame([])
    for col in set(num_vars) - set([customer_col, month_col]):
        df_num_check_common_stat_result_tmp = ff.ff_check_num_total_stat(datafile, col)
        df_num_check_val_pct_result_tmp = ff.ff_check_total_bin_distribution_by_val(datafile, col)
        df_num_check_result_tmp = df_num_check_common_stat_result_tmp.merge(df_num_check_val_pct_result_tmp,
                                                                            left_index=True, right_index=True)
        df_num_check_result_tmp["var_name"] = col
        df_num_check_result = pd.concat([df_num_check_result, df_num_check_result_tmp], ignore_index=True)
    df_num_check_result = df_num_check_result.sort_values(by=["var_name"])
    df_num_check_result["filename"] = filebasename
    #    print(df_num_check_result)

    # 字符类型字段检查
    df_char_check_result = pd.DataFrame([])
    for col in set(char_vars) - set([customer_col, month_col]):
        df_check_category_result = ff.ff_check_category_cnt_pct(datafile, col)
        df_check_category_result["var_name"] = col
        df_check_category_result = df_check_category_result.sort_values(by=["var_name", "cnt"], ascending=[True, False])
        df_char_check_result = pd.concat([df_char_check_result, df_check_category_result])
    df_char_check_result["filename"] = filebasename
    #    print(df_char_check_result)

    return (df_month_str_stat, df_num_check_result, df_char_check_result)

