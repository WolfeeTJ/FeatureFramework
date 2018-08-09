# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 15:44:42 2018

@author: Guang Du
"""
import CreditLife_FeatureFramework.ff_funcs as ff
import pandas as pd
import numpy as np
import datetime
import ast


in_datasource_name="MD_MTH_DATA"
in_key_column="customer"
in_month_column="month_nbr"
in_start_month=1
in_end_month=12


in_dataframe = pd.read_table("data/" + in_datasource_name + ".txt")

# 1.指定时间周期（输入项1）（1）最近N个周期内
df_filter = in_dataframe.query(
    in_month_column + " >= " + str(in_start_month) + " and " + in_month_column + " <=" + str(in_end_month))
df_filter

conf_pct = pd.read_table("conf/config-var-gen-pct.txt")
conf_var_gen_base_stat = pd.read_table("conf/config-var-gen-base-stat.txt")
conf_var_gen_continue_stat = pd.read_table("conf/config-var-gen-continue-stat.txt")
conf_var_gen_bin_stat = pd.read_table("conf/config-var-gen-bin-stat.txt")
conf_var_gen_char_stat = pd.read_table("conf/config-var-gen-char-stat.txt")

df_conf_pct = conf_pct.query("datasource == '" + in_datasource_name + "'")

dic_start_id = 1
dic_result = {0:{"module":"placeholder"}}
df_result_var_pct = df_filter.copy()
for i_df_conf_pct in range(0, len(df_conf_pct)):
    var_combinations = df_conf_pct.iloc[i_df_conf_pct]["var_combinations"]
    month_combinations = df_conf_pct.iloc[i_df_conf_pct]["month_combinations"]
    month_start = df_conf_pct.iloc[i_df_conf_pct]["month_start"]
    month_end = df_conf_pct.iloc[i_df_conf_pct]["month_end"]
    s_var_combination = pd.Series(var_combinations.split(","))
    list_var_combination = s_var_combination.apply(lambda x: x.strip()).tolist()
    s_month_combinations = pd.Series(month_combinations.split(","))
    list_month_combinations = s_month_combinations.apply(lambda x: x.strip()).astype(int).tolist()
    dic_func_pars = df_conf_pct.iloc[i_df_conf_pct].to_dict()
    for var_month in list_month_combinations:
        dic_start_id = max(dic_result.keys()) + 1
        dic_func_pars["N_Months"] = var_month
        dic_func_pars["key_column"] = in_key_column
        dic_func_pars["month_column"] = in_month_column
        dic_func_pars["in_pct_column_lists"] = list_var_combination
        dic_func_pars["in_dic_start_id"] = dic_start_id
        dic_combination_dic, df_pct_var = ff.ff_combination_pct(df_filter, dic_func_pars)
        df_result_var_pct = df_result_var_pct.merge(df_pct_var, how="left", left_index=True, right_index=True,
                                                    validate="one_to_one")
        dic_result.update(dic_combination_dic)
df_result_var_pct = df_result_var_pct.query(
    in_month_column + " >= " + str(month_start) + " and " + in_month_column + " <=" + str(month_end))
print(dic_result)
print(df_result_var_pct)

# dic_result = dict({0: {"dataset": in_datasource_name}})
df_conf_var_gen_base_stat = conf_var_gen_base_stat.query("datasource == '" + in_datasource_name + "'")
i_df_conf_var_gen_base_stat=0
var_name = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat]["var_name"]
month_combinations = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat]["month_combinations"]
month_start = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat]["month_start"]
month_end = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat]["month_end"]
s_month_combinations = pd.Series(month_combinations.split(","))
list_month_combinations = s_month_combinations.apply(lambda x: x.strip()).astype(int).tolist()
dic_func_pars = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat].to_dict()
dic_func_pars["key_column"] = in_key_column
dic_func_pars["month_column"] = in_month_column
dic_func_pars["value_column"] = var_name
dic_func_pars["base_month"] = month_end
dic_func_pars["month_start"] = month_start
dic_func_pars["month_end"] = month_end
var_month=3
dic_func_pars["N_Months"] = var_month

dic_start_id = max(dic_result.keys()) + 1
dic_func_pars["in_dic_start_id"] = 100

dic_func_pars_rolling = dic_func_pars.copy()
dic_func_pars_rolling["is_rolling"]=True

# dic_common_stat, df_common_stat = ff.ff_common_stat(df_filter, dic_func_pars)
dic_common_stat, df_common_stat_rolling = ff.ff_common_stat_rolling(df_filter, dic_func_pars_rolling)

df_result = df_result_var_pct.merge(df_common_stat_rolling, how="outer", on=[in_key_column, in_month_column], validate="one_to_one")
dic_result.update(dic_common_stat)

dic_start_id = max(dic_result.keys()) + 1
dic_func_pars["in_dic_start_id"] = dic_start_id
dic_period_stat, df_period_stat = ff.ff_period_compare_stat(df_filter, dic_func_pars)
df_result = df_result.merge(df_period_stat, how="outer", on=[in_key_column],
                            validate="one_to_one")
dic_result.update(dic_period_stat)
df_result.index = df_result[in_key_column]
print(dic_result)
print(df_result)

df_result_t = df_result.T
print(df_result)


