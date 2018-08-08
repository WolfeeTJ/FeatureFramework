# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 15:44:42 2018

@author: Guang Du
"""
from CreditLife_FeatureFramework.ff_funcs import *
import pandas as pd
import numpy as np
import ast


def ff_main_offline_production(in_datasource_name, in_data_dic_name, in_key_column_name):

    # 读取配置信息和元数据、数据文件
    def conv_func(x):
        if x is None or x == "":
            return np.nan
        else:
            return ast.literal_eval(x)

    configfile = pd.read_csv("data/" + in_data_dic_name, converters={"denominator": conv_func, "numerator": conv_func})

    df_filter = pd.read_table("data/" + in_datasource_name + ".txt")

    df_result = df_filter[in_key_column_name].drop_duplicates().to_frame()
    df_result.index = df_result[in_key_column_name]

    for i in range(1, len(configfile)):
        # print("processing " + str(configfile.iloc[i]))
        func_to_call = eval(configfile.iloc[i]["module"])
        if configfile.iloc[i]["module"] == "ff_combination_pct":
            dic_dummy, df_filter[configfile.iloc[i]["var_name"]] = func_to_call(df_filter, configfile.iloc[i])
        elif configfile.iloc[i]["module"] == "ff_customized_var":
            dic_dummy, df_result[configfile.iloc[i]["var_name"]] = func_to_call(df_result, configfile.iloc[i])
        else:
            dic_dummy, df_result[configfile.iloc[i]["var_name"]] = func_to_call(df_filter, configfile.iloc[i])

    return  df_result