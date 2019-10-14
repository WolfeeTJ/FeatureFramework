# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 15:17:54 2018

@author: Guang Du
"""

import FeatureFrameworkLib.ff_main_offline_production as ff
import pandas as pd


#TODO: 由于离线分析生成的数据字典中仅保存 最近几个月 信息，不保存base month，所以对于最外层流程来说，需要传base month进入流程
base_month=12

# 读取配置信息和元数据、数据文件
configfile = pd.read_table("conf/config-filter-data.txt")
df_result = pd.DataFrame([])
for i in range(0, len(configfile)):
    datasource = configfile.iloc[i]["datasource"]
    key_col = configfile.iloc[i]["key_col"]
    month_col = configfile.iloc[i]["month_col"]
    month_start = configfile.iloc[i]["month_start"]
    month_end = configfile.iloc[i]["month_end"]
    in_where = configfile.iloc[i]["where"]
    df_result_tmp = ff.ff_main_offline_production(datasource, datasource + ".dic", key_col, base_month)
    print("文件： " + datasource)
    print(df_result_tmp)

    df_result_tmp.to_csv("data/" + datasource + "_offline_production.out")

# df_result
df_result_t = df_result_tmp.T
