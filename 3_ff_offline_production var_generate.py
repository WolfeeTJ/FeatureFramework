# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 15:17:54 2018

@author: Guang Du
"""

#import CreditLife_FeatureFramework.ff_main as ff
from CreditLife_FeatureFramework.ff_funcs import *
import pandas as pd
import numpy as np
import json
import ast

# 读取配置信息和元数据、数据文件
def conv_func(x):
    if x is None or x == "":
        return np.nan
    else:
        return ast.literal_eval(x)

configfile = pd.read_csv("data/MD_MTH_DATA.dic", converters={"denominator": conv_func, "numerator": conv_func})

configfile

configfile.iloc[1].to_dict()

configfile.iloc[1].dtypes
configfile.dtypes

type(configfile.iloc[1].to_dict()["denominator"])


func_to_call = eval(configfile.iloc[1]["module"])
# func_to_call(configfile.iloc[1])
df_filter = pd.read_table("data/MD_MTH_DATA.txt")

configfile.iloc[1]
dic_combination_dic, df_pct_var = ff_combination_pct(df_filter, configfile.iloc[4])
dic_combination_dic, df_common_stat = ff_common_stat(df_filter, configfile.iloc[24])
dic_combination_dic, df_period_compare_stat = ff_period_compare_stat(df_filter, configfile.iloc[20])

for i in range(0,len(configfile)):
   print(configfile.iloc[i]["module"])
   func_to_call = eval(configfile.iloc[i]["module"])
   func_to_call(configfile.iloc[i])



type(ast.literal_eval("['a']"))
