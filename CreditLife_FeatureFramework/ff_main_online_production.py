# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 15:17:54 2018

@author: Guang Du
"""
import json
import jsonpath
import pandas as pd
from CreditLife_FeatureFramework.ff_funcs import *

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def ff_main_online_production(in_body_json, dic_config, in_base_month):
    try:

        dicconfigname = dic_config["data_dic_name"]
        dicconfig = pd.read_csv("conf/" + dicconfigname + ".dic")

        dicconfig["base_month"] = in_base_month

        dataframe_json_path_str=dic_config["dataframe_json_path"]
        json_datanode = jsonpath.jsonpath(in_body_json, dataframe_json_path_str)
        df_json_dataframe = pd.io.json.json_normalize(json_datanode)

        key_column_name=dic_config["key_column_name"]

        key_json_path_str=dic_config["key_json_path"]
        if type(key_json_path_str) not in [float, np.float64]:
            key_column_value = jsonpath.jsonpath(in_body_json, key_json_path_str)[0]
            df_json_dataframe[key_column_name] = key_column_value

        df_result = df_json_dataframe[key_column_name].drop_duplicates().to_frame()
        df_result.index = df_result[key_column_name]

        list_single_number_funcs = ['ff_common_stat', 'ff_period_compare_stat', 'ff_continue_gt_N',
                                    'ff_continue_inc_gt_N', 'ff_bin_distribution_by_loc', 'ff_bin_distribution_by_val']
        for i in range(0, len(dicconfig)):
            # print("processing " + str(configfile.iloc[i]))
            func_to_call = eval(dicconfig.iloc[i]["module"])
            if dicconfig.iloc[i]["module"] == "ff_combination_pct":
                dic_dummy, df_json_dataframe[dicconfig.iloc[i]["var_name"]] = func_to_call(df_json_dataframe, dicconfig.iloc[i])
            elif dicconfig.iloc[i]["module"] == "ff_customized_var":
                dic_dummy, df_result[dicconfig.iloc[i]["var_name"]] = func_to_call(df_result, dicconfig.iloc[i])
            else:
                if dicconfig.iloc[i]["module"] in list_single_number_funcs:
                    df_json_dataframe[dicconfig.iloc[i]["value_column"]] = df_json_dataframe[
                        dicconfig.iloc[i]["value_column"]].astype(float)
                dic_dummy, df_result[dicconfig.iloc[i]["var_name"]] = func_to_call(df_json_dataframe, dicconfig.iloc[i])

        return df_result
    except Exception as e:
        print("Exception:", e)
