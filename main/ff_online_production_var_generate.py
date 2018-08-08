# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 15:17:54 2018

@author: Guang Du
"""
import json
import jsonpath
import pandas as pd
from CreditLife_FeatureFramework.ff_funcs import *

def process_online_vars(in_body_json):

    try:
        print("process_online_vars")
        json_datanode = jsonpath.jsonpath(in_body_json, '$.data[*]')
        print("var_a: " +str(type(json_datanode)) + str(json_datanode))
        print("var_a[0]: " +str(type(json_datanode[0])) + str(json_datanode[0]))
        configfile = pd.read_csv("conf/config-online-prd.csv")
        print("config is:")
        print(configfile)
        df_filter=pd.io.json.json_normalize(json_datanode)
        json_seqNo = jsonpath.jsonpath(in_body_json, '$.seqNo')[0]
        df_filter["seqNo"] = json_seqNo

        df_result = df_filter["seqNo"].drop_duplicates().to_frame()
        df_result.index = df_result["seqNo"]
        print("initial result is: " + str(df_result))

        list_single_number_funcs=['ff_common_stat','ff_period_compare_stat','ff_continue_gt_N','ff_continue_inc_gt_N','ff_bin_distribution_by_loc','ff_bin_distribution_by_val']
        for i in range(0, len(configfile)):
            # print("processing " + str(configfile.iloc[i]))
            func_to_call = eval(configfile.iloc[i]["module"])
            if configfile.iloc[i]["module"] == "ff_combination_pct":
                print("calling ff_combination_pct")
                dic_dummy, df_filter[configfile.iloc[i]["var_name"]] = func_to_call(df_filter, configfile.iloc[i])
            elif configfile.iloc[i]["module"] == "ff_customized_var":
                print("calling ff_customized_var")
                dic_dummy, df_result[configfile.iloc[i]["var_name"]] = func_to_call(df_result, configfile.iloc[i])
            else:
                if configfile.iloc[i]["module"] in list_single_number_funcs:
                    df_filter[configfile.iloc[i]["value_column"]] = df_filter[configfile.iloc[i]["value_column"]].astype(float)
                print("calling " + configfile.iloc[i]["module"])
                dic_dummy, df_result[configfile.iloc[i]["var_name"]] = func_to_call(df_filter, configfile.iloc[i])

        print(str(df_result))
    except BaseException as e:
        print(e)
