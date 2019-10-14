# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 15:17:54 2018

@author: Guang Du
"""

import FeatureFrameworkLib.ff_main_online_production as ff
import pandas as pd
import jsonpath
import json

def ff_online_process(in_json_obj):
    configfile = pd.read_csv("conf/config-online-json-parser.csv")

    list_workflow_step = jsonpath.jsonpath(in_json_obj, '$.workflow_step')
    if False == list_workflow_step:
        raise Exception("Error parsing workflow step")
    workflow_step = list_workflow_step[0]

    # TODO: 从json文件中推断base month？从配置文件读取base month？审批传进base month？
    base_month=12

    configfile_filtered=configfile.query("workflow_step == " + workflow_step)

    key_str = jsonpath.jsonpath(in_json_obj, configfile_filtered.iloc[0]["key_json_path"])[0]
    key_column_name = configfile_filtered.iloc[0]["key_column_name"]
    df_result = pd.DataFrame({key_column_name: [key_str]})
    df_result.index = df_result[key_column_name]

    for i in range(0, len(configfile_filtered)):
        dic_config = configfile_filtered.iloc[i].to_dict()
        df_result_tmp = ff.ff_main_online_production(in_json_obj, dic_config, base_month)
        df_result = df_result.merge(df_result_tmp, left_on=key_column_name, right_on=dic_config["key_column_name"])

    return df_result