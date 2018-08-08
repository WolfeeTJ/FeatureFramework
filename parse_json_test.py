# -*- coding: utf-8 -*-

json_file=""
with open("data/法院黑名单.json", "r", encoding="utf8") as f:
    json_file=f.read()

json_file
import json
import jsonpath
import conf.online_config as cf
import pandas as pd
jobj=json.loads(json_file)
jobj
len(jsonpath.jsonpath(jobj, '$.data[*].case_no'))
j1=jsonpath.jsonpath(jobj, '$.data[*]')
j1
d1=pd.read_json(json.dumps(j1))
d2=pd.io.json.json_normalize(j1)
dic_result={}
pd.read

for k in cf.court_blacklist_conf.keys():
    print(k)
    dic_result[k]=jsonpath.jsonpath(jobj, cf.court_blacklist_conf[k])

print(dic_result)


print("process_online_vars")
json_datanode = jsonpath.jsonpath(jobj, '$.data[*]')
json_datanode
print("var_a: " +str(type(json_datanode)) + str(json_datanode))
print("var_a[0]: " +str(type(json_datanode[0])) + str(json_datanode[0]))
configfile = pd.read_csv("conf/config-online-prd.csv")
print("config is:")
print(configfile)
df_filter=pd.io.json.json_normalize(json_datanode)
jsonpath.jsonpath(jobj, '$.seqNo')
json_seqNo = jsonpath.jsonpath(jobj, '$.seqNo')
json_seqNo
df_filter["seqNo"] = json_seqNo
df_filter
df_result = df_filter["seqNo"].drop_duplicates().to_frame()
df_result.index = df_result["seqNo"]
print("initial result is: " + str(df_result))

for i in range(1, len(configfile)):
    # print("processing " + str(configfile.iloc[i]))
    func_to_call = eval(configfile.iloc[i]["module"])
    if configfile.iloc[i]["module"] == "ff_combination_pct":
        dic_dummy, df_filter[configfile.iloc[i]["var_name"]] = func_to_call(df_filter, configfile.iloc[i])
    elif configfile.iloc[i]["module"] == "ff_customized_var":
        dic_dummy, df_result[configfile.iloc[i]["var_name"]] = func_to_call(df_result, configfile.iloc[i])
    else:
        dic_dummy, df_result[configfile.iloc[i]["var_name"]] = func_to_call(df_filter, configfile.iloc[i])

print(str(df_result))

