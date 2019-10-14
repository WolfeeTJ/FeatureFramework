
import pandas as pd

df_pboc_priority_conf=pd.DataFrame({"priority":[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], "cat": ["Z", "D", "G", "7", "6", "5", "4", "3", "2", "1", "N", "*", "C", "#", "/"]})

def replace_rh_to_priority(in_series):
    s=in_series.copy()
    s.name="in_str"
    df=s.to_frame()
    df=df.merge(df_pboc_priority_conf, how="left", left_on="in_str", right_on="cat")
    return df["priority"]


def pboc_24_overdue_summary(in_dataframe):
    df_pboc_priorities = df_a.apply(replace_rh_to_priority)
    df_pboc_priorities["min_priority"]=df_pboc_priorities.min(axis=1)
    df_pboc_priorities = df_pboc_priorities.merge(df_pboc_priority_conf, how="left", left_on="min_priority", right_on="priority")
    str_overdue_summary = df_pboc_priorities["cat"].str.cat()
    return str_overdue_summary


s1="1234567"
s2="ZDGN*C/"
s3="6666666"
df_a=pd.DataFrame({"s1":list(s1), "s2": list(s2), "s3": list(s3)})
print(pboc_24_overdue_summary(df_a))