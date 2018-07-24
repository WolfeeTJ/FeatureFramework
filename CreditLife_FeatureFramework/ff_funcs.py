# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 15:44:42 2018

@author: Guang Du
"""

import pandas as pd;
import numpy as np;


# 数值型字段一般统计报告
def ff_check_num_total_stat(in_dataframe, in_stat_column):
    def cnt_isna(x):
        if x.size == 0:
            return 0;
        else:
            return x[x.isna()].size;

    def pct_isna(x):
        if x.size == 0:
            return 0;
        else:
            return x[x.isna()].size / x.size;

    agg_funcs = [("cnt", np.size),
                 ("sum", np.sum),
                 ("min", np.min),
                 ("max", np.max),
                 ("avg", np.mean),
                 ("med", np.median),
                 ("cnt_isna", cnt_isna),
                 ("pct_isna", pct_isna)
                 ]
    df_result = in_dataframe[in_stat_column].groupby(by=lambda x: 1).agg(agg_funcs)
    return df_result


# 字符型字段数据分布报告
def ff_check_category_cnt_pct(in_dataframe, in_stat_column_name):
    pd_dummy = pd.get_dummies(in_dataframe[in_stat_column_name], dummy_na=True)
    pd_dummy_groupby = pd_dummy.groupby(lambda x: 1)
    pd_dummy_result_cnt = pd_dummy_groupby.sum()
    pd_dummy_result_pct = pd_dummy_groupby.mean()
    pd_dummy_result_cnt_t = pd_dummy_result_cnt.T.rename(columns={1: "cnt"})
    pd_dummy_result_pct_t = pd_dummy_result_pct.T.rename(columns={1: "pct"})
    pd_dummy_result = pd_dummy_result_cnt_t.merge(pd_dummy_result_pct_t, left_index=True, right_index=True)
    pd_dummy_result["var_value"] = pd_dummy_result.index.values
    return pd_dummy_result


# 数据分布报告(25/50/75/90/95分位)
def ff_check_total_bin_distribution_by_val(in_dataframe, in_bin_value_column):
    # 对统计变量范围做百分位统计，计算大于各百分位的数量
    dfsort = in_dataframe.sort_values(by=in_bin_value_column)
    dfsort_range_val = dfsort[in_bin_value_column].max() - dfsort[in_bin_value_column].min()
    dfsort_bins_val_pct = [.25, .5, .75, .9, .95]
    dfsort_valbins = (pd.Series(dfsort_bins_val_pct) * dfsort_range_val) + dfsort[in_bin_value_column].min()
    dfsort["bin_val"] = np.digitize(dfsort[in_bin_value_column], dfsort_valbins, right=False)
    df_groupby_bin_val = dfsort["bin_val"].groupby(lambda x: 1)
    agg_funcs = []
    for i in range(0, len(dfsort_bins_val_pct)):
        agg_funcs += [
            ("P" + str(int(dfsort_bins_val_pct[i] * 100)) + "_pct", lambda x, y=i: np.sum(x > y) / np.size(x))]
    df_result_bin_val = df_groupby_bin_val.agg(agg_funcs)
    for i in range(0, len(dfsort_valbins)):
        df_result_bin_val["P" + str(int(dfsort_bins_val_pct[i] * 100)) + "_num"] = dfsort_valbins.iloc[i]
    return df_result_bin_val


# （2）百分比衍生：（多变量）
# 分子（变量A+变量B+……）/分母（变量A+变量B+变量C……）（分子、分母指定各自的变量范围，作排列组合，不输出分子项等于分母项的结果）

def ff_combination_pct(in_dataframe, in_key_column_name, in_time_interval_column, in_pct_column_lists,
                       in_rolling_months, in_dic_start_id):
    # 排列组合分子
    column_combinations = []
    import itertools
    for i in range(len(in_pct_column_lists) - 1):
        for j in itertools.combinations(in_pct_column_lists, i + 1):
            column_combinations += [j]
    # 排列组合分母
    column_frame = pd.DataFrame({"l": column_combinations})
    column_frame["r"] = column_frame["l"].map(lambda x: tuple(set(in_pct_column_lists) - set(x)))
    dict_result_dic = dict()
    i_dict_key_id = in_dic_start_id
    column_result = set()
    for l in range(column_frame.index.size):
        for i in range(len(column_frame.at[l, "r"]) - 1):
            for j in itertools.combinations(column_frame.at[l, "r"], i + 1):
                column_result.add((column_frame.at[l, "l"], column_frame.at[l, "l"] + j))
                dict_result_dic[i_dict_key_id] = "+".join(column_frame.at[l, "l"]) + "/" + "+".join(
                    column_frame.at[l, "l"] + j) + "_L" + str(in_rolling_months)
                i_dict_key_id += 1

    df_pct_var = in_dataframe.groupby(in_key_column_name)[in_pct_column_lists + [in_time_interval_column]].rolling(
        in_rolling_months, on=in_time_interval_column).sum()
    # df_pct_var
    for tup in column_result:
        numerator = list(tup[0])
        numerator.sort()
        denominator = list(tup[1])
        denominator.sort()
        df_pct_var["+".join(numerator) + "/" + "+".join(denominator) + "_L" + str(in_rolling_months)] = df_pct_var[list(
            tup[0])].sum(axis=1) / df_pct_var[list(tup[1])].sum(axis=1)

    set_result_columns = set(df_pct_var.columns)
    set_result_columns -= set(in_pct_column_lists)
    df_pct_var = df_pct_var.reindex(columns=list(set_result_columns))

    return (dict_result_dic, df_pct_var)


# 基础统计、各时段前后段环比统计
def ff_common_stat(in_dataframe, in_key_column_name, in_time_interval_column_name, in_stat_column_name, in_end_month,
                   in_N_month, in_dic_start_id):
    def cnt_gt_0(x):
        return x[x > 0].size

    def pct_gt_0(x):
        if x.size == 0:
            return 0
        else:
            return x[x > 0].size / x.size

    def cnt_isna(x):
        if x.size == 0:
            return 0
        else:
            return x[x.isna()].size

    def pct_isna(x):
        if x.size == 0:
            return 0
        else:
            return x[x.isna()].size / x.size

    agg_funcs = [(in_stat_column_name + "_cnt_L" + str(in_N_month), np.size),
                 (in_stat_column_name + "_cnt_gt_0_L" + str(in_N_month), cnt_gt_0),
                 (in_stat_column_name + "_pct_gt_0_L" + str(in_N_month), pct_gt_0),
                 (in_stat_column_name + "_sum_L" + str(in_N_month), np.sum),
                 (in_stat_column_name + "_min_L" + str(in_N_month), np.min),
                 (in_stat_column_name + "_max_L" + str(in_N_month), np.max),
                 (in_stat_column_name + "_avg_L" + str(in_N_month), np.average),
                 (in_stat_column_name + "_med_L" + str(in_N_month), np.median),
                 (in_stat_column_name + "_cnt_isna_L" + str(in_N_month), cnt_isna),
                 (in_stat_column_name + "_pct_isna_L" + str(in_N_month), pct_isna)
                 ]

    cond_filter_months = in_time_interval_column_name + " <= " + str(
        in_end_month) + " and " + in_time_interval_column_name + " >= " + str(
        in_end_month - in_N_month + 1)
    in_df_N_months = in_dataframe.copy().query(cond_filter_months)
    df_result = in_df_N_months.groupby(in_key_column_name)[in_stat_column_name].agg(agg_funcs)

    # 过去月份变量对比
    if (in_N_month <= 0):
        raise Exception("parameter last N months should > 0")
    elif (in_N_month > 1):
        minus_last_mth = 1
        minus_1st_half_mths_start = in_N_month - 1
        if ((in_N_month % 2) == 0):
            minus_2nd_half_mths_start = in_N_month / 2 - 1
        else:
            minus_2nd_half_mths_start = (in_N_month + 1) / 2 - 1
        minus_1st_half_mths_end = minus_2nd_half_mths_start + 1
    else:  # in_N_month==1
        minus_last_mth = 0
        minus_1st_half_mths_start = 0
        minus_2nd_half_mths_start = 0
        minus_1st_half_mths_end = 0

    df_curr_mth = in_df_N_months.query(in_time_interval_column_name + " == " + str(in_end_month))
    df_last_mth = in_df_N_months.query(in_time_interval_column_name + " == " + str(in_end_month - minus_last_mth))
    df_1st_half_mths = in_df_N_months.query(
        in_time_interval_column_name + " >= " + str(
            in_end_month - minus_1st_half_mths_start) + " and " + in_time_interval_column_name + " <=" + str(
            in_end_month - minus_1st_half_mths_end))
    df_2nd_half_mths = in_df_N_months.query(
        in_time_interval_column_name + " >= " + str(
            in_end_month - minus_2nd_half_mths_start) + " and " + in_time_interval_column_name + " <=" + str(
            in_end_month))

    agg_funcs = [("#sum", np.sum), ("#max", np.max), ("#avg", np.mean)]
    df_curr_mth_agg = df_curr_mth.groupby(in_key_column_name)[in_stat_column_name].agg(agg_funcs)
    df_last_mth_agg = df_last_mth.groupby(in_key_column_name)[in_stat_column_name].agg(agg_funcs)
    df_1st_half_mths_agg = df_1st_half_mths.groupby(in_key_column_name)[in_stat_column_name].agg(agg_funcs)
    df_2nd_half_mths_agg = df_2nd_half_mths.groupby(in_key_column_name)[in_stat_column_name].agg(agg_funcs)
    df_all_agg = in_df_N_months.groupby(in_key_column_name)[in_stat_column_name].agg(agg_funcs)

    df_agg1 = df_all_agg.merge(df_curr_mth_agg, how="left", on=in_key_column_name, validate="one_to_one",
                               suffixes=["", "_curr"])
    df_agg2 = df_agg1.merge(df_last_mth_agg, how="left", on=in_key_column_name, validate="one_to_one",
                            suffixes=["", "_last"])
    df_agg3 = df_agg2.merge(df_1st_half_mths_agg, how="left", on=in_key_column_name, validate="one_to_one",
                            suffixes=["", "_1st_half"])
    df_agg = df_agg3.merge(df_2nd_half_mths_agg, how="left", on=in_key_column_name, validate="one_to_one",
                           suffixes=["", "_2nd_half"])

    # special additional cases
    df_1st_half_mths_special = pd.DataFrame([])
    df_2nd_half_mths_special = pd.DataFrame([])
    if (in_N_month == 3):
        df_1st_half_mths_special = in_df_N_months.query(
            in_time_interval_column_name + " >= " + str(
                in_end_month - 2) + " and " + in_time_interval_column_name + " <=" + str(in_end_month - 1))
        df_2nd_half_mths_special = in_df_N_months.query(in_time_interval_column_name + " == " + str(in_end_month))
    elif (in_N_month == 9):
        df_1st_half_mths_special = in_df_N_months.query(
            in_time_interval_column_name + " >= " + str(
                in_end_month - 8) + " and " + in_time_interval_column_name + " <=" + str(in_end_month - 3))
        df_2nd_half_mths_special = in_df_N_months.query(
            in_time_interval_column_name + " >= " + str(
                in_end_month - 2) + " and " + in_time_interval_column_name + " <=" + str(in_end_month))
    elif (in_N_month == 12):
        df_1st_half_mths_special = in_df_N_months.query(
            in_time_interval_column_name + " >= " + str(
                in_end_month - 11) + " and " + in_time_interval_column_name + " <=" + str(in_end_month - 3))
        df_2nd_half_mths_special = in_df_N_months.query(
            in_time_interval_column_name + " >= " + str(
                in_end_month - 2) + " and " + in_time_interval_column_name + " <=" + str(in_end_month))

    df_agg[in_stat_column_name + "_1/N_sum_L" + str(in_N_month)] = df_agg["#sum_curr"] / df_agg["#sum"]
    df_agg[in_stat_column_name + "_1/N_max_L" + str(in_N_month)] = df_agg["#max_curr"] / df_agg["#max"]
    df_agg[in_stat_column_name + "_1/N_avg_L" + str(in_N_month)] = df_agg["#avg_curr"] / df_agg["#avg"]
    df_agg[in_stat_column_name + "_2h/N_sum_L" + str(in_N_month)] = df_agg["#sum_2nd_half"] / df_agg["#sum"]
    df_agg[in_stat_column_name + "_2h/N_max_L" + str(in_N_month)] = df_agg["#max_2nd_half"] / df_agg["#max"]
    df_agg[in_stat_column_name + "_2h/N_avg_L" + str(in_N_month)] = df_agg["#avg_2nd_half"] / df_agg["#avg"]
    df_agg[in_stat_column_name + "_2h/1h_sum_L" + str(in_N_month)] = df_agg["#sum_2nd_half"] / df_agg["#sum_1st_half"]
    df_agg[in_stat_column_name + "_2h/1h_max_L" + str(in_N_month)] = df_agg["#max_2nd_half"] / df_agg["#max_1st_half"]
    df_agg[in_stat_column_name + "_2h/1h_avg_L" + str(in_N_month)] = df_agg["#avg_2nd_half"] / df_agg["#avg_1st_half"]
    if (in_N_month in [3, 9, 12]):
        df_1st_half_mths_agg_special = df_1st_half_mths_special.groupby(in_key_column_name)[in_stat_column_name].agg(
            agg_funcs)
        df_2nd_half_mths_agg_special = df_2nd_half_mths_special.groupby(in_key_column_name)[in_stat_column_name].agg(
            agg_funcs)
        df_agg = df_agg.merge(df_1st_half_mths_agg_special, how="left", on=in_key_column_name, validate="one_to_one",
                              suffixes=["", "_1st_half_special"])
        df_agg = df_agg.merge(df_2nd_half_mths_agg_special, how="left", on=in_key_column_name, validate="one_to_one",
                              suffixes=["", "_2nd_half_special"])
        df_agg[in_stat_column_name + "_2h/1h_sum_special"] = df_agg["#sum_2nd_half_special"] / df_agg[
            "#sum_1st_half_special"]
        df_agg[in_stat_column_name + "_2h/1h_max_special"] = df_agg["#max_2nd_half_special"] / df_agg[
            "#max_1st_half_special"]
        df_agg[in_stat_column_name + "_2h/1h_avg_special"] = df_agg["#avg_2nd_half_special"] / df_agg[
            "#avg_1st_half_special"]

    df_result_agg = df_agg.filter(regex="^[^#]", axis=1)

    df_result = df_result.merge(df_result_agg, on=in_key_column_name)

    return df_result


# 大于N连续出现最大次数
def ff_continue_gt_N(in_dataframe, in_key_column_name, in_time_interval_column, in_stat_column_name, in_value_gt_N,
                     in_end_month,
                     in_N_month, in_dic_start_id):
    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        in_end_month) + " and " + in_time_interval_column + " >= " + str(
        in_end_month - in_N_month + 1)
    dfleft = in_dataframe.copy().query(cond_filter_months).sort_values(by=[in_key_column_name, in_time_interval_column])
    dfnext = dfleft.copy()
    # TODO: add different interval according to corresponding time interval
    dfnext[in_time_interval_column] = dfleft[in_time_interval_column] + 1
    dfjoin = dfleft.merge(dfnext, how="left", on=[in_time_interval_column, in_key_column_name], validate="one_to_one",
                          suffixes=["", "_l1"])
    dfjoin[in_stat_column_name + "_l1"] = dfjoin[in_stat_column_name + "_l1"].fillna(-1)

    dfjoin["gt_N"] = dfjoin[in_stat_column_name] > in_value_gt_N
    dfjoin["l1_gt_N"] = dfjoin[in_stat_column_name + "_l1"] > in_value_gt_N
    dfjoin["continue_gt"] = ~(dfjoin["gt_N"] == dfjoin["l1_gt_N"])
    dfjoin["continue_gt_cum"] = dfjoin.groupby(in_key_column_name)["continue_gt"].cumsum()
    # 若将Y/N都统计，则将起加入groupby
    # dfjoin.groupby(["customer","gt_N"])["not_eq_cum"].value_counts()[df_value_count.index.get_level_values(1)==True].groupby(level=0).max()
    df_result_continues_gt_N = dfjoin.query("gt_N == True").groupby(in_key_column_name)[
        "continue_gt_cum"].value_counts().groupby(level=0).max()
    return df_result_continues_gt_N


# 连续增加，且大于N，最大次数
def ff_continue_inc_gt_N(in_dataframe, in_key_column_name, in_time_interval_column, in_stat_column_name,
                         in_value_continue_inc_gt_N, in_end_month,
                         in_N_month, in_dic_start_id):
    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        in_end_month) + " and " + in_time_interval_column + " >= " + str(
        in_end_month - in_N_month + 1)
    dfleft = in_dataframe.copy().query(cond_filter_months).sort_values(by=[in_key_column_name, in_time_interval_column])
    dfnext = dfleft.copy()
    # TODO: add different interval according to corresponding time interval
    dfnext[in_time_interval_column] = dfleft[in_time_interval_column] + 1
    dfnext2 = dfnext.copy()
    dfnext2[in_time_interval_column] = dfnext[in_time_interval_column] + 1
    dfnext = dfnext.merge(dfnext2, how="left", on=[in_time_interval_column, in_key_column_name], validate="one_to_one",
                          suffixes=["_l1", "_l2"])
    dfjoin = dfleft.merge(dfnext, how="left", on=[in_time_interval_column, in_key_column_name], validate="one_to_one")
    dfjoin[in_stat_column_name + "_l1"] = dfjoin[in_stat_column_name + "_l1"].fillna(-1)
    dfjoin[in_stat_column_name + "_l2"] = dfjoin[in_stat_column_name + "_l2"].fillna(-1)

    dfjoin["gt_pre"] = dfjoin[in_stat_column_name] > dfjoin[in_stat_column_name + "_l1"]
    dfjoin["l1_gt_pre"] = dfjoin[in_stat_column_name + "_l1"] > dfjoin[in_stat_column_name + "_l2"]
    dfjoin["gt_N"] = dfjoin[in_stat_column_name] > in_value_continue_inc_gt_N
    dfjoin["l1_gt_N"] = dfjoin[in_stat_column_name + "_l1"] > in_value_continue_inc_gt_N
    dfjoin["continue_inc"] = ~(
            (dfjoin["gt_pre"] & dfjoin["gt_N"]) & ((dfjoin["l1_gt_pre"] & dfjoin["l1_gt_N"]) | dfjoin["l1_gt_N"]))
    dfjoin["continue_inc_cum"] = dfjoin.groupby(in_key_column_name)["continue_inc"].cumsum()

    df_result_continues_gt_N_inc = dfjoin.query("gt_N == True").groupby(in_key_column_name)[
        "continue_inc_cum"].value_counts().groupby(level=0).max()
    df_key = in_dataframe[in_key_column_name].drop_duplicates().sort_values()
    df_result_continues_gt_N_inc = df_result_continues_gt_N_inc.reindex(df_key).fillna(0).astype(int)
    return df_result_continues_gt_N_inc


# 按给定data frame，以数量分段，统计各key在分段内的比例
def ff_bin_distribution_by_loc(in_dataframe, in_key_column_name, in_bin_value_column, in_bin_N, in_time_interval_column,
                               in_end_month, in_N_month, in_dic_start_id):
    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        in_end_month) + " and " + in_time_interval_column + " >= " + str(
        in_end_month - in_N_month + 1)
    dfsort = in_dataframe.query(cond_filter_months).sort_values(by=in_bin_value_column)
    dfsort_locbins_loc = np.linspace(0, dfsort[in_bin_value_column].count() - 1, in_bin_N + 1)
    dfsort_locbins = dfsort[in_bin_value_column].iloc[pd.Index(dfsort_locbins_loc)]
    dfsort_locbins.iloc[0] = dfsort_locbins.iloc[0] - 1  # include the first elem in first bin

    dfsort["bin_loc"] = np.digitize(dfsort[in_bin_value_column], dfsort_locbins, right=True)
    df_groupby_bin_loc = dfsort.groupby(in_key_column_name)["bin_loc"]
    agg_funcs = []
    for i in range(1, dfsort_locbins.size):
        agg_funcs += [(in_bin_value_column + "_ge_cnt_bin" + str(i) + "_L" + str(in_N_month),
                       lambda x, y=i: np.sum(x >= y) / np.size(x))]
    df_result_bin_loc = df_groupby_bin_loc.agg(agg_funcs)
    return df_result_bin_loc


# 按给定data frame，以max - min分段，统计各key在分段内的比例
def ff_bin_distribution_by_val(in_dataframe, in_key_column_name, in_bin_value_column, in_bin_N, in_time_interval_column,
                               in_end_month, in_N_month, in_dic_start_id):
    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        in_end_month) + " and " + in_time_interval_column + " >= " + str(
        in_end_month - in_N_month + 1)
    dfsort = in_dataframe.query(cond_filter_months).sort_values(by=in_bin_value_column)
    dfsort_valbins = np.linspace(dfsort[in_bin_value_column].min(), dfsort[in_bin_value_column].max(), in_bin_N + 1)
    dfsort_valbins[0] = dfsort_valbins[0] - 1  # include the first elem in first bin
    dfsort["bin_val"] = np.digitize(dfsort[in_bin_value_column], dfsort_valbins, right=True)
    df_groupby_bin_val = dfsort.groupby(in_key_column_name)["bin_val"]
    agg_funcs = []
    for i in range(1, dfsort_valbins.size):
        agg_funcs += [(in_bin_value_column + "_ge_val_bin" + str(i) + "_L" + str(in_N_month),
                       lambda x, y=i: np.sum(x >= y) / np.size(x))]
    df_result_bin_val = df_groupby_bin_val.agg(agg_funcs)
    return df_result_bin_val


# 对于字符型字段，统计各值出现的数量和比例
def ff_category_cnt_pct(in_dataframe, in_key_column_name, in_stat_column_name, in_time_interval_column,
                        in_end_month, in_N_month, in_dic_start_id):
    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        in_end_month) + " and " + in_time_interval_column + " >= " + str(
        in_end_month - in_N_month + 1)
    df_last_N_months=in_dataframe.query(cond_filter_months)
    pd_dummy = pd.get_dummies(df_last_N_months[in_stat_column_name], prefix=in_stat_column_name)
    pd_dummy[in_key_column_name] = in_dataframe[in_key_column_name]
    pd_dummy_groupby = pd_dummy.groupby(in_key_column_name)
    pd_dummy_result_cnt = pd_dummy_groupby.sum().add_suffix("_cnt_L" + str(in_N_month))
    pd_dummy_result_pct = pd_dummy_groupby.mean().add_suffix("_pct_L" + str(in_N_month))
    pd_dummy_result = pd_dummy_result_cnt.merge(pd_dummy_result_pct, left_index=True, right_index=True)
    return pd_dummy_result
