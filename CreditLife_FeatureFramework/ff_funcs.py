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
                       in_N_month, in_dic_start_id):
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
    for l in range(column_frame.index.size):
        for i in range(len(column_frame.at[l, "r"]) - 1):
            for j in itertools.combinations(column_frame.at[l, "r"], i + 1):
                dict_result_dic[i_dict_key_id] = {
                    "module": "ff_combination_pct",
                    "key_column": in_key_column_name,
                    "month_column": in_time_interval_column,
                    "numerator": column_frame.at[l, "l"],
                    "denominator": column_frame.at[l, "l"] + j,
                    "N_Months": str(in_N_month)}
                i_dict_key_id += 1

    df_pct_var = in_dataframe.groupby(in_key_column_name)[in_pct_column_lists + [in_time_interval_column]].rolling(
        in_N_month, on=in_time_interval_column).sum()
    # df_pct_var
    for k in dict_result_dic.keys():
        tup = dict_result_dic[k]
        numerator = list(tup["numerator"])
        numerator.sort()
        denominator = list(tup["denominator"])
        denominator.sort()
        df_pct_var["var_" + str(k)] = df_pct_var[list(
            tup["numerator"])].sum(axis=1) / df_pct_var[list(tup["denominator"])].sum(axis=1)

    set_result_columns = set(df_pct_var.columns)
    set_result_columns -= set(in_pct_column_lists)
    df_pct_var = df_pct_var.reindex(columns=list(set_result_columns))

    return (dict_result_dic, df_pct_var)


# 基础统计
def ff_common_stat(in_dataframe, in_key_column_name, in_time_interval_column_name, in_stat_column_name, in_end_month,
                   in_N_month, in_dic_start_id, in_agg_funcs_str=None):
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

    if in_agg_funcs_str is None:
        agg_funcs_str = ["np.size", "cnt_gt_0", "pct_gt_0", "np.sum", "np.min", "np.max", "np.average", "np.median",
                         "cnt_isna", "pct_isna"]
    else:
        agg_funcs_str = in_agg_funcs_str

    cond_filter_months = in_time_interval_column_name + " <= " + str(
        in_end_month) + " and " + in_time_interval_column_name + " >= " + str(
        in_end_month - in_N_month + 1)
    in_df_N_months = in_dataframe.copy().query(cond_filter_months)
    # df_result = in_df_N_months.groupby(in_key_column_name)[in_stat_column_name].agg(agg_funcs)

    dict_result_dic = dict()
    agg_funcs = []
    i_dict_key_id = in_dic_start_id
    for func_name in agg_funcs_str:
        dict_result_dic[i_dict_key_id] = {
            "module": "ff_common_stat",
            "key_column": in_key_column_name,
            "month_column": in_time_interval_column_name,
            "value_column": in_stat_column_name,
            "base_month": str(in_end_month),
            "N_Months": str(in_N_month),
            "func_name": func_name}
        agg_funcs += [(in_stat_column_name + "_" + func_name + "_L" + str(in_N_month), eval(func_name))]
        i_dict_key_id += 1
    df_result = in_df_N_months.groupby(in_key_column_name)[in_stat_column_name].agg(agg_funcs)

    return (dict_result_dic, df_result)


# 各时段前后段环比统计
def ff_period_compare_stat(in_dataframe, in_key_column_name, in_time_interval_column_name, in_stat_column_name,
                           in_end_month,
                           in_N_month, in_dic_start_id, in_2nd_months=None, in_1st_months=None,
                           in_agg_funcs_str=None):
    # 过去月份变量对比
    minus_2nd_months = in_2nd_months  # 分子月份数量（从end month往前推的月份数，含end month当月）
    minus_1st_months = in_1st_months  # 分母月份数量（从end month - N + 1开始，往后推的月份数，含end month - N + 1）

    if (in_N_month <= 0):
        raise Exception("parameter last N months should > 0")
    elif (in_N_month > 1):
        if (minus_2nd_months is None and minus_1st_months is None):
            if ((in_N_month % 2) == 0):
                minus_2nd_months = in_N_month / 2
                minus_1st_months = in_N_month / 2
            else:
                minus_2nd_months = (in_N_month + 1) / 2
                minus_1st_months = (in_N_month + 1) / 2
    else:  # in_N_month==1
        minus_2nd_months = 1
        minus_1st_months = 1
    minus_2nd_months = int(minus_2nd_months)
    minus_1st_months = int(minus_1st_months)

    cond_filter_months = in_time_interval_column_name + " <= " + str(
        in_end_month) + " and " + in_time_interval_column_name + " >= " + str(
        in_end_month - in_N_month + 1)
    in_df_N_months = in_dataframe.copy().query(cond_filter_months)
    df_result_agg = in_df_N_months[in_key_column_name].drop_duplicates().to_frame()
    df_result_agg.index = df_result_agg[in_key_column_name]

    # 定义循环月份列表
    months_loop = set([(1, in_N_month), (minus_2nd_months, in_N_month), (minus_2nd_months, minus_1st_months)])

    agg_funcs_str = in_agg_funcs_str
    agg_funcs_str = ["np.sum", "np.max", "np.mean", ]

    # special additional cases
    df_1st_half_mths_special = pd.DataFrame([])
    df_2nd_half_mths_special = pd.DataFrame([])

    dict_result_dic = dict()
    i_dict_key_id = in_dic_start_id
    for month_item in months_loop:
        minus_1st_half_mths_start = in_N_month - 1
        minus_1st_half_mths_end = in_N_month - month_item[1]
        minus_2nd_half_mths_start = month_item[0] - 1
        df_1st_half_mths = in_df_N_months.query(
            in_time_interval_column_name + " >= " + str(
                in_end_month - minus_1st_half_mths_start) + " and " + in_time_interval_column_name + " <=" + str(
                in_end_month - minus_1st_half_mths_end))
        df_2nd_half_mths = in_df_N_months.query(
            in_time_interval_column_name + " >= " + str(
                in_end_month - minus_2nd_half_mths_start) + " and " + in_time_interval_column_name + " <=" + str(
                in_end_month))
        agg_funcs = []
        for func_name in agg_funcs_str:
            func_tuple = ("#" + func_name, eval(func_name))
            agg_funcs += [func_tuple]
            dict_result_dic[i_dict_key_id] = {
                "module": "ff_period_compare_stat",
                "key_column": in_key_column_name,
                "month_column": in_time_interval_column_name,
                "value_column": in_stat_column_name,
                "base_month": str(in_end_month),
                "N_Months": str(in_N_month),
                "func_name": func_name,
                "numerator_months": month_item[0],
                "denomerator_months": month_item[1]}
            i_dict_key_id += 1
            df_1st_half_mths_agg = df_1st_half_mths.groupby(in_key_column_name)[in_stat_column_name].agg([func_tuple])
            df_2nd_half_mths_agg = df_2nd_half_mths.groupby(in_key_column_name)[in_stat_column_name].agg([func_tuple])
            df_agg = df_2nd_half_mths_agg.merge(df_1st_half_mths_agg, how="outer", on=in_key_column_name,
                                                validate="one_to_one",
                                                suffixes=["_2nd_half", "_1st_half"])
            df_result_agg[
                in_stat_column_name + "_" + str(month_item[0]) + "/" + str(
                    month_item[1]) + "_" + func_name + "_L" + str(
                    in_N_month)] = df_agg[func_tuple[0] + "_2nd_half"] / df_agg[func_tuple[0] + "_1st_half"]

            if (in_N_month in [3, 9, 12]):
                if (in_N_month == 3):
                    minus_2nd_months = 1
                    minus_1st_months = 2
                elif (in_N_month == 9):
                    minus_2nd_months = 3
                    minus_1st_months = 6
                elif (in_N_month == 12):
                    minus_2nd_months = 3
                    minus_1st_months = 9
                minus_1st_half_mths_start = in_N_month - 1
                minus_1st_half_mths_end = in_N_month - minus_2nd_months
                minus_2nd_half_mths_start = minus_2nd_months - 1
                df_1st_half_mths_special = in_df_N_months.query(
                    in_time_interval_column_name + " >= " + str(
                        in_end_month - minus_1st_half_mths_start) + " and " + in_time_interval_column_name + " <=" + str(
                        in_end_month - minus_1st_half_mths_end))
                df_2nd_half_mths_special = in_df_N_months.query(
                    in_time_interval_column_name + " >= " + str(
                        in_end_month - minus_2nd_half_mths_start) + " and " + in_time_interval_column_name + " <=" + str(
                        in_end_month))
                df_1st_half_mths_agg_special = df_1st_half_mths_special.groupby(in_key_column_name)[
                    in_stat_column_name].agg(
                    [func_tuple])
                df_2nd_half_mths_agg_special = df_2nd_half_mths_special.groupby(in_key_column_name)[
                    in_stat_column_name].agg(
                    [func_tuple])
                df_agg = df_2nd_half_mths_agg_special.merge(df_1st_half_mths_agg_special, how="outer",
                                                            on=in_key_column_name,
                                                            validate="one_to_one",
                                                            suffixes=["_2nd_half_special_L" + str(in_N_month),
                                                                      "_1st_half_special_L" + str(in_N_month)])

                dict_result_dic[i_dict_key_id] = {
                    "module": "ff_period_compare_stat",
                    "key_column": in_key_column_name,
                    "month_column": in_time_interval_column_name,
                    "value_column": in_stat_column_name,
                    "base_month": str(in_end_month),
                    "N_Months": str(in_N_month),
                    "func_name": func_name,
                    "numerator_months": str(minus_2nd_months),
                    "denomerator_months": str(minus_1st_months)}
                i_dict_key_id += 1
                df_result_agg[
                    in_stat_column_name + "_" + str(minus_2nd_months) + "/" + str(
                        minus_1st_months) + "_" + func_name + "_L" + str(
                        in_N_month)] = df_agg[func_tuple[0] + "_2nd_half_special_L" + str(in_N_month)] / df_agg[
                    func_tuple[0] + "_1st_half_special_L" + str(in_N_month)]

    df_result_agg = df_result_agg.filter(regex="^[^#]", axis=1)

    return (dict_result_dic, df_result_agg)


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
    dic_result = dict({in_dic_start_id: {
        "module": "ff_continue_gt_N",
        "key_column": in_key_column_name,
        "month_column": in_time_interval_column,
        "value_column": in_stat_column_name,
        "base_month": str(in_end_month),
        "N_Months": str(in_N_month),
        "gt_N": in_value_gt_N}})
    return (dic_result, df_result_continues_gt_N)


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
    dic_result = dict({in_dic_start_id: {
        "module": "ff_continue_inc_gt_N",
        "key_column": in_key_column_name,
        "month_column": in_time_interval_column,
        "value_column": in_stat_column_name,
        "base_month": str(in_end_month),
        "N_Months": str(in_N_month),
        "gt_N": in_value_continue_inc_gt_N}})
    return (dic_result, df_result_continues_gt_N_inc)


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
    # dfsort_locbins.iloc[0] = dfsort_locbins.iloc[0] - 1  # include the first elem in first bin

    dfsort["bin_loc"] = np.digitize(dfsort[in_bin_value_column], dfsort_locbins)
    df_groupby_bin_loc = dfsort.groupby(in_key_column_name)["bin_loc"]
    agg_funcs = dict()
    for i in range(1, dfsort_locbins.size):
        agg_funcs[dfsort_locbins.values[i - 1]] = (
            (in_bin_value_column + "_ge_cnt_bin" + str(dfsort_locbins.values[i - 1]) + "_L" + str(in_N_month),
             lambda x, y=i: np.sum(x >= y) / np.size(x)))

    dict_result_dic = dict()
    i_dict_key_id = in_dic_start_id
    for k in agg_funcs.keys():
        dict_result_dic[i_dict_key_id] = {
            "module": "ff_bin_distribution_by_loc",
            "key_column": in_key_column_name,
            "month_column": in_time_interval_column,
            "value_column": in_bin_value_column,
            "base_month": str(in_end_month),
            "N_Months": str(in_N_month),
            "number_of_bins": str(in_bin_N),
            "bin_value": k}
        i_dict_key_id += 1
        df_result_bin_loc = df_groupby_bin_loc.agg([agg_funcs[k]])

    return (dict_result_dic, df_result_bin_loc)


# 按给定data frame，以max - min分段，统计各key在分段内的比例
def ff_bin_distribution_by_val(in_dataframe, in_key_column_name, in_bin_value_column, in_bin_N, in_time_interval_column,
                               in_end_month, in_N_month, in_dic_start_id):
    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        in_end_month) + " and " + in_time_interval_column + " >= " + str(
        in_end_month - in_N_month + 1)
    dfsort = in_dataframe.query(cond_filter_months).sort_values(by=in_bin_value_column)
    dfsort_valbins = np.linspace(dfsort[in_bin_value_column].min(), dfsort[in_bin_value_column].max(), in_bin_N + 1)
    # dfsort_valbins[0] = dfsort_valbins[0] - 1  # include the first elem in first bin
    dfsort["bin_val"] = np.digitize(dfsort[in_bin_value_column], dfsort_valbins)
    df_groupby_bin_val = dfsort.groupby(in_key_column_name)["bin_val"]
    agg_funcs = dict()
    for i in range(1, dfsort_valbins.size):
        agg_funcs[dfsort_valbins[i - 1]] = (
            in_bin_value_column + "_ge_val_bin" + str(dfsort_valbins[i - 1]) + "_L" + str(in_N_month),
            lambda x, y=i: np.sum(x >= y) / np.size(x))

    dict_result_dic = dict()
    i_dict_key_id = in_dic_start_id
    for k in agg_funcs.keys():
        dict_result_dic[i_dict_key_id] = {
            "module": "ff_bin_distribution_by_val",
            "key_column": in_key_column_name,
            "month_column": in_time_interval_column,
            "value_column": in_bin_value_column,
            "base_month": str(in_end_month),
            "N_Months": str(in_N_month),
            "number_of_bins": str(in_bin_N),
            "bin_value": k}
        i_dict_key_id += 1
        df_result_bin_val = df_groupby_bin_val.agg([agg_funcs[k]])
    return (dict_result_dic, df_result_bin_val)


# 对于字符型字段，统计各值出现的数量和比例
def ff_category_cnt_pct(in_dataframe, in_key_column_name, in_stat_column_name, in_time_interval_column,
                        in_end_month, in_N_month, in_dic_start_id):
    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        in_end_month) + " and " + in_time_interval_column + " >= " + str(
        in_end_month - in_N_month + 1)
    df_last_N_months = in_dataframe.query(cond_filter_months)
    pd_dummy = pd.get_dummies(df_last_N_months[in_stat_column_name])
    col_list = pd_dummy.columns
    pd_dummy[in_key_column_name] = df_last_N_months[in_key_column_name]
    pd_dummy_groupby = pd_dummy.groupby(in_key_column_name)
    pd_dummy_result = in_dataframe[in_key_column_name].drop_duplicates().to_frame()
    pd_dummy_result.index = pd_dummy_result[in_key_column_name]
    dict_result_dic = dict()
    i_dict_key_id = in_dic_start_id
    for col_name in col_list:
        dict_result_dic[i_dict_key_id] = {
            "module": "ff_category_cnt_pct",
            "key_column": in_key_column_name,
            "month_column": in_time_interval_column,
            "base_month": str(in_end_month),
            "value_column": in_stat_column_name,
            "func_name": "cnt",
            "N_Months": str(in_N_month),
            "category_value": col_name}
        i_dict_key_id += 1
        df_dummy_cnt_result = pd_dummy_groupby[col_name].sum()
        pd_dummy_result[in_stat_column_name + "_" + col_name + "_cnt_L" + str(in_N_month)] = df_dummy_cnt_result

        dict_result_dic[i_dict_key_id] = {
            "module": "ff_category_cnt_pct",
            "key_column": in_key_column_name,
            "month_column": in_time_interval_column,
            "base_month": str(in_end_month),
            "value_column": in_stat_column_name,
            "func_name": "pct",
            "N_Months": str(in_N_month),
            "category_value": col_name}
        i_dict_key_id += 1
        df_dummy_pct_result = pd_dummy_groupby[col_name].mean()
        pd_dummy_result[in_stat_column_name + "_" + col_name + "_pct_L" + str(in_N_month)] = df_dummy_pct_result
    pd_dummy_result = pd_dummy_result.reindex(columns=list(set(pd_dummy_result.columns.values) - set([in_key_column_name])))

    return (dict_result_dic, pd_dummy_result)
