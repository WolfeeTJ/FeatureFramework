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

def ff_combination_pct(in_dataframe, kwargs):
    # 取得入参
    in_key_column_name = kwargs.get("key_column")
    in_time_interval_column = kwargs.get("month_column")
    in_pct_column_lists = kwargs.get("in_pct_column_lists")
    in_N_month = int(kwargs.get("N_Months"))
    in_dic_start_id = kwargs.get("in_dic_start_id")
    numerator = kwargs.get("numerator")
    denominator = kwargs.get("denominator")
    var_name = kwargs.get("var_name")

    # 要么排列组合，要么计算指定组合，不能同时为空
    if in_pct_column_lists is None and (numerator is None or denominator is None or var_name is None):
        raise Exception("in_pct_column_lists and (numerator, denominator, var_name) should not both be None")

    dict_result_dic = dict()

    if in_pct_column_lists is not None:
        # 排列组合分子
        column_combinations = []
        import itertools
        for i in range(len(in_pct_column_lists) - 1):
            for j in itertools.combinations(in_pct_column_lists, i + 1):
                column_combinations += [j]
        # 排列组合分母
        column_frame = pd.DataFrame({"l": column_combinations})
        column_frame["r"] = column_frame["l"].map(lambda x: tuple(set(in_pct_column_lists) - set(x)))
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
        set_result_columns -= set(in_pct_column_lists + [in_time_interval_column])

    else:  # column list is None, using dict to calculate specific var
        col_list = set(numerator) | set(denominator) | set([in_time_interval_column])
        df_pct_var = in_dataframe.groupby(in_key_column_name)[list(col_list)].rolling(
            in_N_month, on=in_time_interval_column).sum()
        df_pct_var[var_name] = df_pct_var[numerator].sum(axis=1) / df_pct_var[denominator].sum(axis=1)
        set_result_columns = set(df_pct_var.columns)
        set_result_columns -= set(denominator + [in_time_interval_column])
    df_pct_var = df_pct_var.reindex(columns=list(set_result_columns))
    df_pct_var.index = df_pct_var.index.get_level_values(1)

    return (dict_result_dic, df_pct_var)


# 基础统计
def ff_common_stat(in_dataframe, kwargs):
    # 取得入参
    in_key_column_name = kwargs.get("key_column")
    in_time_interval_column = kwargs.get("month_column")
    base_month = kwargs.get("base_month")
    in_N_month = kwargs.get("N_Months")
    if not np.isnan(in_N_month):
        in_N_month = int(in_N_month)
    value_column = kwargs.get("value_column")
    func_name = kwargs.get("func_name")
    in_dic_start_id = kwargs.get("in_dic_start_id")
    var_name = kwargs.get("var_name")

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

    dict_result_dic = dict()
    if not type(in_time_interval_column) in [np.float, np.float64]:
        cond_filter_months = in_time_interval_column + " <= " + str(
            base_month) + " and " + in_time_interval_column + " >= " + str(
            base_month - in_N_month + 1)
        in_df_N_months = in_dataframe.query(cond_filter_months)
    else:
        in_df_N_months = in_dataframe
    # df_result = in_df_N_months.groupby(in_key_column_name)[value_column].agg(agg_funcs)

    agg_funcs = []
    if func_name is None:
        agg_funcs_str = ["np.size", "cnt_gt_0", "pct_gt_0", "np.sum", "np.min", "np.max", "np.average", "np.median",
                         "cnt_isna", "pct_isna"]
        i_dict_key_id = in_dic_start_id
        for func_name in agg_funcs_str:
            dict_result_dic[i_dict_key_id] = {
                "module": "ff_common_stat",
                "key_column": in_key_column_name,
                "month_column": in_time_interval_column,
                "value_column": value_column,
                # "base_month": str(base_month),
                "N_Months": str(in_N_month),
                "func_name": func_name}
            agg_funcs += [("var_" + str(i_dict_key_id), eval(func_name))]
            i_dict_key_id += 1
    else:
        agg_funcs = [(var_name, eval(func_name))]

    df_result = in_df_N_months.groupby(in_key_column_name)[value_column].agg(agg_funcs)

    return (dict_result_dic, df_result)

# 针对month_start至month_end，分布进行基础统计，并输出合并后含月份的数据集
def ff_common_stat_rolling(in_dataframe, kwargs):
    # 取得入参
    in_month_start = kwargs.get("month_start")
    in_month_end = kwargs.get("month_end")
    in_time_interval_column = kwargs.get("month_column")

    df_result = pd.DataFrame([])
    dict_result_dic = {}

    for m in range(in_month_start, in_month_end +1):
        kwargs["base_month"] = m
        dict_result_dic, df_tmp = ff_common_stat(in_dataframe, kwargs)
        df_tmp[in_time_interval_column] = m
        df_result = df_result.append(df_tmp)

    return (dict_result_dic, df_result)



# 各时段前后段环比统计
def ff_period_compare_stat(in_dataframe, kwargs):
    # 取得入参
    in_key_column_name = kwargs.get("key_column")
    in_time_interval_column = kwargs.get("month_column")
    base_month = kwargs.get("base_month")
    in_N_month = int(kwargs.get("N_Months"))
    value_column = kwargs.get("value_column")
    func_name = kwargs.get("func_name")
    in_dic_start_id = kwargs.get("in_dic_start_id")
    var_name = kwargs.get("var_name")
    minus_2nd_months = kwargs.get("numerator_months")  # 分子月份数量（从end month往前推的月份数，含end month当月）
    minus_1st_months = kwargs.get("denominator_months")  # 分母月份数量（从end month - N + 1开始，往后推的月份数，含end month - N + 1）

    dict_result_dic = dict()
    cond_filter_months = in_time_interval_column + " <= " + str(
        base_month) + " and " + in_time_interval_column + " >= " + str(
        base_month - in_N_month + 1)
    in_df_N_months = in_dataframe.copy().query(cond_filter_months)
    df_result_agg = in_df_N_months[in_key_column_name].drop_duplicates().to_frame()
    df_result_agg.index = df_result_agg[in_key_column_name]

    if minus_2nd_months is not None and minus_1st_months is not None:
        func_tuple = ("#" + func_name, eval(func_name))
        minus_1st_half_mths_start = in_N_month - 1
        minus_1st_half_mths_end = in_N_month - minus_2nd_months
        minus_2nd_half_mths_start = minus_2nd_months - 1
        df_1st_half_mths_special = in_df_N_months.query(
            in_time_interval_column + " >= " + str(
                base_month - minus_1st_half_mths_start) + " and " + in_time_interval_column + " <=" + str(
                base_month - minus_1st_half_mths_end))
        df_2nd_half_mths_special = in_df_N_months.query(
            in_time_interval_column + " >= " + str(
                base_month - minus_2nd_half_mths_start) + " and " + in_time_interval_column + " <=" + str(
                base_month))
        df_1st_half_mths_agg_special = df_1st_half_mths_special.groupby(in_key_column_name)[
            value_column].agg(
            [func_tuple])
        df_2nd_half_mths_agg_special = df_2nd_half_mths_special.groupby(in_key_column_name)[
            value_column].agg(
            [func_tuple])
        df_agg = df_2nd_half_mths_agg_special.merge(df_1st_half_mths_agg_special, how="outer",
                                                    on=in_key_column_name,
                                                    validate="one_to_one",
                                                    suffixes=["_2nd_half_L" + str(in_N_month),
                                                              "_1st_half_L" + str(in_N_month)])
        df_result_agg[var_name] = df_agg[func_tuple[0] + "_2nd_half_L" + str(in_N_month)] / df_agg[
            func_tuple[0] + "_1st_half_L" + str(in_N_month)]
        df_result_agg = df_result_agg.reindex(columns=[var_name])
    else:  # 离线分析，自行构建变量
        if in_N_month <= 0:
            raise Exception("parameter last N months should > 0")
        elif in_N_month > 1:
            if minus_2nd_months is None and minus_1st_months is None:
                if (in_N_month % 2) == 0:
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

        # 定义循环月份列表
        months_loop = {(1, in_N_month), (minus_2nd_months, in_N_month), (minus_2nd_months, minus_1st_months)}

        agg_funcs_str = ["np.sum", "np.max", "np.mean", ]

        i_dict_key_id = in_dic_start_id
        for month_item in months_loop:
            minus_1st_half_mths_start = in_N_month - 1
            minus_1st_half_mths_end = in_N_month - month_item[1]
            minus_2nd_half_mths_start = month_item[0] - 1
            df_1st_half_mths = in_df_N_months.query(
                in_time_interval_column + " >= " + str(
                    base_month - minus_1st_half_mths_start) + " and " + in_time_interval_column + " <=" + str(
                    base_month - minus_1st_half_mths_end))
            df_2nd_half_mths = in_df_N_months.query(
                in_time_interval_column + " >= " + str(
                    base_month - minus_2nd_half_mths_start) + " and " + in_time_interval_column + " <=" + str(
                    base_month))
            agg_funcs = []
            for func_name in agg_funcs_str:
                func_tuple = ("#" + func_name, eval(func_name))
                agg_funcs += [func_tuple]
                dict_result_dic[i_dict_key_id] = {
                    "module": "ff_period_compare_stat",
                    "key_column": in_key_column_name,
                    "month_column": in_time_interval_column,
                    "value_column": value_column,
                    # "base_month": str(base_month),
                    "N_Months": str(in_N_month),
                    "func_name": func_name,
                    "numerator_months": month_item[0],
                    "denominator_months": month_item[1]}
                df_1st_half_mths_agg = df_1st_half_mths.groupby(in_key_column_name)[value_column].agg([func_tuple])
                df_2nd_half_mths_agg = df_2nd_half_mths.groupby(in_key_column_name)[value_column].agg([func_tuple])
                df_agg = df_2nd_half_mths_agg.merge(df_1st_half_mths_agg, how="outer", on=in_key_column_name,
                                                    validate="one_to_one",
                                                    suffixes=["_2nd_half", "_1st_half"])
                df_result_agg["var_" + str(i_dict_key_id)] = df_agg[func_tuple[0] + "_2nd_half"] / df_agg[
                    func_tuple[0] + "_1st_half"]
                i_dict_key_id += 1

                if in_N_month in [3, 9, 12]:
                    if in_N_month == 3:
                        minus_2nd_months = 1
                        minus_1st_months = 2
                    elif in_N_month == 9:
                        minus_2nd_months = 3
                        minus_1st_months = 6
                    elif in_N_month == 12:
                        minus_2nd_months = 3
                        minus_1st_months = 9
                    minus_1st_half_mths_start = in_N_month - 1
                    minus_1st_half_mths_end = in_N_month - minus_2nd_months
                    minus_2nd_half_mths_start = minus_2nd_months - 1
                    df_1st_half_mths_special = in_df_N_months.query(
                        in_time_interval_column + " >= " + str(
                            base_month - minus_1st_half_mths_start) + " and " + in_time_interval_column + " <=" + str(
                            base_month - minus_1st_half_mths_end))
                    df_2nd_half_mths_special = in_df_N_months.query(
                        in_time_interval_column + " >= " + str(
                            base_month - minus_2nd_half_mths_start) + " and " + in_time_interval_column + " <=" + str(
                            base_month))
                    df_1st_half_mths_agg_special = df_1st_half_mths_special.groupby(in_key_column_name)[
                        value_column].agg(
                        [func_tuple])
                    df_2nd_half_mths_agg_special = df_2nd_half_mths_special.groupby(in_key_column_name)[
                        value_column].agg(
                        [func_tuple])
                    df_agg = df_2nd_half_mths_agg_special.merge(df_1st_half_mths_agg_special, how="outer",
                                                                on=in_key_column_name,
                                                                validate="one_to_one",
                                                                suffixes=["_2nd_half_special_L" + str(in_N_month),
                                                                          "_1st_half_special_L" + str(in_N_month)])

                    dict_result_dic[i_dict_key_id] = {
                        "module": "ff_period_compare_stat",
                        "key_column": in_key_column_name,
                        "month_column": in_time_interval_column,
                        "value_column": value_column,
                        # "base_month": str(base_month),
                        "N_Months": str(in_N_month),
                        "func_name": func_name,
                        "numerator_months": str(minus_2nd_months),
                        "denominator_months": str(minus_1st_months)}
                    df_result_agg["var_" + str(i_dict_key_id)] = df_agg[func_tuple[0] + "_2nd_half_special_L" + str(
                        in_N_month)] / df_agg[
                                                                     func_tuple[0] + "_1st_half_special_L" + str(
                                                                         in_N_month)]
                    i_dict_key_id += 1

    df_result_agg = df_result_agg.filter(regex="^[^#]", axis=1)

    return dict_result_dic, df_result_agg

# 各时段前后段环比统计
def ff_period_compare_stat_rolling(in_dataframe, kwargs):
    # 取得入参
    in_month_start = kwargs.get("month_start")
    in_month_end = kwargs.get("month_end")
    in_time_interval_column = kwargs.get("month_column")

    df_result = pd.DataFrame([])
    dict_result_dic = {}

    for m in range(in_month_start, in_month_end +1):
        kwargs["base_month"] = m
        dict_result_dic, df_tmp = ff_period_compare_stat(in_dataframe, kwargs)
        df_tmp[in_time_interval_column] = m
        df_result = df_result.append(df_tmp)

    return (dict_result_dic, df_result)


# 大于N连续出现最大次数
def ff_continue_gt_N(in_dataframe, kwargs):
    # 取得入参
    in_key_column_name = kwargs.get("key_column")
    in_time_interval_column = kwargs.get("month_column")
    base_month = kwargs.get("base_month")
    in_N_month = int(kwargs.get("N_Months"))
    value_column = kwargs.get("value_column")
    threshold_value = kwargs.get("threshold_value")
    in_dic_start_id = kwargs.get("in_dic_start_id")
    var_name = kwargs.get("var_name")

    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        base_month) + " and " + in_time_interval_column + " >= " + str(
        base_month - in_N_month + 1)
    dfleft = in_dataframe.copy().query(cond_filter_months).sort_values(by=[in_key_column_name, in_time_interval_column])
    dfnext = dfleft.copy()
    # TODO: add different interval according to corresponding time interval
    dfnext[in_time_interval_column] = dfleft[in_time_interval_column] + 1
    dfjoin = dfleft.merge(dfnext, how="left", on=[in_time_interval_column, in_key_column_name], validate="one_to_one",
                          suffixes=["", "_l1"])
    dfjoin[value_column + "_l1"] = dfjoin[value_column + "_l1"].fillna(-1)

    dfjoin["gt_N"] = dfjoin[value_column] > threshold_value
    dfjoin["l1_gt_N"] = dfjoin[value_column + "_l1"] > threshold_value
    dfjoin["continue_gt"] = ~(dfjoin["gt_N"] == dfjoin["l1_gt_N"])
    dfjoin["continue_gt_cum"] = dfjoin.groupby(in_key_column_name)["continue_gt"].cumsum()
    # 若将Y/N都统计，则将起加入groupby
    # dfjoin.groupby(["customer","gt_N"])["not_eq_cum"].value_counts()[df_value_count.index.get_level_values(1)==True].groupby(level=0).max()
    df_result_continues_gt_N = dfjoin.query("gt_N == True").groupby(in_key_column_name)[
        "continue_gt_cum"].value_counts().groupby(level=0).agg([("var_" + str(in_dic_start_id), np.max)])
    dic_result = dict({in_dic_start_id: {
        "module": "ff_continue_gt_N",
        "key_column": in_key_column_name,
        "month_column": in_time_interval_column,
        "value_column": value_column,
        # "base_month": str(base_month),
        "N_Months": str(in_N_month),
        "threshold_value": str(threshold_value)}})
    return (dic_result, df_result_continues_gt_N)

def ff_continue_gt_N_rolling(in_dataframe, kwargs):
    # 取得入参
    in_month_start = kwargs.get("month_start")
    in_month_end = kwargs.get("month_end")
    in_time_interval_column = kwargs.get("month_column")

    df_result = pd.DataFrame([])
    dict_result_dic = {}

    for m in range(in_month_start, in_month_end +1):
        kwargs["base_month"] = m
        dict_result_dic, df_tmp = ff_continue_gt_N(in_dataframe, kwargs)
        df_tmp[in_time_interval_column] = m
        df_result = df_result.append(df_tmp)

    return (dict_result_dic, df_result)


# 连续增加，且大于N，最大次数
def ff_continue_inc_gt_N(in_dataframe, kwargs):
    # 取得入参
    in_key_column_name = kwargs.get("key_column")
    in_time_interval_column = kwargs.get("month_column")
    base_month = kwargs.get("base_month")
    in_N_month = int(kwargs.get("N_Months"))
    value_column = kwargs.get("value_column")
    threshold_value = kwargs.get("threshold_value")
    in_dic_start_id = kwargs.get("in_dic_start_id")
    var_name = kwargs.get("var_name")

    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        base_month) + " and " + in_time_interval_column + " >= " + str(
        base_month - in_N_month + 1)
    dfleft = in_dataframe.copy().query(cond_filter_months).sort_values(by=[in_key_column_name, in_time_interval_column])
    dfnext = dfleft.copy()
    # TODO: add different interval according to corresponding time interval
    dfnext[in_time_interval_column] = dfleft[in_time_interval_column] + 1
    dfnext2 = dfnext.copy()
    dfnext2[in_time_interval_column] = dfnext[in_time_interval_column] + 1
    dfnext = dfnext.merge(dfnext2, how="left", on=[in_time_interval_column, in_key_column_name], validate="one_to_one",
                          suffixes=["_l1", "_l2"])
    dfjoin = dfleft.merge(dfnext, how="left", on=[in_time_interval_column, in_key_column_name], validate="one_to_one")
    dfjoin[value_column + "_l1"] = dfjoin[value_column + "_l1"].fillna(-1)
    dfjoin[value_column + "_l2"] = dfjoin[value_column + "_l2"].fillna(-1)

    dfjoin["gt_pre"] = dfjoin[value_column] > dfjoin[value_column + "_l1"]
    dfjoin["l1_gt_pre"] = dfjoin[value_column + "_l1"] > dfjoin[value_column + "_l2"]
    dfjoin["gt_N"] = dfjoin[value_column] > threshold_value
    dfjoin["l1_gt_N"] = dfjoin[value_column + "_l1"] > threshold_value
    dfjoin["continue_inc"] = ~(
            (dfjoin["gt_pre"] & dfjoin["gt_N"]) & ((dfjoin["l1_gt_pre"] & dfjoin["l1_gt_N"]) | dfjoin["l1_gt_N"]))
    dfjoin["continue_inc_cum"] = dfjoin.groupby(in_key_column_name)["continue_inc"].cumsum()

    df_result_continues_gt_N_inc = dfjoin.query("gt_N == True").groupby(in_key_column_name)[
        "continue_inc_cum"].value_counts().groupby(level=0).agg([("var_" + str(in_dic_start_id), np.max)])
    df_key = in_dataframe[in_key_column_name].drop_duplicates().sort_values()
    df_result_continues_gt_N_inc = df_result_continues_gt_N_inc.reindex(df_key).fillna(0).astype(int)
    dic_result = dict({in_dic_start_id: {
        "module": "ff_continue_inc_gt_N",
        "key_column": in_key_column_name,
        "month_column": in_time_interval_column,
        "value_column": value_column,
        # "base_month": str(base_month),
        "N_Months": str(in_N_month),
        "threshold_value": str(threshold_value)}})
    return (dic_result, df_result_continues_gt_N_inc)

def ff_continue_inc_gt_N_rolling(in_dataframe, kwargs):
    # 取得入参
    in_month_start = kwargs.get("month_start")
    in_month_end = kwargs.get("month_end")
    in_time_interval_column = kwargs.get("month_column")

    df_result = pd.DataFrame([])
    dict_result_dic = {}

    for m in range(in_month_start, in_month_end +1):
        kwargs["base_month"] = m
        dict_result_dic, df_tmp = ff_continue_inc_gt_N(in_dataframe, kwargs)
        df_tmp[in_time_interval_column] = m
        df_result = df_result.append(df_tmp)

    return (dict_result_dic, df_result)


# 按给定data frame，以数量分段，统计各key在分段内的比例
def ff_bin_distribution_by_loc(in_dataframe, kwargs):
    # 取得入参
    in_key_column_name = kwargs.get("key_column")
    in_time_interval_column = kwargs.get("month_column")
    base_month = kwargs.get("base_month")
    in_N_month = int(kwargs.get("N_Months"))
    value_column = kwargs.get("value_column")
    number_of_bins = kwargs.get("number_of_bins")
    threshold_value = kwargs.get("threshold_value")
    in_dic_start_id = kwargs.get("in_dic_start_id")
    var_name = kwargs.get("var_name")

    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        base_month) + " and " + in_time_interval_column + " >= " + str(
        base_month - in_N_month + 1)
    dfsort = in_dataframe.query(cond_filter_months).sort_values(by=value_column)
    dict_result_dic = dict()

    if threshold_value is None:
        dfsort_locbins_loc = np.linspace(0, dfsort[value_column].count() - 1, number_of_bins + 1)
        dfsort_locbins = dfsort[value_column].iloc[pd.Index(dfsort_locbins_loc)]
        # dfsort_locbins.iloc[0] = dfsort_locbins.iloc[0] - 1  # include the first elem in first bin

        dfsort["bin_loc"] = np.digitize(dfsort[value_column], dfsort_locbins)
        df_groupby_bin_loc = dfsort.groupby(in_key_column_name)["bin_loc"]
        agg_funcs = dict()
        i_dict_key_id = in_dic_start_id
        for i in range(1, dfsort_locbins.size):
            agg_funcs[dfsort_locbins.values[i - 1]] = lambda x, y=i: np.sum(x >= y) / np.size(x)
        agg_func_list = []
        for k in agg_funcs.keys():
            dict_result_dic[i_dict_key_id] = {
                "module": "ff_bin_distribution_by_loc",
                "key_column": in_key_column_name,
                "month_column": in_time_interval_column,
                "value_column": value_column,
                # "base_month": str(base_month),
                "N_Months": str(in_N_month),
                "number_of_bins": str(number_of_bins),
                "bin_value": k}
            agg_func_list += [("var_" + str(i_dict_key_id), agg_funcs[k])]
            i_dict_key_id += 1
        df_result_bin_loc = df_groupby_bin_loc.agg(agg_func_list)
    else:
        df_groupby_bin_loc = dfsort.groupby(in_key_column_name)[value_column]
        df_result_bin_loc = df_groupby_bin_loc.agg([(var_name, lambda x: np.sum(x >= threshold_value) / np.size(x))])

    return (dict_result_dic, df_result_bin_loc)

def ff_bin_distribution_by_loc_rolling(in_dataframe, kwargs):
    # 取得入参
    in_month_start = kwargs.get("month_start")
    in_month_end = kwargs.get("month_end")
    in_time_interval_column = kwargs.get("month_column")

    df_result = pd.DataFrame([])
    dict_result_dic = {}

    for m in range(in_month_start, in_month_end +1):
        kwargs["base_month"] = m
        dict_result_dic, df_tmp = ff_bin_distribution_by_loc(in_dataframe, kwargs)
        df_tmp[in_time_interval_column] = m
        df_result = df_result.append(df_tmp)

    return (dict_result_dic, df_result)

# 按给定data frame，以max - min分段，统计各key在分段内的比例
def ff_bin_distribution_by_val(in_dataframe, kwargs):
    # 取得入参
    in_key_column_name = kwargs.get("key_column")
    in_time_interval_column = kwargs.get("month_column")
    base_month = kwargs.get("base_month")
    in_N_month = int(kwargs.get("N_Months"))
    value_column = kwargs.get("value_column")
    number_of_bins = kwargs.get("number_of_bins")
    threshold_value = kwargs.get("threshold_value")
    in_dic_start_id = kwargs.get("in_dic_start_id")
    var_name = kwargs.get("var_name")

    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        base_month) + " and " + in_time_interval_column + " >= " + str(
        base_month - in_N_month + 1)
    dfsort = in_dataframe.query(cond_filter_months).sort_values(by=value_column)
    dict_result_dic = dict()

    if threshold_value is None:
        dfsort_valbins = np.linspace(dfsort[value_column].min(), dfsort[value_column].max(), number_of_bins + 1)
        # dfsort_valbins[0] = dfsort_valbins[0] - 1  # include the first elem in first bin
        dfsort["bin_val"] = np.digitize(dfsort[value_column], dfsort_valbins)
        df_groupby_bin_val = dfsort.groupby(in_key_column_name)["bin_val"]
        agg_funcs = dict()
        i_dict_key_id = in_dic_start_id
        for i in range(1, dfsort_valbins.size):
            agg_funcs[dfsort_valbins[i - 1]] = lambda x, y=i: np.sum(x >= y) / np.size(x)
        agg_func_list = []
        for k in agg_funcs.keys():
            dict_result_dic[i_dict_key_id] = {
                "module": "ff_bin_distribution_by_val",
                "key_column": in_key_column_name,
                "month_column": in_time_interval_column,
                "value_column": value_column,
                # "base_month": str(base_month),
                "N_Months": str(in_N_month),
                "number_of_bins": str(number_of_bins),
                "bin_value": k}
            agg_func_list += [("var_" + str(i_dict_key_id), agg_funcs[k])]
            i_dict_key_id += 1
        df_result_bin_val = df_groupby_bin_val.agg(agg_func_list)
    else:
        df_groupby_bin_val = dfsort.groupby(in_key_column_name)[value_column]
        df_result_bin_val = df_groupby_bin_val.agg([(var_name, lambda x: np.sum(x >= threshold_value) / np.size(x))])

    return (dict_result_dic, df_result_bin_val)

def ff_bin_distribution_by_val_rolling(in_dataframe, kwargs):
    # 取得入参
    in_month_start = kwargs.get("month_start")
    in_month_end = kwargs.get("month_end")
    in_time_interval_column = kwargs.get("month_column")

    df_result = pd.DataFrame([])
    dict_result_dic = {}

    for m in range(in_month_start, in_month_end +1):
        kwargs["base_month"] = m
        dict_result_dic, df_tmp = ff_bin_distribution_by_val(in_dataframe, kwargs)
        df_tmp[in_time_interval_column] = m
        df_result = df_result.append(df_tmp)

    return (dict_result_dic, df_result)

# 对于字符型字段，统计各值出现的数量和比例
def ff_category_cnt_pct(in_dataframe, kwargs):
    # 取得入参
    in_key_column_name = kwargs.get("key_column")
    in_time_interval_column = kwargs.get("month_column")
    base_month = kwargs.get("base_month")
    in_N_month = int(kwargs.get("N_Months"))
    value_column = kwargs.get("value_column")
    category_value = kwargs.get("category_value")
    in_dic_start_id = kwargs.get("in_dic_start_id")
    var_name = kwargs.get("var_name")

    # 预处理
    cond_filter_months = in_time_interval_column + " <= " + str(
        base_month) + " and " + in_time_interval_column + " >= " + str(
        base_month - in_N_month + 1)
    df_last_N_months = in_dataframe.query(cond_filter_months)

    pd_dummy_result = df_last_N_months[in_key_column_name].drop_duplicates().to_frame()
    pd_dummy_result.index = pd_dummy_result[in_key_column_name]

    dict_result_dic = dict()
    if category_value is None:
        pd_dummy = pd.get_dummies(df_last_N_months[value_column])
        col_list = pd_dummy.columns
        pd_dummy[in_key_column_name] = df_last_N_months[in_key_column_name]
        pd_dummy_groupby = pd_dummy.groupby(in_key_column_name)
        i_dict_key_id = in_dic_start_id
        for col_name in col_list:
            dict_result_dic[i_dict_key_id] = {
                "module": "ff_category_cnt_pct",
                "key_column": in_key_column_name,
                "month_column": in_time_interval_column,
                # "base_month": str(base_month),
                "value_column": value_column,
                "func_name": "cnt",
                "N_Months": str(in_N_month),
                "category_value": col_name}
            df_dummy_cnt_result = pd_dummy_groupby[col_name].sum()
            pd_dummy_result["var_" + str(i_dict_key_id)] = df_dummy_cnt_result
            i_dict_key_id += 1

            dict_result_dic[i_dict_key_id] = {
                "module": "ff_category_cnt_pct",
                "key_column": in_key_column_name,
                "month_column": in_time_interval_column,
                # "base_month": str(base_month),
                "value_column": value_column,
                "func_name": "pct",
                "N_Months": str(in_N_month),
                "category_value": col_name}
            df_dummy_pct_result = pd_dummy_groupby[col_name].mean()
            pd_dummy_result["var_" + str(i_dict_key_id)] = df_dummy_pct_result
            i_dict_key_id += 1
    else:
        pd_dummy_result[var_name] = df_last_N_months.groupby(in_key_column_name)[value_column].agg(
            lambda x: x[x == category_value].size)
    pd_dummy_result = pd_dummy_result.reindex(
        columns=list(set(pd_dummy_result.columns.values) - set([in_key_column_name])))

    return (dict_result_dic, pd_dummy_result)

def ff_category_cnt_pct_rolling(in_dataframe, kwargs):
    # 取得入参
    in_month_start = kwargs.get("month_start")
    in_month_end = kwargs.get("month_end")
    in_time_interval_column = kwargs.get("month_column")

    df_result = pd.DataFrame([])
    dict_result_dic = {}

    for m in range(in_month_start, in_month_end +1):
        kwargs["base_month"] = m
        dict_result_dic, df_tmp = ff_category_cnt_pct(in_dataframe, kwargs)
        df_tmp[in_time_interval_column] = m
        df_result = df_result.append(df_tmp)

    return (dict_result_dic, df_result)


# 给定聚合后的data frame，执行自定义的表达式来计算变量
def ff_customized_var(df, kwargs):
    # 取得入参
    expression = kwargs.get("expression")

    dict_result_dic = dict()
    df_customized_var = eval(expression)

    return (dict_result_dic, df_customized_var)
