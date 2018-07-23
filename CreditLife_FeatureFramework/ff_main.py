# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 15:44:42 2018

@author: Guang Du
"""
import CreditLife_FeatureFramework.ff_funcs as ff
import pandas as pd
import datetime


def ff_check_file(filebasename, customer_col, month_col, month_start, month_end, num_vars, char_vars):
    datafile = pd.read_table(filebasename + ".txt")

    # 检查数据缺失情况
    file_customers = datafile[customer_col].drop_duplicates()
    df_customers = pd.DataFrame({"dummy": 1, customer_col: file_customers})

    df_month = pd.DataFrame({"dummy": 1, "all_months": range(month_start, month_end + 1)})

    df_valid_cus_month = df_customers.merge(df_month, on="dummy")

    df_joined = df_valid_cus_month.merge(datafile[[customer_col, month_col]], how="left",
                                         left_on=[customer_col, "all_months"], right_on=[customer_col, month_col],
                                         validate="one_to_one")

    df_joined[month_col + "_isna"] = df_joined.isna()[month_col].astype(int).astype(str)
    df_month_str = df_joined.groupby(customer_col)[month_col + "_isna"].agg(lambda x: x.str.cat())
    s_month_str_stat = df_month_str.value_counts()
    df_month_str_stat = pd.DataFrame(
        {"month_str": s_month_str_stat.index.values, "month_str_count": s_month_str_stat, "filename": filebasename})
    #    print(df_month_str_stat)

    # 数据分布情况

    # 数据类型字段检查
    df_num_check_result = pd.DataFrame([])
    for col in set(num_vars) - set([customer_col, month_col]):
        df_num_check_common_stat_result_tmp = ff.ff_check_num_total_stat(datafile, col)
        df_num_check_val_pct_result_tmp = ff.ff_check_total_bin_distribution_by_val(datafile, col)
        df_num_check_result_tmp = df_num_check_common_stat_result_tmp.merge(df_num_check_val_pct_result_tmp,
                                                                            left_index=True, right_index=True)
        df_num_check_result_tmp["var_name"] = col
        df_num_check_result = pd.concat([df_num_check_result, df_num_check_result_tmp], ignore_index=True)
    df_num_check_result = df_num_check_result.sort_values(by=["var_name"])
    df_num_check_result["filename"] = filebasename
    #    print(df_num_check_result)

    # 字符类型字段检查
    df_char_check_result = pd.DataFrame([])
    for col in set(char_vars) - set([customer_col, month_col]):
        df_check_category_result = ff.ff_check_category_cnt_pct(datafile, col)
        df_check_category_result["var_name"] = col
        df_check_category_result = df_check_category_result.sort_values(by=["var_name", "cnt"], ascending=[True, False])
        df_char_check_result = pd.concat([df_char_check_result, df_check_category_result])
    df_char_check_result["filename"] = filebasename
    #    print(df_char_check_result)

    return (df_month_str_stat, df_num_check_result, df_char_check_result)


def log_cur_time(in_str=None):
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 现在
    print(nowTime + " " + in_str)


def ff_main(in_datasource_name, in_key_column, in_month_column, in_start_month, in_end_month, in_where):
    in_dataframe = pd.read_table("data/" + in_datasource_name + ".txt")


    # 1.指定时间周期（输入项1）（1）最近N个周期内
    df_filter = in_dataframe.query(
        in_month_column + " >= " + str(in_start_month) + " and " + in_month_column + " <=" + str(in_end_month))
    df_filter

    # 2.数据筛选条件变量（输入项2）
    if (in_where is not None):
        df_filter = df_filter.query(in_where)
        df_filter

    conf_pct = pd.read_table("conf/config-var-gen-pct.txt")
    conf_var_gen_base_stat = pd.read_table("conf/config-var-gen-base-stat.txt")
    conf_var_gen_continue_stat = pd.read_table("conf/config-var-gen-continue-stat.txt")
    conf_var_gen_bin_stat = pd.read_table("conf/config-var-gen-bin-stat.txt")
    conf_var_gen_char_stat = pd.read_table("conf/config-var-gen-char-stat.txt")

    df_conf_pct = conf_pct.query("datasource == '" + in_datasource_name + "'")
    log_cur_time("百分比衍生")

    df_result_var_pct = df_filter.copy()
    for i_df_conf_pct in range(0, len(df_conf_pct)):
        var_combinations = df_conf_pct.iloc[i_df_conf_pct]["var_combinations"]
        month_combinations = df_conf_pct.iloc[i_df_conf_pct]["month_combinations"]
        month_start = df_conf_pct.iloc[i_df_conf_pct]["month_start"]
        month_end = df_conf_pct.iloc[i_df_conf_pct]["month_end"]
        s_var_combination = pd.Series(var_combinations.split(","))
        list_var_combination = s_var_combination.apply(lambda x: x.strip()).tolist()
        s_month_combinations = pd.Series(month_combinations.split(","))
        list_month_combinations = s_month_combinations.apply(lambda x: x.strip()).astype(int).tolist()
        for var_month in list_month_combinations:
            dic_combination_dic, df_pct_var = ff.ff_combination_pct(df_filter, in_key_column, in_month_column,
                                                                    list_var_combination, var_month)
            df_result_var_pct = df_result_var_pct.merge(df_pct_var, how="left", on=[in_key_column, in_month_column],
                                        validate="one_to_one")
    df_result_var_pct = df_result_var_pct.query(
        in_month_column + " >= " + str(month_start) + " and " + in_month_column + " <=" + str(month_end))
    log_cur_time("中间层+百分比衍生结果")
    print(df_result_var_pct)

    log_cur_time("一般统计")
    df_result = df_result_var_pct[in_key_column].drop_duplicates().to_frame()
    df_conf_var_gen_base_stat = conf_var_gen_base_stat.query("datasource == '" + in_datasource_name + "'")
    for i_df_conf_var_gen_base_stat in range(0, len(df_conf_var_gen_base_stat)):
        var_name = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat]["var_name"]
        month_combinations = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat]["month_combinations"]
        month_start = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat]["month_start"]
        month_end = df_conf_var_gen_base_stat.iloc[i_df_conf_var_gen_base_stat]["month_end"]
        s_month_combinations = pd.Series(month_combinations.split(","))
        list_month_combinations = s_month_combinations.apply(lambda x: x.strip()).astype(int).tolist()
        for var_month in list_month_combinations:
            dic_common_stat = ff.ff_common_stat(df_filter, in_key_column, in_month_column, var_name,
                                                month_end, var_month)
            df_result = df_result.merge(dic_common_stat, how="outer", on=[in_key_column],
                                        validate="one_to_one")

    log_cur_time("最终结果")
    df_result_t = df_result.T
    print(df_result)

    return df_result
