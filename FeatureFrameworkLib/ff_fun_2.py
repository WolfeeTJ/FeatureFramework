import pandas as pd


def get_data_condition(in_data, by, condition_var, condition, keep_vars=None):
    """
    本函数用于输出 （数据集 in_data） 中 （每个分组 by），（变量 condition_var）满足相应（条件 condition）的一条数据。
    输出数据中每个分组by只对应一条数据，by为主键

    in_data:        pandas.DataFrame类型
                    输入数据集，必须包括by， condition_var中的变量
    by:             list类型
                    分组变量的名称列表，可以有多个分组变量
    condition_var:  string类型
                    用于计算条件的变量名称，
    condition:      list类型，取值可以是'min'，'max'
                    对应变量condition_var应该满足的条件
                    其'min'表示取按by分组的，每组condition_var最小的一行数据
                    其'max'表示取按by分组的，每组condition_var最大的一行数据
                    如：condition = ['min'], condition=['min', 'max']
    keep_vars:      list类型
                    输出数据集所包含的变量的名称列表，不填写默认为输出所有变量。

    Example:    f = open('data/MD_MTH_DATA.txt', encoding='utf-8')
                in_data = pd.read_table(f)
                get_data_condition(in_data=in_data,
                                   by=['customer'],
                                   condition_var='month_nbr',
                                   condition=['min', 'max'],
                                   keep_vars=['amt1', 'amt2'])
    """
    # 参数处理
    by = list(by)
    condition = list(condition)
    if keep_vars is None:
        keep_vars = list(in_data.keys())
    else:
        keep_vars.extend(by)
        keep_vars = list(pd.unique(keep_vars))
    result_tb = None  # 输出数据集名称

    # 汇总计算
    group_tb = in_data.groupby(by=by)[condition_var]
    for kk in range(len(condition)):
        each_con = condition[kk]
        choosed_row = in_data.loc[getattr(group_tb, 'idx' + each_con)().values, keep_vars]
        # 重命名
        new_vars = [each if each in by else each_con + '_' + condition_var + '_' + each for each in keep_vars]
        choosed_row.columns = new_vars

        # 合并数据
        if kk == 0:
            result_tb = choosed_row
        else:
            result_tb = result_tb.merge(right=choosed_row, how='inner', on=by, validate='one_to_one')

    return result_tb


def matching_data(in_data, match_var, words_list, method='equal'):
    """
    本函数用于数据集变量匹配关键词，并返回匹配结果（是否匹配成功 0：否，1：是），
    返回结果是pandas.Series类型

    in_data:    pandas.DataFrame类型
                需要进行匹配操作的数据集，包含字段match_var
    match_var:  string类型
                in_data中的字段名，需要进行匹配的字段
    words_list: list类型
                需要匹配的关键词列表
    method:     string类型，可以取值'equal'，'contain'，'contained'，默认为'equal'
                指定匹配方式，其中
                'equal'表示match_var的值等于words_list的中的关键词
                'contain'表示match_var的值包含words_list中的关键词
                'contained'表示words_list中的关键词包含match_var的值

    Example:    f = open('data/MD_MTH_DATA.txt', encoding='utf-8')
                in_data = pd.read_table(f)
                matching_data(in_data=in_data,
                              match_var='customer',
                              words_list=['小明', '小王', '张', '小李头'],
                              method='equal')
    """

    # 自定义函数
    def contain(x):
        is_contain = sum([each in x for each in words_list]) > 0
        if is_contain:
            return 1
        else:
            return 0

    def contained(x):
        is_contained = sum([x in each for each in words_list]) > 0
        if is_contained:
            return 1
        else:
            return 0

    # 开始匹配
    if method == 'equal':
        result_series = in_data[match_var].isin(words_list) + 0
    elif method == 'contain':
        result_series = in_data[match_var].apply(contain)
    elif method == 'contained':
        result_series = in_data[match_var].apply(contained)

    return result_series



