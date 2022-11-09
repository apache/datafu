# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
 
import pyspark
from pyspark.sql import DataFrame
from pyspark_utils.bridge_utils import _getjvm_class


def _get_utils(df):
    _gateway = df._sc._gateway
    return _getjvm_class(_gateway, "datafu.spark.SparkDFUtilsBridge")


# public:


def dedup_with_order(df, group_col, order_cols = []):
    """
    Used get the 'latest' record (after ordering according to the provided order columns) in each group.
    :param df: DataFrame to operate on
    :param group_col: column to group by the records
    :param order_cols: columns to order the records according to.
    :return: DataFrame representing the data after the operation
    """
    java_cols = _cols_to_java_cols(order_cols)
    jdf = _get_utils(df).dedupWithOrder(df._jdf, group_col._jc, java_cols)
    return DataFrame(jdf, df.sql_ctx)


def dedup_top_n(df, n, group_col, order_cols = []):
    """
    Used get the top N records (after ordering according to the provided order columns) in each group.
    :param df: DataFrame to operate on
    :param n: number of records to return from each group
    :param group_col: column to group by the records
    :param order_cols: columns to order the records according to
    :return: DataFrame representing the data after the operation
    """
    java_cols = _cols_to_java_cols(order_cols)
    jdf = _get_utils(df).dedupTopN(df._jdf, n, group_col._jc, java_cols)
    return DataFrame(jdf, df.sql_ctx)


def dedup_with_combiner(df, group_col, order_by_col, desc = True, columns_filter = [], columns_filter_keep = True):
    """
    Used get the 'latest' record (after ordering according to the provided order columns) in each group.
    :param df: DataFrame to operate on
    :param group_col: column to group by the records
    :param order_by_col: column to order the records according to
    :param desc: have the order as desc
    :param columns_filter: columns to filter
    :param columns_filter_keep: indicates whether we should filter the selected columns 'out' or alternatively have only
*                          those columns in the result
    :return: DataFrame representing the data after the operation
    """
    jdf = _get_utils(df).dedupWithCombiner(df._jdf, group_col._jc, order_by_col._jc, desc, columns_filter, columns_filter_keep)
    return DataFrame(jdf, df.sql_ctx)


def change_schema(df, new_scheme = []):
    """
    Returns a DataFrame with the column names renamed to the column names in the new schema
    :param df: DataFrame to operate on
    :param new_scheme: new column names
    :return: DataFrame representing the data after the operation
    """
    jdf = _get_utils(df).changeSchema(df._jdf, new_scheme)
    return DataFrame(jdf, df.sql_ctx)


def join_skewed(df_left, df_right, join_exprs, num_shards = 30, join_type="inner"):
    """
    Used to perform a join when the right df is relatively small but doesn't fit to perform broadcast join.
    Use cases:
        a. excluding keys that might be skew from a medium size list.
        b. join a big skewed table with a table that has small number of very big rows.
    :param df_left: left DataFrame
    :param df_right: right DataFrame
    :param join_exprs: join expression
    :param num_shards: number of shards
    :param join_type: join type
    :return: DataFrame representing the data after the operation
    """
    jdf = _get_utils(df_left).joinSkewed(df_left._jdf, df_right._jdf, join_exprs._jc, num_shards, join_type)
    return DataFrame(jdf, df_left.sql_ctx)


def broadcast_join_skewed(not_skewed_df, skewed_df, join_col, number_of_custs_to_broadcast, filter_cnt, join_type):
    """
    Suitable to perform a join in cases when one DF is skewed and the other is not skewed.
    splits both of the DFs to two parts according to the skewed keys.
    1. Map-join: broadcasts the skewed-keys part of the not skewed DF to the skewed-keys part of the skewed DF
    2. Regular join: between the remaining two parts.
    :param not_skewed_df: not skewed DataFrame
    :param skewed_df: skewed DataFrame
    :param join_col: join column
    :param number_of_custs_to_broadcast: number of custs to broadcast
    :param filter_cnt: filter out unskewed rows from the boardcast to ease limit calculation
    :param join_type: join type
    :return: DataFrame representing the data after the operation
    """
    jdf = _get_utils(skewed_df).broadcastJoinSkewed(not_skewed_df._jdf, skewed_df._jdf, join_col, number_of_custs_to_broadcast, filter_cnt, join_type)
    return DataFrame(jdf, not_skewed_df.sql_ctx)


def join_with_range(df_single, col_single, df_range, col_range_start, col_range_end, decrease_factor):
    """
    Helper function to join a table with column to a table with range of the same column.
    For example, ip table with whois data that has range of ips as lines.
    The main problem which this handles is doing naive explode on the range can result in huge table.
    requires:
    1. single table needs to be distinct on the join column, because there could be a few corresponding ranges so we dedup at the end - we choose the minimal range.
    2. the range and single columns to be numeric.
    """
    jdf = _get_utils(df_single).joinWithRange(df_single._jdf, col_single, df_range._jdf, col_range_start, col_range_end, decrease_factor)
    return DataFrame(jdf, df_single.sql_ctx)


def join_with_range_and_dedup(df_single, col_single, df_range, col_range_start, col_range_end, decrease_factor, dedup_small_range):
    """
    Helper function to join a table with column to a table with range of the same column.
    For example, ip table with whois data that has range of ips as lines.
    The main problem which this handles is doing naive explode on the range can result in huge table.
    requires:
    1. single table needs to be distinct on the join column, because there could be a few corresponding ranges so we dedup at the end - we choose the minimal range.
    2. the range and single columns to be numeric.
    """
    jdf = _get_utils(df_single).joinWithRangeAndDedup(df_single._jdf, col_single, df_range._jdf, col_range_start, col_range_end, decrease_factor, dedup_small_range)
    return DataFrame(jdf, df_single.sql_ctx)

def explode_array(df, array_col, alias):
    """
    Given an array column that you need to explode into different columns, use this method.
    This function counts the number of output columns by executing the Spark job internally on the input array column.
    Consider caching the input dataframe if this is an expensive operation.
    :param df: DataFrame to operate on
    :param array_col: Array Column
    :param alias: Alias for new columns after explode
    """
    jdf = _get_utils(df).explodeArray(df._jdf, array_col._jc, alias)
    return DataFrame(jdf, df.sql_ctx)

def _cols_to_java_cols(cols):
    return _map_if_needed(lambda x: x._jc, cols)


def _dfs_to_java_dfs(dfs):
    return _map_if_needed(lambda x: x._jdf, dfs)


def _map_if_needed(func, itr):
    return map(func, itr) if itr is not None else itr


def activate():
    """Activate integration between datafu-spark and PySpark.
    This function only needs to be called once.

    This technique taken from pymongo_spark
    https://github.com/mongodb/mongo-hadoop/blob/master/spark/src/main/python/pymongo_spark.py
    """
    pyspark.sql.DataFrame.dedup_with_order = dedup_with_order
    pyspark.sql.DataFrame.dedup_top_n = dedup_top_n
    pyspark.sql.DataFrame.dedup_with_combiner = dedup_with_combiner
    pyspark.sql.DataFrame.change_schema = change_schema
    pyspark.sql.DataFrame.join_skewed = join_skewed
    pyspark.sql.DataFrame.broadcast_join_skewed = broadcast_join_skewed
    pyspark.sql.DataFrame.join_with_range = join_with_range
    pyspark.sql.DataFrame.join_with_range_and_dedup = join_with_range_and_dedup
    pyspark.sql.DataFrame.explode_array = explode_array

