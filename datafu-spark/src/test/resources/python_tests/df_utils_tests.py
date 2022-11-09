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

# This file is used by the datafu-spark unit tests

import os
import sys
from pprint import pprint as p

from pyspark_utils import df_utils

p('CHECKING IF PATHS EXISTS:')
for x in sys.path:
    p('PATH ' + x + ': ' + str(os.path.exists(x)))


df_utils.activate()

df_people = sqlContext.createDataFrame([
    ("a", "Alice", 34),
    ("a", "Sara", 33),
    ("b", "Bob", 36),
    ("b", "Charlie", 30),
    ("c", "David", 29),
    ("c", "Esther", 32),
    ("c", "Fanny", 36),
    ("c", "Zoey", 36)],
    ["id", "name", "age"])

func_dedup_res = df_people.dedup_with_order(group_col=df_people.id,
                             order_cols=[df_people.age.desc(), df_people.name.desc()])
func_dedup_res.registerTempTable("dedup_with_order")

func_dedupTopN_res = df_people.dedup_top_n(n=2, group_col=df_people.id,
                                     order_cols=[df_people.age.desc(), df_people.name.desc()])
func_dedupTopN_res.registerTempTable("dedupTopN")

func_dedup2_res = df_people.dedup_with_combiner(group_col=df_people.id, order_by_col=df_people.age, desc=True,
                               columns_filter=["name"], columns_filter_keep=False)
func_dedup2_res.registerTempTable("dedup_with_combiner")

func_changeSchema_res = df_people.change_schema(new_scheme=["id1", "name1", "age1"])
func_changeSchema_res.registerTempTable("changeSchema")

df_people2 = sqlContext.createDataFrame([
    ("a", "Laura", 34),
    ("a", "Stephani", 33),
    ("b", "Margaret", 36)],
    ["id", "name", "age"])

simpleDF = sqlContext.createDataFrame([
    ("a", "1")],
    ["id", "value"])
from pyspark.sql.functions import expr

func_joinSkewed_res = df_utils.join_skewed(df_left=df_people2.alias("df1"), df_right=simpleDF.alias("df2"),
                                           join_exprs=expr("df1.id == df2.id"), num_shards=5,
                                           join_type="inner")
func_joinSkewed_res.registerTempTable("joinSkewed")

func_broadcastJoinSkewed_res = df_utils.broadcast_join_skewed(not_skewed_df=df_people2, skewed_df=simpleDF, join_col="id",
                                                              number_of_custs_to_broadcast=5, filter_cnt=0, join_type="inner")
func_broadcastJoinSkewed_res.registerTempTable("broadcastJoinSkewed")

dfRange = sqlContext.createDataFrame([
    ("a", 34, 36)],
    ["id1", "start", "end"])
func_joinWithRange_res = df_utils.join_with_range(df_single=df_people2, col_single="age", df_range=dfRange,
                                                  col_range_start="start", col_range_end="end",
                                                  decrease_factor=5)
func_joinWithRange_res.registerTempTable("joinWithRange")

func_joinWithRangeAndDedup_res = df_utils.join_with_range_and_dedup(df_single=df_people2, col_single="age", df_range=dfRange,
                                                                    col_range_start="start", col_range_end="end",
                                                                    decrease_factor=5, dedup_small_range=True)
func_joinWithRangeAndDedup_res.registerTempTable("joinWithRangeAndDedup")

dfArray = sqlContext.createDataFrame([
    (0.0, ["Hi", "I heard", "about", "Spark"])],
    ["label", "sentence_arr"])

func_explodeArray_res = df_utils.explode_array(df=dfArray, array_col=dfArray.sentence_arr, alias="token")
func_explodeArray_res.registerTempTable("explodeArray")
