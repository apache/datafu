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

from pyspark_utils.df_utils import PySparkDFUtils

p('CHECKING IF PATHS EXISTS:')
for x in sys.path:
    p('PATH ' + x + ': ' + str(os.path.exists(x)))


df_utils = PySparkDFUtils()

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

func_dedup_res = df_utils.dedup(dataFrame=df_people, groupCol=df_people.id,
                             orderCols=[df_people.age.desc(), df_people.name.desc()])
func_dedup_res.registerTempTable("dedup")

func_dedupTopN_res = df_utils.dedupTopN(dataFrame=df_people, n=2, groupCol=df_people.id,
                                     orderCols=[df_people.age.desc(), df_people.name.desc()])
func_dedupTopN_res.registerTempTable("dedupTopN")

func_dedup2_res = df_utils.dedup2(dataFrame=df_people, groupCol=df_people.id, orderByCol=df_people.age, desc=True,
                               columnsFilter=["name"], columnsFilterKeep=False)
func_dedup2_res.registerTempTable("dedup2")

func_changeSchema_res = df_utils.changeSchema(dataFrame=df_people, newScheme=["id1", "name1", "age1"])
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

func_joinSkewed_res = df_utils.joinSkewed(dfLeft=df_people2.alias("df1"), dfRight=simpleDF.alias("df2"),
                                       joinExprs=expr("df1.id == df2.id"), numShards=5,
                                       joinType="inner")
func_joinSkewed_res.registerTempTable("joinSkewed")

func_broadcastJoinSkewed_res = df_utils.broadcastJoinSkewed(notSkewed=df_people2, skewed=simpleDF, joinCol="id",
                                                         numberCustsToBroadcast=5)
func_broadcastJoinSkewed_res.registerTempTable("broadcastJoinSkewed")

dfRange = sqlContext.createDataFrame([
    ("a", 34, 36)],
    ["id1", "start", "end"])
func_joinWithRange_res = df_utils.joinWithRange(dfSingle=df_people2, colSingle="age", dfRange=dfRange,
                                             colRangeStart="start", colRangeEnd="end",
                                             decreaseFactor=5)
func_joinWithRange_res.registerTempTable("joinWithRange")

func_joinWithRangeAndDedup_res = df_utils.joinWithRangeAndDedup(dfSingle=df_people2, colSingle="age", dfRange=dfRange,
                                                  colRangeStart="start", colRangeEnd="end",
                                                  decreaseFactor=5, dedupSmallRange=True)
func_joinWithRangeAndDedup_res.registerTempTable("joinWithRangeAndDedup")
