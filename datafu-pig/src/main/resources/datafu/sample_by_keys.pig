/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Macro for sampling a table by a list of keys.
 *
 * Params:
 *   - table_name               - table name to sample
 *   - sample_set               - a set of keys
 *   - join_key_table           - join column name in the table
 *   - join_key_sample          - join column name in the sample
 */
DEFINE sample_by_keys(table, sample_set, join_key_table, join_key_sample) RETURNS out {
    t = FOREACH $table GENERATE
        $join_key_table AS join_key,
        TOTUPLE(*)      AS original;

    s = FOREACH $sample_set GENERATE $join_key_sample;
    sd = DISTINCT s;

    joined = JOIN   t       BY join_key,
                    sd      BY $join_key_sample USING 'replicated';

    flat = FOREACH joined GENERATE FLATTEN(original);

    -- as the previous is a map only job, this row makes sure we use reducers so there won't be many output files
    $out = ORDER flat BY $join_key_table PARALLEL 1;
};
