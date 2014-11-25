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

REGISTER '../../datafu-pig/build/libs/datafu-pig-1.2.1.jar';

DEFINE ExactQuartile     datafu.pig.stats.Quantile(         '5');
DEFINE ApproxQuartile    datafu.pig.stats.StreamingQuantile('5');
-- Similar to (with different field names):
-- DEFINE ExactQuartile  datafu.pig.stats.Quantile(          '0.0', '0.25', '0.50', '0.75', '1.0' );
-- DEFINE ApproxQuartile datafu.pig.stats.StreamingQuantile( '0.0', '0.25', '0.50', '0.75', '1.0' );

DEFINE FirstTupleFromBag datafu.pig.bags.FirstTupleFromBag;

temperature = LOAD 'temperature.tsv' AS (id:chararray, temp:double);
temperature = GROUP temperature BY id;

quartiles_fast = FOREACH temperature {
  -- sort not necessary, because streaming
  GENERATE group as id, COUNT_STAR(temperature) AS n_recs, ApproxQuartile(temperature.temp) AS qvals:tuple(q0,q1,q2,q3,q4);
};
DESCRIBE quartiles_fast;

quartiles_slow = FOREACH temperature {
  -- sort is necessary, because exact
  sorted = ORDER temperature by temp;
  GENERATE group as id, COUNT_STAR(temperature) AS n_recs, ExactQuartile(sorted.temp) AS qvals:tuple(q0,q1,q2,q3,q4);
};
DESCRIBE quartiles_slow;

--
-- Group the two results together and compare.
-- The differences are in the range of 0.5% to 1.5%
--
quartiles_diff = FOREACH (COGROUP quartiles_slow BY id, quartiles_fast BY id) {
  count_fast = FirstTupleFromBag(quartiles_fast.n_recs, null);
  count_slow = FirstTupleFromBag(quartiles_fast.n_recs, null);
  qvals_fast = FirstTupleFromBag(quartiles_fast.qvals,  null).qvals;
  qvals_slow = FirstTupleFromBag(quartiles_slow.qvals,  null).qvals;

  GENERATE group AS id, count_fast.n_recs, qvals_slow AS qvals,
    ( (qvals_fast.q0 - qvals_slow.q0) / qvals_slow.q0,
      (qvals_fast.q1 - qvals_slow.q1) / qvals_slow.q1,
      (qvals_fast.q2 - qvals_slow.q2) / qvals_slow.q2,
      (qvals_fast.q3 - qvals_slow.q3) / qvals_slow.q3,
      (qvals_fast.q4 - qvals_slow.q4) / qvals_slow.q4 ) AS diffs:tuple(dq0,dq1,dq2,dq3,dq4)
    ;
};
DESCRIBE quartiles_diff;

rmf                        /tmp/quartiles-approx;
STORE quartiles_fast INTO '/tmp/quartiles-approx';

rmf                        /tmp/quartiles-diff;
STORE quartiles_diff INTO '/tmp/quartiles-diff';
