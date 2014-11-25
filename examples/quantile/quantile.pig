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

define ExactQuartile datafu.pig.stats.Quantile('0.0','0.25','0.5','0.75','1.0');
 
temperature = LOAD 'temperature.tsv' AS (id:chararray, temp:double);
temperature = GROUP temperature BY id;
 
quartiles_slow = FOREACH temperature {
  -- sort is necessary, because exact
  sorted = ORDER temperature by temp;
  GENERATE group as id, COUNT_STAR(temperature) AS n_recs, ExactQuartile(sorted.temp) AS qvals:tuple(q0,q1,q2,q3,q4);
};
DESCRIBE quartiles_slow;
 
rmf                        /tmp/quartiles-exact;
STORE quartiles_slow INTO '/tmp/quartiles-exact';
