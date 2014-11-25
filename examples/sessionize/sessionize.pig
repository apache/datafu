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

REGISTER           '../../datafu-pig/build/libs/datafu-pig-1.2.1.jar';
 
DEFINE Sessionize  datafu.pig.sessions.Sessionize('10m');
DEFINE Median      datafu.pig.stats.Median();
DEFINE Quantile    datafu.pig.stats.StreamingQuantile('0.75','0.90','0.95');
DEFINE VAR         datafu.pig.stats.VAR();
 
pv = LOAD 'clicks.csv' USING PigStorage(',') AS (memberId:int, time:long, url:chararray);
 
pv = FOREACH pv
     GENERATE time,
              memberId;

pv_sessionized = FOREACH (GROUP pv BY memberId) {
  ordered = ORDER pv BY time;
  GENERATE FLATTEN(Sessionize(ordered)) AS (time, memberId, sessionId);
};
 
pv_sessionized = FOREACH pv_sessionized GENERATE sessionId, time;
 
-- compute length of each session in minutes
session_times = FOREACH (GROUP pv_sessionized BY sessionId)
                GENERATE group as sessionId,
                         (MAX(pv_sessionized.time)-MIN(pv_sessionized.time))
                            / 1000.0 / 60.0 as session_length;
 
-- compute stats on session length
session_stats = FOREACH (GROUP session_times ALL) {
  ordered = ORDER session_times BY session_length;
  GENERATE
    AVG(ordered.session_length) as avg_session,
    SQRT(VAR(ordered.session_length)) as std_dev_session,
    Median(ordered.session_length) as median_session,
    Quantile(ordered.session_length) as quantiles_session;
};
 
DUMP session_stats
--(15.737532575757575,31.29552045993877,(2.848041666666667),(14.648516666666666,31.88788333333333,86.69525))
