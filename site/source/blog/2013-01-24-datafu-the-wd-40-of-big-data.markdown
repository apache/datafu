---
title: DataFu, The WD-40 of Big Data
author: Matthew Hayes, Sam Shah
license: >
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
---

If Pig is the “[duct tape for big data](http://blog.linkedin.com/2010/07/01/linkedin-apache-pig/)“, then DataFu is the WD-40. Or something.

No, seriously, DataFu is a collection of Pig UDFs for data analysis on Hadoop. DataFu includes routines for common statistics tasks (e.g., median, variance), PageRank, set operations, and bag operations.

It’s helpful to understand the history of the library. Over the years, we developed several routines that were used across LinkedIn and were thrown together into an internal package we affectionately called “littlepiggy.” The unfortunate part, and this is true of many such efforts, is that the UDFs were ill-documented, ill-organized, and easily got broken when someone made a change. Along came [PigUnit](http://pig.apache.org/docs/r0.10.0/test.html#pigunit), which allowed UDF testing, so we spent the time to clean up these routines by adding documentation and rigorous unit tests. From this “datafoo” package, we thought this would help the community at large, and there you have DataFu.

So what can this library do for you? Let’s look at one of the classical examples that showcase the power and flexibility of Pig: sessionizing a click stream.

<pre>
<code>
A = load ‘clicks’;
B = group A by user;
C = foreach B {
  C1 = order A by timestamp;
  generate user, <em>Sessonize</em>(C1);
}
D = group C by session_id;
E = foreach D generate group as session_id, (MAX(C.timestamp) - MIN(C.timestamp)) as session_length;
F = group E all;
G = foreach F generate
  AVG(E.session_length) as avg_session_length,
  SQRT(<em>VAR</em>(E.session_length)) as sd_session_length,
  <em>MEDIAN</em>(E.session_length) as median_session_length,
  <em>Q75</em>(E.session_length) as session_length_75pct,
  <em>Q90</em>(E.session_length) as session_length_90pct,
  <em>Q95</em>(E.session_length) as session_length_95pct;
</code>
</pre>

(In fact, this is basically the example for the Accumulator interface that was added in Pig 0.6.)

Here, we’re just computing some summary statistics on a sessionized click stream. Pig does the heavy lifting of transforming your query into MapReduce goodness, but DataFu fills in the gaps by providing the missing routines for every italicized function.

You can grab sample data and code you can run on your own for this sessionization example below.

### Sessionization Example

Suppose that we have a stream of page views from which we have extracted a member ID and UNIX timestamp. It might look something like this:

    memberId timestamp      url
    1        1357718725941  /
    1        1357718871442  /profile
    1        1357719038706  /inbox
    1        1357719110742  /groups
    ...
    2        1357752955401  /inbox
    2        1357752982385  /profile
    ...

The full data set for this example can be found [here](https://gist.github.com/raw/4614332/8231534822295e4626af75b3341239177ec44fbe/clicks.csv).

Using DataFu we can assign session IDs to each of these events and group by session ID in order to compute the length of each session. From there we can complete the exercise by simply applying the statistics UDFs provided by DataFu.

```pig
REGISTER piggybank.jar;
REGISTER datafu-0.0.6.jar;
REGISTER guava-13.0.1.jar; -- needed by StreamingQuantile

DEFINE UnixToISO   org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE Sessionize  datafu.pig.sessions.Sessionize('10m');
DEFINE Median      datafu.pig.stats.Median();
DEFINE Quantile    datafu.pig.stats.StreamingQuantile('0.75','0.90','0.95');
DEFINE VAR         datafu.pig.stats.VAR();

pv = LOAD 'clicks.csv' USING PigStorage(',') AS (memberId:int, time:long, url:chararray);

pv = FOREACH pv
     -- Sessionize expects an ISO string
     GENERATE UnixToISO(time) as isoTime,
              time,
              memberId;

pv_sessionized = FOREACH (GROUP pv BY memberId) {
  ordered = ORDER pv BY isoTime;
  GENERATE FLATTEN(Sessionize(ordered)) AS (isoTime, time, memberId, sessionId);
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
```

This is just a taste. There’s plenty more in the library for you to peruse. Take a look [here](/docs/datafu/guide.html). DataFu is freely available under the Apache 2 license. We welcome contributions, so please send us your pull requests!