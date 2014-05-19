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
