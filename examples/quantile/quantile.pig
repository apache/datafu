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
