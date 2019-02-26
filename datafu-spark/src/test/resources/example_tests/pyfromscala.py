
# print the PYTHONPATH
import sys
from pprint import pprint as p
p(sys.path)

from pyspark.sql import functions as F


import os
print os.getcwd()


###############################################################
# query scala defined DF
###############################################################
dfout = sqlContext.sql("select num * 2 as d from dfin")
dfout.registerTempTable("dfout")
dfout.groupBy(dfout['d']).count().show()
sqlContext.sql("select count(*) as cnt from dfout").show()
dfout.groupBy(dfout['d']).agg(F.count(F.col('d')).alias('cnt')).show()

sqlContext.sql("select d * 4 as d from dfout").registerTempTable("dfout2")


###############################################################
# check python UDFs
###############################################################

def magic_func(s):

    return s + " magic"

sqlContext.udf.register("magic", magic_func)


###############################################################
# check sc.textFile
###############################################################

DEL = '\x10'

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType

schema = StructType([
    StructField("A", StringType()),
    StructField("B", StringType())
])

txt_df = sqlContext.read.csv('src/test/resources/text.csv', sep=DEL, schema=schema)

print type(txt_df)
print dir(txt_df)
print txt_df.count()

txt_df.show()

txt_df2 = sc.textFile('src/test/resources/text.csv').map(lambda x: x.split(DEL)).toDF()
txt_df2.show()


###############################################################
# convert python dict to DataFrame
###############################################################

d = {'a': 0.1, 'b': 2}
d = [(k,1.0*d[k]) for k in d]
stats_df = sc.parallelize(d, 1).toDF(["name", "val"])
stats_df.registerTempTable('stats')

sqlContext.table("stats").show()


