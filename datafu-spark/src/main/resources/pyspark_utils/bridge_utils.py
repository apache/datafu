import os

from py4j.java_gateway import JavaGateway, GatewayClient
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# use jvm gateway to create a java class instance by full-qualified class name
def _getjvm_class(gateway, fullClassName):
    return gateway.jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(fullClassName).newInstance()


class Context(object):

    def __init__(self):
        from py4j.java_gateway import java_import
        """When running a Python script from Scala - this function is called
        by the script to initialize the connection to the Java Gateway and get the spark context.
        code is basically copied from: 
         https://github.com/apache/zeppelin/blob/master/spark/interpreter/src/main/resources/python/zeppelin_pyspark.py#L30
        """

        if os.environ.get("SPARK_EXECUTOR_URI"):
            SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

        gateway = JavaGateway(GatewayClient(port=int(os.environ.get("PYSPARK_GATEWAY_PORT"))), auto_convert=True)
        java_import(gateway.jvm, "org.apache.spark.SparkEnv")
        java_import(gateway.jvm, "org.apache.spark.SparkConf")
        java_import(gateway.jvm, "org.apache.spark.api.java.*")
        java_import(gateway.jvm, "org.apache.spark.api.python.*")
        java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
        java_import(gateway.jvm, "org.apache.spark.sql.*")
        java_import(gateway.jvm, "org.apache.spark.sql.hive.*")

        intp = gateway.entry_point

        jSparkSession = intp.pyGetSparkSession()
        jsc = intp.pyGetJSparkContext(jSparkSession)
        jconf = intp.pyGetSparkConf(jsc)
        conf = SparkConf(_jvm = gateway.jvm, _jconf = jconf)
        self.sc = SparkContext(jsc=jsc, gateway=gateway, conf=conf)

        # Spark 2
        self.sparkSession = SparkSession(self.sc, jSparkSession)
        self.sqlContext = self.sparkSession._wrapped

ctx = None


def get_contexts():
    global ctx
    if not ctx:
        ctx = Context()

    return ctx.sc, ctx.sqlContext, ctx.sparkSession
