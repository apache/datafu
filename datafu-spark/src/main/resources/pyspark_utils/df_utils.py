from pyspark.sql import DataFrame

from pyspark_utils.bridge_utils import _getjvm_class


class PySparkDFUtils(object):

    def __init__(self):
        self._sc = None

    def _initSparkContext(self, sc, sqlContext):
        self._sc = sc
        self._sqlContext = sqlContext
        self._gateway = sc._gateway

    def _get_jvm_spark_utils(self):
        jvm_utils = _getjvm_class(self._gateway, "datafu.spark.SparkDFUtilsBridge")
        return jvm_utils

    # public:

    def dedup(self, dataFrame, groupCol, orderCols = []):
        """
        Used get the 'latest' record (after ordering according to the provided order columns) in each group.
        :param dataFrame: DataFrame to operate on
        :param groupCol: column to group by the records
        :param orderCols: columns to order the records according to.
        :return: DataFrame representing the data after the operation
        """
        self._initSparkContext(dataFrame._sc, dataFrame.sql_ctx)
        java_cols = self._cols_to_java_cols(orderCols)
        jdf = self._get_jvm_spark_utils().dedup(dataFrame._jdf, groupCol._jc, java_cols)
        return DataFrame(jdf, self._sqlContext)

    def dedupTopN(self, dataFrame, n, groupCol, orderCols = []):
        """
        Used get the top N records (after ordering according to the provided order columns) in each group.
        :param dataFrame: DataFrame to operate on
        :param n: number of records to return from each group
        :param groupCol: column to group by the records
        :param orderCols: columns to order the records according to
        :return: DataFrame representing the data after the operation
        """
        self._initSparkContext(dataFrame._sc, dataFrame.sql_ctx)
        java_cols = self._cols_to_java_cols(orderCols)
        jdf = self._get_jvm_spark_utils().dedupTopN(dataFrame._jdf, n, groupCol._jc, java_cols)
        return DataFrame(jdf, self._sqlContext)

    def dedup2(self, dataFrame, groupCol, orderByCol, desc = True, columnsFilter = [], columnsFilterKeep = True):
        """
        Used get the 'latest' record (after ordering according to the provided order columns) in each group.
        :param dataFrame: DataFrame to operate on
        :param groupCol: column to group by the records
        :param orderByCol: column to order the records according to
        :param desc: have the order as desc
        :param columnsFilter: columns to filter
        :param columnsFilterKeep: indicates whether we should filter the selected columns 'out' or alternatively have only
    *                          those columns in the result
        :return: DataFrame representing the data after the operation
        """
        self._initSparkContext(dataFrame._sc, dataFrame.sql_ctx)
        jdf = self._get_jvm_spark_utils().dedup2(dataFrame._jdf, groupCol._jc, orderByCol._jc, desc, columnsFilter, columnsFilterKeep)
        return DataFrame(jdf, self._sqlContext)

    def changeSchema(self, dataFrame, newScheme = []):
        """
        Returns a DataFrame with the column names renamed to the column names in the new schema
        :param dataFrame: DataFrame to operate on
        :param newScheme: new column names
        :return: DataFrame representing the data after the operation
        """
        self._initSparkContext(dataFrame._sc, dataFrame.sql_ctx)
        jdf = self._get_jvm_spark_utils().changeSchema(dataFrame._jdf, newScheme)
        return DataFrame(jdf, self._sqlContext)

    def joinSkewed(self, dfLeft, dfRight, joinExprs, numShards = 30, joinType= "inner"):
        """
        Used to perform a join when the right df is relatively small but doesn't fit to perform broadcast join.
        Use cases:
            a. excluding keys that might be skew from a medium size list.
            b. join a big skewed table with a table that has small number of very big rows.
        :param dfLeft: left DataFrame
        :param dfRight: right DataFrame
        :param joinExprs: join expression
        :param numShards: number of shards
        :param joinType: join type
        :return: DataFrame representing the data after the operation
        """
        self._initSparkContext(dfLeft._sc, dfLeft.sql_ctx)
        utils = self._get_jvm_spark_utils()
        jdf = utils.joinSkewed(dfLeft._jdf, dfRight._jdf, joinExprs._jc, numShards, joinType)
        return DataFrame(jdf, self._sqlContext)

    def broadcastJoinSkewed(self, notSkewed, skewed, joinCol, numberCustsToBroadcast):
        """
        Suitable to perform a join in cases when one DF is skewed and the other is not skewed.
        splits both of the DFs to two parts according to the skewed keys.
        1. Map-join: broadcasts the skewed-keys part of the not skewed DF to the skewed-keys part of the skewed DF
        2. Regular join: between the remaining two parts.
        :param notSkewed: not skewed DataFrame
        :param skewed: skewed DataFrame
        :param joinCol: join column
        :param numberCustsToBroadcast: number of custs to broadcast
        :return: DataFrame representing the data after the operation
        """
        self._initSparkContext(skewed._sc, skewed.sql_ctx)
        jdf = self._get_jvm_spark_utils().broadcastJoinSkewed(notSkewed._jdf, skewed._jdf, joinCol, numberCustsToBroadcast)
        return DataFrame(jdf, self._sqlContext)

    def joinWithRange(self, dfSingle, colSingle, dfRange, colRangeStart, colRangeEnd, decreaseFactor):
        """
        Helper function to join a table with column to a table with range of the same column.
        For example, ip table with whois data that has range of ips as lines.
        The main problem which this handles is doing naive explode on the range can result in huge table.
        requires:
        1. single table needs to be distinct on the join column, because there could be a few corresponding ranges so we dedup at the end - we choose the minimal range.
        2. the range and single columns to be numeric.
        """
        self._initSparkContext(dfSingle._sc, dfSingle.sql_ctx)
        jdf = self._get_jvm_spark_utils().joinWithRange(dfSingle._jdf, colSingle, dfRange._jdf, colRangeStart, colRangeEnd, decreaseFactor)
        return DataFrame(jdf, self._sqlContext)

    def joinWithRangeAndDedup(self, dfSingle, colSingle, dfRange, colRangeStart, colRangeEnd, decreaseFactor, dedupSmallRange):
        """
        Helper function to join a table with column to a table with range of the same column.
        For example, ip table with whois data that has range of ips as lines.
        The main problem which this handles is doing naive explode on the range can result in huge table.
        requires:
        1. single table needs to be distinct on the join column, because there could be a few corresponding ranges so we dedup at the end - we choose the minimal range.
        2. the range and single columns to be numeric.
        """
        self._initSparkContext(dfSingle._sc, dfSingle.sql_ctx)
        jdf = self._get_jvm_spark_utils().joinWithRangeAndDedup(dfSingle._jdf, colSingle, dfRange._jdf, colRangeStart, colRangeEnd, decreaseFactor, dedupSmallRange)
        return DataFrame(jdf, self._sqlContext)

    def _cols_to_java_cols(self, cols):
        return self._map_if_needed(lambda x: x._jc, cols)

    def _dfs_to_java_dfs(self, dfs):
        return self._map_if_needed(lambda x: x._jdf, dfs)

    def _map_if_needed(self, func, itr):
        return map(func, itr) if itr is not None else itr

