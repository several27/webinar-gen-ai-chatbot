from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def lookup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import expr, array, struct
    from spark_ai.dbs.pinecone import PineconeDB, IdVector
    from pyspark.dbutils import DBUtils
    PineconeDB(DBUtils(spark).secrets.get(scope = "pinecone", key = "token"), "us-east-1-aws").register_udfs(spark)

    return in0\
        .withColumn("_vector", col("openai_embedding"))\
        .withColumn("_response", expr(f"pinecone_query(\"dais-all-vectors\", _vector, {1})"))\
        .withColumn("pinecone_matches", col("_response.matches"))\
        .withColumn("pinecone_error", col("_response.error"))\
        .drop("_vector", "_response")
