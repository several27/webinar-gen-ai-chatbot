from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def pinecone_target(spark: SparkSession, web_embeddings_1: DataFrame):
    from pyspark.sql.functions import expr, array, struct
    from spark_ai.dbs.pinecone import PineconeDB, IdVector
    from pyspark.dbutils import DBUtils
    PineconeDB(DBUtils(spark).secrets.get(scope = "pinecone", key = "token"), "us-east-1-aws").register_udfs(spark)
    web_embeddings_1\
        .withColumn("_row_num", row_number().over(Window.partitionBy().orderBy(col("id"))))\
        .withColumn("_group_num", ceil(col("_row_num") / 20))\
        .withColumn("_id_vector", struct(col("id"), col("openai_embedding").alias("vector")))\
        .groupBy(col("_group_num"))\
        .agg(collect_list(col("_id_vector")).alias("id_vectors"))\
        .withColumn("upserted", expr(f"pinecone_upsert(\"dais-all-vectors\", id_vectors)"))\
        .select(col("*"), col("upserted.*"))\
        .select(col("id_vectors"), col("count"), col("error"))\
        .count()
