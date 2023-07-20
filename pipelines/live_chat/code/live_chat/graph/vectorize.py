from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def vectorize(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from spark_ai.llms.openai import OpenAiLLM
    from pyspark.dbutils import DBUtils
    OpenAiLLM(api_key = DBUtils(spark).secrets.get(scope = "open_ai", key = "token")).register_udfs(spark = spark)

    return in0\
        .withColumn("_texts", array(col("text")))\
        .withColumn("_embedded", expr("openai_embed_texts(_texts)"))\
        .withColumn("openai_embedding", expr("_embedded.embeddings[0]"))\
        .withColumn("openai_error", col("_embedded.error"))\
        .drop("_texts", "_embedded")
