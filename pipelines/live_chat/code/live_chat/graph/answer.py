from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def answer(spark: SparkSession, by_id: DataFrame) -> DataFrame:
    from spark_ai.llms.openai import OpenAiLLM
    from pyspark.dbutils import DBUtils
    OpenAiLLM(api_key = DBUtils(spark).secrets.get(scope = "open_ai", key = "token")).register_udfs(spark = spark)

    return by_id\
        .withColumn("_context", col("content"))\
        .withColumn("_query", col("text"))\
        .withColumn(
          "openai_answer",
          expr(
            "openai_answer_question(_context, _query, \" Answer the question based on the context below.\nContext:\n```\n{context}\n```\nQuestion: \n```\n{query}\n```\nAnswer:\n \")"
          )
        )\
        .drop("_context", "_query")
