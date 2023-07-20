from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def web_embeddings(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"default.dais_content_2023_2")
