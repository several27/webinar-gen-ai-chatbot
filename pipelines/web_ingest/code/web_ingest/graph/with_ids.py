from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def with_ids(spark: SparkSession, embed: DataFrame) -> DataFrame:
    return embed.select(
        col("content"), 
        col("url"), 
        concat(lit("web-"), monotonically_increasing_id()).alias("id"), 
        col("openai_embedding")
    )
