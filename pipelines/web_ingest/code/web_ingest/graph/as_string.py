from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def as_string(spark: SparkSession, web_url: DataFrame) -> DataFrame:
    return web_url.select(col("content").cast(StringType()).alias("text"))
