from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def is_not_bot(spark: SparkSession, slack_source: DataFrame) -> DataFrame:
    return slack_source.filter((col("user") != lit("U05AU1K4ELV")))
