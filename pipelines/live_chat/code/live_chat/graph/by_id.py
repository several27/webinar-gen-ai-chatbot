from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def by_id(spark: SparkSession, flat: DataFrame, in1: DataFrame, ) -> DataFrame:
    return flat.alias("flat").join(in1.alias("in1"), (col("flat.pinecone_id") == col("in1.id")), "inner")
