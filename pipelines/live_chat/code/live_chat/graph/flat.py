from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def flat(spark: SparkSession, lookup: DataFrame) -> DataFrame:
    flt_col = lookup.withColumn("pinecone_matches", explode_outer("pinecone_matches")).columns
    selectCols = [col("pinecone_id") if "pinecone_id" in flt_col else col("pinecone_matches.id").alias("pinecone_id"),                   col("ts") if "ts" in flt_col else col("ts"),                   col("channel") if "channel" in flt_col else col("channel"),                   col("text") if "text" in flt_col else col("text")]

    return lookup.withColumn("pinecone_matches", explode_outer("pinecone_matches")).select(*selectCols)
