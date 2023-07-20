from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def flatten(spark: SparkSession, chunkify: DataFrame) -> DataFrame:
    flt_col = chunkify.withColumn("result_chunks", explode_outer("result_chunks")).columns
    selectCols = [col("content") if "content" in flt_col else col("result_chunks").alias("content"),                   col("url") if "url" in flt_col else col("loc").alias("url")]

    return chunkify.withColumn("result_chunks", explode_outer("result_chunks")).select(*selectCols)
