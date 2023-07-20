from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def not_tags(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(~ col("loc").startswith(lit("https://docs.prophecy.io/tags")))
