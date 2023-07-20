from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def web_embeddings_1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"default.dais_content_2023_2")
