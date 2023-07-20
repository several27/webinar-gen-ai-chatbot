from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def web_embeddings(spark: SparkSession, with_ids: DataFrame):
    with_ids.write.format("delta").mode("overwrite").saveAsTable(f"default.dais_content_2023_2")
