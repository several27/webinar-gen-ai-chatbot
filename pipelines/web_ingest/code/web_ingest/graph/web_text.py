from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def web_text(spark: SparkSession, as_string: DataFrame):
    as_string.write.format("text").text("dbfs:/prophecy_data/web_index/", compression = None, lineSep = None)
