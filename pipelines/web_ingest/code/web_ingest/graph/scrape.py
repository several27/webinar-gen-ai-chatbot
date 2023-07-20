from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *

def scrape(spark: SparkSession, not_tags: DataFrame) -> DataFrame:
    from pyspark.sql.functions import expr, array, struct
    from spark_ai.webapps import WebUtils
    WebUtils().register_udfs(spark)

    return not_tags.withColumn("result_content", expr(f"web_scrape_text(loc)"))
