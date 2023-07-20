from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def slack_source(spark: SparkSession) -> DataFrame:
    from pyspark.dbutils import DBUtils

    return spark.readStream\
        .format("io.prophecy.spark_ai.webapps.slack.SlackSourceProvider")\
        .option("token", DBUtils(spark).secrets.get(scope = "slack", key = "app_token"))\
        .load()
