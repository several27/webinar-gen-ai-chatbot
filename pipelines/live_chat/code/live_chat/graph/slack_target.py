from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def slack_target(spark: SparkSession, pick_answer: DataFrame):
    from pyspark.dbutils import DBUtils
    from spark_ai.webapps.slack import SlackUtilities
    SlackUtilities(token = DBUtils(spark).secrets.get(scope = "slack", key = "token"), spark = spark)\
        .write_messages(pick_answer)
