from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *

def pick_answer(spark: SparkSession, OpenAI_1: DataFrame) -> DataFrame:
    return OpenAI_1.select(col("channel"), col("ts"), col("openai_answer.choices")[0].alias("answer"))
