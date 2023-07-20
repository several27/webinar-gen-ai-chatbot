from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from live_chat.config.ConfigStore import *
from live_chat.udfs.UDFs import *
from prophecy.utils import *
from live_chat.graph import *

def pipeline(spark: SparkSession) -> None:
    df_slack_source = slack_source(spark)
    df_is_not_bot = is_not_bot(spark, df_slack_source)
    df_vectorize = vectorize(spark, df_is_not_bot)
    df_lookup = lookup(spark, df_vectorize)
    df_flat = flat(spark, df_lookup)
    df_web_embeddings = web_embeddings(spark)
    df_by_id = by_id(spark, df_flat, df_web_embeddings)
    df_answer = answer(spark, df_by_id)
    df_pick_answer = pick_answer(spark, df_answer)
    slack_target(spark, df_pick_answer)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/live_chat")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/live_chat")
    pipeline(spark)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
