from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from web_ingest.config.ConfigStore import *
from web_ingest.udfs.UDFs import *
from prophecy.utils import *
from web_ingest.graph import *

def pipeline(spark: SparkSession) -> None:
    df_web_url = web_url(spark)
    df_as_string = as_string(spark, df_web_url)
    df_web_xml = web_xml(spark)
    df_not_tags = not_tags(spark, df_web_xml)
    df_scrape = scrape(spark, df_not_tags)
    df_chunkify = chunkify(spark, df_scrape)
    df_web_embeddings_1 = web_embeddings_1(spark)
    df_flatten = flatten(spark, df_chunkify)
    df_embed = embed(spark, df_flatten)
    df_with_ids = with_ids(spark, df_embed)
    web_embeddings(spark, df_with_ids)
    pinecone_target(spark, df_web_embeddings_1)
    web_text(spark, df_as_string)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/web_ingest")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/web_ingest")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
