import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, year, month, dayofmonth

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the nested JSON files
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://coincap-data/data/"],
        "recurse": True
    },
    format="json",
    transformation_ctx="datasource0"
)

# Flatten the data
df = datasource0.toDF()
df_flattened = df.selectExpr("explode(data) as data_item", "timestamp").selectExpr(
    "data_item.id as id",
    "data_item.rank as rank",
    "data_item.symbol as symbol",
    "data_item.name as name",
    "data_item.supply as supply",
    "data_item.maxSupply as maxSupply",
    "data_item.marketCapUsd as marketCapUsd",
    "data_item.volumeUsd24Hr as volumeUsd24Hr",
    "data_item.priceUsd as priceUsd",
    "data_item.changePercent24Hr as changePercent24Hr",
    "data_item.vwap24Hr as vwap24Hr",
    "data_item.explorer as explorer",
    "timestamp"
)

# Ensure the timestamp is of TimestampType
df_flattened = df_flattened.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Extract year, month, and day for partitioning
df_flattened = df_flattened.withColumn("year", year(col("timestamp"))) \
                           .withColumn("month", month(col("timestamp"))) \
                           .withColumn("day", dayofmonth(col("timestamp")))

# Write the flattened data back to S3 with partitioning
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(df_flattened, glueContext, "df_flattened"),
    connection_type="s3",
    connection_options={
        "path": "s3://coincap-data/flattaned_data/",
        "partitionKeys": ["year", "month", "day"]
    },
    format="parquet",
    transformation_ctx="datasink0"
)

job.commit()