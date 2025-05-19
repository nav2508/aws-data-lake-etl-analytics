import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load raw JSON data
raw_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://my-data-lake-bucket25/raw/"]},
    format="json"
)

# Example transformation: rename and filter columns
processed_df = raw_df.rename_field("name", "full_name") \
                     .drop_fields(["unnecessary_column"])

# Write processed data
glueContext.write_dynamic_frame.from_options(
    frame=processed_df,
    connection_type="s3",
    connection_options={"path": "s3://my-data-lake-bucket25/processed/", "partitionKeys": ["year", "month"]},
    format="parquet"
)

job.commit()
