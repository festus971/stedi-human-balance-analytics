import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1703396830024 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1703396830024",
)

# Script generated for node Amazon S3
AmazonS3_node1703396831075 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing",
    transformation_ctx="AmazonS3_node1703396831075",
)

# Script generated for node Join
Join_node1703396840695 = Join.apply(
    frame1=AmazonS3_node1703396831075,
    frame2=customer_trusted_node1703396830024,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1703396840695",
)

# Script generated for node Drop Fields
DropFields_node1703396854163 = DropFields.apply(
    frame=Join_node1703396840695,
    paths=["user", "x", "y", "z"],
    transformation_ctx="DropFields_node1703396854163",
)

# Script generated for node customer_curated
customer_curated_node1703396866982 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703396854163,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/customer/landing1/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node1703396866982",
)

job.commit()
