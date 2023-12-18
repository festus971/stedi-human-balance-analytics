import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1702624360762 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/accelerometer/landing1/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1702624360762",
)

# Script generated for node privacyFilter
privacyFilter_node1702624366760 = Filter.apply(
    frame=AmazonS3_node1702624360762,
    f=lambda row: (bool(re.match("email", row["user"]))),
    transformation_ctx="privacyFilter_node1702624366760",
)

# Script generated for node accelerometer_trusted_zone
accelerometer_trusted_zone_node1702624371360 = (
    glueContext.write_dynamic_frame.from_options(
        frame=privacyFilter_node1702624366760,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": "s3://stedi-lake-house/accelerometer/landing1/",
            "partitionKeys": [],
        },
        format_options={"compression": "snappy"},
        transformation_ctx="accelerometer_trusted_zone_node1702624371360",
    )
)

job.commit()
