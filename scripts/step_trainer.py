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

# Script generated for node Amazon S3
AmazonS3_node1702622242842 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/step_trainer/landing1/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1702622242842",
)

# Script generated for node privacyFilter
privacyFilter_node1702622248136 = Filter.apply(
    frame=AmazonS3_node1702622242842,
    f=lambda row: (not (row["sensorReadingTime"] == 0)),
    transformation_ctx="privacyFilter_node1702622248136",
)

# Script generated for node Trusted Step_trainer Zone
TrustedStep_trainerZone_node1702622254217 = (
    glueContext.write_dynamic_frame.from_options(
        frame=privacyFilter_node1702622248136,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": "s3://stedi-lake-house/step_trainer/landing1/",
            "partitionKeys": [],
        },
        format_options={"compression": "snappy"},
        transformation_ctx="TrustedStep_trainerZone_node1702622254217",
    )
)

job.commit()
