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
AmazonS3_node1702928052547 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1702928052547",
)

# Script generated for node Amazon S3
AmazonS3_node1702928042624 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi2",
    table_name="accelerometer_landing",
    transformation_ctx="AmazonS3_node1702928042624",
)

# Script generated for node customer privacy filter
customerprivacyfilter_node1702928055481 = Join.apply(
    frame1=AmazonS3_node1702928052547,
    frame2=AmazonS3_node1702928042624,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="customerprivacyfilter_node1702928055481",
)

# Script generated for node Drop Fields
DropFields_node1702928199015 = DropFields.apply(
    frame=customerprivacyfilter_node1702928055481,
    paths=[
        "customername",
        "email",
        "phone",
        "birthdate",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicsasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1702928199015",
)

# Script generated for node S3 bucket
S3bucket_node1702928058600 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1702928199015,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/accelerometer/landing1/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node1702928058600",
)

job.commit()
