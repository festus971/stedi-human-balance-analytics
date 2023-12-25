import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job



def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1702472698251 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/landing1"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1702472698251",
)

# Script generated for node privacyFilter
privacyFilter_node1702472708587 = Filter.apply(
    frame=AmazonS3_node1702472698251,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="privacyFilter_node1702472708587",
)

# Script generated for node TrustedCustomerZone
TrustedCustomerZone_node1702472715806 = glueContext.write_dynamic_frame.from_options(
    frame=privacyFilter_node1702472708587,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/customer/landing1/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCustomerZone_node1702472715806",
)

job.commit()
