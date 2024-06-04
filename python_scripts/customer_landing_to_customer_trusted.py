import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1717487795825 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/customers/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1717487795825")

# Script generated for node Drop Non-Consenting
DropNonConsenting_node1717487921527 = Filter.apply(frame=CustomerLanding_node1717487795825, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="DropNonConsenting_node1717487921527")

# Script generated for node Amazon S3
AmazonS3_node1717488080223 = glueContext.write_dynamic_frame.from_options(frame=DropNonConsenting_node1717487921527, connection_type="s3", format="json", connection_options={"path": "s3://jwawss3testbucket/customers/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1717488080223")

job.commit()