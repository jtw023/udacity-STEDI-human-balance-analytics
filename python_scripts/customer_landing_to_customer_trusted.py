import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import re

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Remove PII
SqlQuery1927 = '''
SELECT
    serialNumber,
    birthDay,
    registrationDate,
    shareWithResearchAsOfDate,
    CONCAT(SUBSTRING(SPLIT_PART(customerName, ' ', 1), 0, 1), SUBSTRING(SPLIT_PART(customerName, ' ', 2), 0, 1)) AS customerInitials,
    shareWithFriendsAsOfDate,
    email,
    lastUpdateDate,
    phone,
    shareWithPublicAsOfDate
FROM myDataSource

'''
RemovePII_node1717515620498 = sparkSqlQuery(glueContext, query = SqlQuery1927, mapping = {"myDataSource":DropNonConsenting_node1717487921527}, transformation_ctx = "RemovePII_node1717515620498")

# Script generated for node Amazon S3
AmazonS3_node1717488080223 = glueContext.getSink(path="s3://jwawss3testbucket/customers/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1717488080223")
AmazonS3_node1717488080223.setCatalogInfo(catalogDatabase="customer trusted",catalogTableName="customer_catalog")
AmazonS3_node1717488080223.setFormat("json")
AmazonS3_node1717488080223.writeFrame(RemovePII_node1717515620498)
job.commit()