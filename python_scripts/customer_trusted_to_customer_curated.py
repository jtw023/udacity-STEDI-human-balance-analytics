import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1717491250474 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1717491250474")

# Script generated for node Customer Landing
CustomerLanding_node1717517901802 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/customers/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1717517901802")

# Script generated for node Remove Non-Consensual
SqlQuery1684 = '''
SELECT
    *
FROM myDataSource
WHERE shareWithResearchAsOfDate IS NOT NULL

'''
RemoveNonConsensual_node1717519529420 = sparkSqlQuery(glueContext, query = SqlQuery1684, mapping = {"myDataSource":CustomerLanding_node1717517901802}, transformation_ctx = "RemoveNonConsensual_node1717519529420")

# Script generated for node Only Accelerometer Join
OnlyAccelerometerJoin_node1717491210216 = Join.apply(frame1=AccelerometerTrusted_node1717491250474, frame2=RemoveNonConsensual_node1717519529420, keys1=["user"], keys2=["email"], transformation_ctx="OnlyAccelerometerJoin_node1717491210216")

# Script generated for node Remove PII
SqlQuery1685 = '''
SELECT
    serialNumber,
    birthDay,
    registrationDate,
    shareWithResearchAsOfDate,
    CONCAT(SUBSTRING(SPLIT_PART(customerName, ' ', 1), 0, 1), SUBSTRING(SPLIT_PART(customerName, ' ', 2), 0, 1)) AS customerInitials,
    shareWithFriendsAsOfDate,
    SPLIT_PART(email, '@', 2) AS emailDomain,
    lastUpdateDate,
    phone,
    shareWithPublicAsOfDate
FROM myDataSource

'''
RemovePII_node1717518131144 = sparkSqlQuery(glueContext, query = SqlQuery1685, mapping = {"myDataSource":OnlyAccelerometerJoin_node1717491210216}, transformation_ctx = "RemovePII_node1717518131144")

# Script generated for node Drop Duplicates
DropDuplicates_node1717491662868 =  DynamicFrame.fromDF(RemovePII_node1717518131144.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1717491662868")

# Script generated for node Customers Curated
CustomersCurated_node1717491875178 = glueContext.getSink(path="s3://jwawss3testbucket/customers/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomersCurated_node1717491875178")
CustomersCurated_node1717491875178.setCatalogInfo(catalogDatabase="customer curated",catalogTableName="customer_catalog")
CustomersCurated_node1717491875178.setFormat("json")
CustomersCurated_node1717491875178.writeFrame(DropDuplicates_node1717491662868)
job.commit()