import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

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
CustomerLanding_node1717488763241 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/customers/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1717488763241")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1717487795825 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1717487795825")

# Script generated for node Accelerometer to Customer Join
AccelerometertoCustomerJoin_node1717488816137 = Join.apply(frame1=CustomerLanding_node1717488763241, frame2=AccelerometerLanding_node1717487795825, keys1=["email"], keys2=["user"], transformation_ctx="AccelerometertoCustomerJoin_node1717488816137")

# Script generated for node SQL Remove Non-Consent
SqlQuery1692 = '''
SELECT
    myDataSource.user,
    myDataSource.timestamp,
    x,
    y,
    z
FROM myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
'''
SQLRemoveNonConsent_node1717489716736 = sparkSqlQuery(glueContext, query = SqlQuery1692, mapping = {"myDataSource":AccelerometertoCustomerJoin_node1717488816137}, transformation_ctx = "SQLRemoveNonConsent_node1717489716736")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1717488080223 = glueContext.getSink(path="s3://jwawss3testbucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1717488080223")
AccelerometerTrusted_node1717488080223.setCatalogInfo(catalogDatabase="accelerometer trusted",catalogTableName="accelerometer_catalog")
AccelerometerTrusted_node1717488080223.setFormat("json")
AccelerometerTrusted_node1717488080223.writeFrame(SQLRemoveNonConsent_node1717489716736)
job.commit()
