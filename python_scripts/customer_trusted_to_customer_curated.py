import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1717491250474 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1717491250474")

# Script generated for node Customer Trusted
CustomerTrusted_node1717491078297 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/customers/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1717491078297")

# Script generated for node Only Accelerometer Join
OnlyAccelerometerJoin_node1717491210216 = Join.apply(frame1=CustomerTrusted_node1717491078297, frame2=AccelerometerTrusted_node1717491250474, keys1=["email"], keys2=["user"], transformation_ctx="OnlyAccelerometerJoin_node1717491210216")

# Script generated for node Drop Fields
DropFields_node1717491713445 = DropFields.apply(frame=OnlyAccelerometerJoin_node1717491210216, paths=["z", "user", "y", "x", "timestamp"], transformation_ctx="DropFields_node1717491713445")

# Script generated for node Drop Duplicates
DropDuplicates_node1717491662868 =  DynamicFrame.fromDF(DropFields_node1717491713445.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1717491662868")

# Script generated for node Customers Curated
CustomersCurated_node1717491875178 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1717491662868, connection_type="s3", format="json", connection_options={"path": "s3://jwawss3testbucket/customers/curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="CustomersCurated_node1717491875178")

job.commit()