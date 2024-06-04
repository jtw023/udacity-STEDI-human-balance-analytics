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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1717495390734 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1717495390734")

# Script generated for node Customer Trusted
CustomerTrusted_node1717491078297 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/customers/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1717491078297")

# Script generated for node Only Accelerometer Join
OnlyAccelerometerJoin_node1717491210216 = Join.apply(frame1=CustomerTrusted_node1717491078297, frame2=AccelerometerTrusted_node1717491250474, keys1=["email"], keys2=["user"], transformation_ctx="OnlyAccelerometerJoin_node1717491210216")

# Script generated for node Drop Fields from Accelerometer Join
DropFieldsfromAccelerometerJoin_node1717491713445 = DropFields.apply(frame=OnlyAccelerometerJoin_node1717491210216, paths=["z", "user", "y", "x", "timestamp"], transformation_ctx="DropFieldsfromAccelerometerJoin_node1717491713445")

# Script generated for node Drop Duplicates
DropDuplicates_node1717491662868 =  DynamicFrame.fromDF(DropFieldsfromAccelerometerJoin_node1717491713445.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1717491662868")

# Script generated for node Renamed keys for Inner Join
RenamedkeysforInnerJoin_node1717495519955 = ApplyMapping.apply(frame=DropDuplicates_node1717491662868, mappings=[("serialnumber", "string", "cc_serialnumber", "string"), ("birthday", "string", "birthday", "string"), ("registrationdate", "bigint", "registrationdate", "bigint"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"), ("customername", "string", "customername", "string"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "bigint"), ("email", "string", "customer_trusted_customer_curated_email", "string"), ("lastupdatedate", "bigint", "lastupdatedate", "bigint"), ("phone", "string", "customer_trusted_customer_curated_phone", "string"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "bigint")], transformation_ctx="RenamedkeysforInnerJoin_node1717495519955")

# Script generated for node Customer Curated and Step Trainer Landing
CustomerCuratedandStepTrainerLanding_node1717496299412 = Join.apply(frame1=StepTrainerLanding_node1717495390734, frame2=RenamedkeysforInnerJoin_node1717495519955, keys1=["serialnumber"], keys2=["cc_serialnumber"], transformation_ctx="CustomerCuratedandStepTrainerLanding_node1717496299412")

# Script generated for node Drop Fields from Customer Curated Join
DropFieldsfromCustomerCuratedJoin_node1717496722995 = DropFields.apply(frame=CustomerCuratedandStepTrainerLanding_node1717496299412, paths=[], transformation_ctx="DropFieldsfromCustomerCuratedJoin_node1717496722995")

# Script generated for node Step Trainer Trusted 
StepTrainerTrusted_node1717491875178 = glueContext.write_dynamic_frame.from_options(frame=DropFieldsfromCustomerCuratedJoin_node1717496722995, connection_type="s3", format="json", connection_options={"path": "s3://jwawss3testbucket/step_trainer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1717491875178")

job.commit()