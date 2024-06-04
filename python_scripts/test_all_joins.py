import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer
StepTrainer_node1717500756340 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainer_node1717500756340")

# Script generated for node Customer
Customer_node1717501902832 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/customers/landing/"], "recurse": True}, transformation_ctx="Customer_node1717501902832")

# Script generated for node Accelerometer
Accelerometer_node1717501575538 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jwawss3testbucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometer_node1717501575538")

# Script generated for node Renamed keys for Step Trainer and Customer
RenamedkeysforStepTrainerandCustomer_node1717502057503 = ApplyMapping.apply(frame=Customer_node1717501902832, mappings=[("customername", "string", "cc_customername", "string"), ("email", "string", "cc_email", "string"), ("phone", "string", "cc_phone", "string"), ("birthday", "string", "cc_birthday", "string"), ("serialnumber", "string", "cc_serialnumber", "string"), ("registrationdate", "long", "cc_registrationdate", "long"), ("lastupdatedate", "long", "cc_lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "cc_sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "cc_sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "cc_sharewithfriendsasofdate", "long")], transformation_ctx="RenamedkeysforStepTrainerandCustomer_node1717502057503")

# Script generated for node Step Trainer and Accelerometer
StepTrainerandAccelerometer_node1717501654061 = Join.apply(frame1=StepTrainer_node1717500756340, frame2=Accelerometer_node1717501575538, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="StepTrainerandAccelerometer_node1717501654061")

# Script generated for node Step Trainer and Customer
StepTrainerandCustomer_node1717502010798 = Join.apply(frame1=StepTrainer_node1717500756340, frame2=RenamedkeysforStepTrainerandCustomer_node1717502057503, keys1=["serialnumber"], keys2=["cc_serialnumber"], transformation_ctx="StepTrainerandCustomer_node1717502010798")

# Script generated for node Customer and Accelerometer
CustomerandAccelerometer_node1717502313200 = Join.apply(frame1=Accelerometer_node1717501575538, frame2=RenamedkeysforStepTrainerandCustomer_node1717502057503, keys1=["user"], keys2=["cc_email"], transformation_ctx="CustomerandAccelerometer_node1717502313200")

job.commit()