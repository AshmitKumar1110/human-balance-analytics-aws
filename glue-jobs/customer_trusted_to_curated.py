import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET'])
S3_BUCKET = args['S3_BUCKET']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1783409800182 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-data",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1783409800182",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1783409797899 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-data",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1783409797899",
)

# Script generated for node Join Privacy
JoinPrivacy_node1783409837199 = Join.apply(
    frame1=AccelerometerLanding_node1783409797899,
    frame2=CustomerTrusted_node1783409800182,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinPrivacy_node1783409837199",
)

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct customername, email, phone, birthday,
serialnumber, registrationdate, lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
SQLQuery_node1783411614196 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": JoinPrivacy_node1783409837199},
    transformation_ctx="SQLQuery_node1783411614196",
)

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(
    frame=SQLQuery_node1783411614196,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1783409618569",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)

CustomerCurated_node1783409938198 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1783411614196,
    connection_type="s3",
    format="json",
    connection_options={
        "path": f"s3://{S3_BUCKET}/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1783409938198",
)

job.commit()
