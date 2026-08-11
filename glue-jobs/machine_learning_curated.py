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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1783426912372 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-data",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1783426912372",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1783426914066 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-data",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1783426914066",
)

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.timestamp,
    a.user,
    a.x,
    a.y,
    a.z
FROM s
INNER JOIN a
ON s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1783427197104 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "s": step_trainer_trusted_node1783426912372,
        "a": accelerometer_trusted_node1783426914066,
    },
    transformation_ctx="SQLQuery_node1783427197104",
)

# Script generated for node Mapping Learning Curated
EvaluateDataQuality().process_rows(
    frame=SQLQuery_node1783427197104,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1783426907061",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)

MappingLearningCurated_node1783427295793 = glueContext.getSink(
    path=f"s3://{S3_BUCKET}/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MappingLearningCurated_node1783427295793",
)
MappingLearningCurated_node1783427295793.setCatalogInfo(
    catalogDatabase="stedi-data", catalogTableName="machine_learning_curated"
)
MappingLearningCurated_node1783427295793.setFormat("json")
MappingLearningCurated_node1783427295793.writeFrame(SQLQuery_node1783427197104)

job.commit()
