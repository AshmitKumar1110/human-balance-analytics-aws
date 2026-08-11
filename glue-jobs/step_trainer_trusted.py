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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783422642808 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-data",
    table_name="step_trainer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1783422642808",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783422643821 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-data",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1783422643821",
)

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    s.*
FROM
    s
INNER JOIN
    c
ON
    s.serialnumber = c.serialnumber;
'''
SQLQuery_node1783422703589 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "s": AWSGlueDataCatalog_node1783422642808,
        "c": AWSGlueDataCatalog_node1783422643821,
    },
    transformation_ctx="SQLQuery_node1783422703589",
)

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(
    frame=SQLQuery_node1783422703589,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1783422453008",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)

StepTrainerTrusted_node1783422772306 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1783422703589,
    connection_type="s3",
    format="json",
    connection_options={
        "path": f"s3://{S3_BUCKET}/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1783422772306",
)

job.commit()
