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

# Script generated for node Customer Landing
CustomerLanding_node1783400158879 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [f"s3://{S3_BUCKET}/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1783400158879",
)

# Script generated for node Privacy Filter
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
PrivacyFilter_node1783400677161 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": CustomerLanding_node1783400158879},
    transformation_ctx="PrivacyFilter_node1783400677161",
)

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(
    frame=PrivacyFilter_node1783400677161,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1783400146587",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)

CustomerTrusted_node1783400922296 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node1783400677161,
    connection_type="s3",
    format="json",
    connection_options={
        "path": f"s3://{S3_BUCKET}/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1783400922296",
)

job.commit()
