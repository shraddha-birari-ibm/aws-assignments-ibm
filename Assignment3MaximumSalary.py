import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1690888362987 = glueContext.create_dynamic_frame.from_catalog(
    database="db_employee",
    table_name="tb_sb_employee",
    transformation_ctx="AmazonS3_node1690888362987",
)

# Script generated for node Aggregate
Aggregate_node1690888580729 = sparkAggregate(
    glueContext,
    parentFrame=AmazonS3_node1690888362987,
    groups=["department_id"],
    aggs=[["salary", "max"]],
    transformation_ctx="Aggregate_node1690888580729",
)

# Script generated for node Amazon S3
AmazonS3_node1690888703122 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1690888580729,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://s3-shraddha/assignment3-output/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1690888703122",
)

job.commit()
