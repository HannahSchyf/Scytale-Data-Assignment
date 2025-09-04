from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import explode
from pyspark.sql.types import *
from pyspark import SparkContext
import argparse as args
#import relevant libraries

# ------------
# Spark Setup
# ------------
spark = SparkSession.builder.appName("DataTransform").getOrCreate()
sc = spark.sparkContext
# We set up a spark spession we have called DataTransform and get the spark context (this is needed for RDD).
parser = args.ArgumentParser(description="Get organization and filters")
parser.add_argument("--org", type=str, required=True, help="GitHub organization name")
args = parser.parse_args()

ORG_NAME = args.org  # Get org name from command line
# We want to create a schema spark table which tells spark what fields to expect and their types from the JSON files. 
schema = StructType([
    StructField("pull_requests", ArrayType( # Our JSON is nested since we have multiple PRs in some, this tells the schema it is an array of structured objects.
        StructType([
            StructField("repo_id", LongType(), True),
            StructField("repo_name", StringType(), True),
            StructField("repo_owner", StringType(), True),
            StructField("org_name", StringType(), True),
            StructField("pr_id", LongType(), True),
            StructField("pr_number", LongType(), True),
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("merged_at", TimestampType(), True),
            StructField("approvers", IntegerType(), True),
            StructField("has_checks", BooleanType(),True),
            StructField("CHECKS_PASSED", StringType(),True),

        ])
    ), True)
])

# -----------------------
# Read JSON using schema
# -----------------------
prs_df = spark.read.schema(schema).option("multiline", True).json(f"../repo_data/{ORG_NAME}/*.json")
# use the schema above to interpret the JSON files, and produce a dataframe wehere each row contains the info from a single JSON object array. 

df_flat = prs_df.select(f.explode("pull_requests").alias("pr")).select("pr.*")

df_final = df_flat.withColumn(
    "passed_CR",
    f.when((f.col("approvers") > 0) & (f.col("has_checks") == True), f.lit("true"))
     .otherwise(f.lit("false"))
)

df_final = df_final.withColumn(
    "is_compliant",
    f.when((f.col("approvers") > 0) & (f.col("CHECKS_PASSED") == True), f.lit("true"))
     .when(f.col("CHECKS_PASSED").isNull(), f.lit("NULL"))
     .otherwise(f.lit("false"))
)
# we want each element of the array to have its own row, and flatten the nested structure so that all PR fiels are top-level columns in our new dataframe. 

# ---------------
# Create Filters
# ---------------

filtered_df = df_final

#parser = args.ArgumentParser(description="Process PR data with filters")
parser.add_argument("--start_date", type=str, help="Start date (YYYY-MM-DD)")
parser.add_argument("--end_date", type=str, help="End date (YYYY-MM-DD)")
parser.add_argument("--passed_CR", action="store_true", help="Only include PRs which have passed CR")
parser.add_argument("--PR_merged", action="store_true", help="Only include merged PRs")
parser.add_argument("--author", type=str, help="Filter PRs by author")
parser.add_argument("--repo", type=str, help="Filter PRs by repo name")
parser.add_argument("--passed_checks", action="store_true", help="Only include PRs where all checks passed")
parser.add_argument("--is_compliant", action="store_true", help="Only include PRs which have been approved and pass all checks")
args = parser.parse_args()

# Adding some filters

# Date range filter of merged
if args.start_date:
    filtered_df = filtered_df.filter(f.col("merged_at") >= args.start_date)
if args.end_date:
    filtered_df = filtered_df.filter(f.col("merged_at") <= args.end_date)

# If passed CR
if args.passed_CR:
    filtered_df = filtered_df.filter(f.col("CR_passed") == "True")

# If PR merged
if args.PR_merged:
    filtered_df = filtered_df.filter(f.col("merged_at").isNotNull())

# Author
if args.author:
    filtered_df = filtered_df.filter(f.lower(f.col("author")) == args.author.lower())

# Repo
if args.repo:
    filtered_df = filtered_df.filter(f.lower(f.col("repo_name")).contains(args.repo.lower()))

# If all checks were passed
if args.passed_checks:
    filtered_df = filtered_df.filter(f.col("CHECKS_PASSED") == "True")

# Check is PR is compliant
if args.is_compliant:
    filtered_df = filtered_df.filter(f.col("is_compliant") == "True")

# We want to the output to be sorted by date starting at the lastest merge
sorted_df = filtered_df.orderBy(f.col("merged_at").desc())
sorted_df.show(truncate=False)

# Save to Parquet
sorted_df.write.parquet('../Merged_PRs.pqt', mode="overwrite")

# if you want to double check the information is being saved to a parquet correctly uncomment below. 

# df_check = spark.read.parquet('/home/hannah/Scytale/extract.pqt')
# df_check.show(truncate=False)
print ()
print(f"Data successfully saved to Parquet") 
