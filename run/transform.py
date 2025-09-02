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
            StructField("merged_at", StringType(), True),
            StructField("approvers", StringType(), True),
            StructField("CR_passed", StringType(),True),
            StructField("has_checks", StringType(),True),
            StructField("CHECKS_PASSED", StringType(),True),
        ])
    ), True)
])

# -----------------------
# Read JSON using schema
# -----------------------
prs_df = spark.read.schema(schema).option("multiline", True).json("../repo_data/*.json")
# use the schema above to interpret the JSON files, and produce a dataframe wehere each row contains the info from a single JSON object array. 

df_flat = prs_df.select(f.explode("pull_requests").alias("pr")).select("pr.*")
# we want each element of the array to have its own row, and flatten the nested structure so that all PR fiels are top-level columns in our new dataframe. 

# ---------------
# Create Filters
# ---------------

filtered_df = df_flat

parser = args.ArgumentParser(description="Process PR data with filters")
parser.add_argument("--start_date", type=str, help="Start date (YYYY-MM-DD)")
parser.add_argument("--end_date", type=str, help="End date (YYYY-MM-DD)")
parser.add_argument("--passed_CR", action="store_true", help="Passed CR")
parser.add_argument("--PR_merged", action="store_true", help="Only include merged PRs")
parser.add_argument("--author", type=str, help="Filter PRs by author")
parser.add_argument("--repo", type=str, help="Filter PRs by repo name")
parser.add_argument("--passed_checks", action="store_true", help="Only include PRs where all checks passed")
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

# We want to the output to be sorted by date starting at the lastest merge
sorted_df = filtered_df.orderBy(f.col("merged_at").desc())
sorted_df.show(truncate=False)
# Save to Parquet
sorted_df.write.parquet('/home/hannah/Scytale/extract.pqt', mode="overwrite")

# if you want to double check the information is being saved to a parquet correctly
# df_check = spark.read.parquet('/home/hannah/Scytale/extract.pqt')
# df_check.show(truncate=False)

print(f"Data successfully saved to Parquet") 
