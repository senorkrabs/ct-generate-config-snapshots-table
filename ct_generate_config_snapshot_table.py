""" /*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */ """

import argparse
import logging
import sys
import datetime
import awswrangler as wr

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


# OPTIONAL ARGUMENT PARSER (getResolvedOptions doesn't support optional arguments at the moment)
arg_parser = argparse.ArgumentParser(
    description="Creates or updates a Glue table that contains Config snapshots stored in a Control Tower LogArchive account.", 
    epilog="NOTES: The script expects the source config data to be stored in the standard folder/prefix naming convention that Control Tower uses: <org id>/AWSLogs/<account_number>"
)
arg_parser.add_argument('--s3_source_bucket', required=True, help="The Control Tower Logs source bucket where Config data is stored")
arg_parser.add_argument('--database_name', type=str, required=True, default=None, help="The name of the Glue database to create the table in.")
arg_parser.add_argument('--table_name', type=str, required=True, default=None, help="The name of the Glue table to create or overwrite.")

# Not used, but included because Glue passes these arguments in
arg_parser.add_argument('--extra-py-files', type=str, required=False, default=None, help="NOT USED")
arg_parser.add_argument('--scriptLocation', type=str, required=False, default=None, help="NOT USED")
arg_parser.add_argument('--job-bookmark-option', type=str, required=False, default=None, help="NOT USED")
arg_parser.add_argument('--job-language', type=str, required=False, default=None, help="NOT USED")
arg_parser.add_argument('--connection-names', type=str, required=False, default=None, help="NOT USED")

log.info(vars(arg_parser.parse_args() ))
args = vars(arg_parser.parse_args())


S3_SOURCE_BUCKET = args["s3_source_bucket"]
DATABASE_NAME = args["database_name"]
TABLE_NAME = args["table_name"]

# Retrieve list of account numbers and regions containing config data based on S3 prefixes in Control Tower logs bucket
org_ids = set()
accounts = set()
regions = set()
begin_date = datetime.date.today()


log.info("Enumerating orgs, accounts, and regions from bucket prefixes")
for org in wr.s3.list_directories(path="s3://"+S3_SOURCE_BUCKET+"/"):
    org_ids.add(org.rsplit("/",2)[1])
    for account in wr.s3.list_directories(path=org+'AWSLogs/'):
        accounts.add(account.rsplit("/",2)[1])
        for region in wr.s3.list_directories(path=account+'Config/'):
            regions.add(region.rsplit("/",2)[1])
            firstyear = min([int(year.rsplit("/", 2)[1]) for year in wr.s3.list_directories(path=region)])
            firstmonth = min([int(month.rsplit("/", 2)[1]) for month in wr.s3.list_directories(path=region+str(firstyear)+'/')])
            firstday = min([int(day.rsplit("/", 2)[1]) for day in wr.s3.list_directories(path=region+str(firstyear)+'/'+str(firstmonth)+'/')])
            first_date = datetime.date(firstyear, firstmonth, firstday)
            if first_date < begin_date:
                begin_date = first_date
print(begin_date)        

wr.catalog.delete_table_if_exists(
  database=DATABASE_NAME,
  table=TABLE_NAME
)
print(",".join(org_ids))
print(",".join(accounts))
print(",".join(regions))

log.info("Creating table {}.{}".format(DATABASE_NAME, TABLE_NAME))
log.setLevel(logging.DEBUG)
wr.athena.read_sql_query(
  database=DATABASE_NAME,
  ctas_approach=False,
  sql="""
  CREATE EXTERNAL TABLE `:database_name;`.`:table_name;`
  (
    fileversion STRING,
    configSnapshotId STRING,
    configurationitems ARRAY < STRUCT <
        configurationItemVersion : STRING,
        configurationItemCaptureTime : STRING,
        configurationStateId : BIGINT,
        awsAccountId : STRING,
        configurationItemStatus : STRING,
        resourceType : STRING,
        resourceId : STRING,
        resourceName : STRING,
        relationships: ARRAY < STRING >,
        ARN : STRING,
        awsRegion : STRING,
        availabilityZone : STRING,
        configurationStateMd5Hash : STRING,
        configuration : STRING,
        supplementaryConfiguration : MAP < STRING, STRING >,
        tags: MAP < STRING, STRING >,
        resourceCreationTime : STRING
    > >      
  ) 
  PARTITIONED BY (
    `org` string,
    `account` string,
    `region` string,
    `date` string    
  ) 
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
  LOCATION 's3://:bucket_name;/'
  TBLPROPERTIES
  (
    "projection.enabled" = "true",
    "projection.account.type" = "enum",
    "projection.account.values" = ":accounts;",
    "projection.region.type" = "enum",
    "projection.region.values" = ":regions;",
    "projection.org.type" = "enum",
    "projection.org.values" = ":orgs;",         
    "projection.date.format" = "yyyy/M/d",
    "projection.date.interval" = "1",
    "projection.date.interval.unit" = "DAYS",
    "projection.date.range" = "begindate;,NOW",
    "projection.date.type" = "date", 
    "storage.location.template" = "s3://:bucket_name;/${org}/AWSLogs/${account}/Config/${region}/${date}/ConfigSnapshot"
  )
    """,
  params={
    "database_name": DATABASE_NAME,
    "table_name": TABLE_NAME,
    "bucket_name": S3_SOURCE_BUCKET,
    "orgs": ",".join(org_ids),
    "accounts": ",".join(accounts),
    "regions": ",".join(regions),
    "begindate": f'{begin_date.year}/{begin_date.month}/{begin_date.day}'
  }
)





log.info ("Finished")
