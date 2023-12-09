import os
import great_expectations as gx
from great_expectations.data_context import FileDataContext
from great_expectations.core.expectation_suite import ExpectationConfiguration
from dotenv import load_dotenv

load_dotenv()

# Fetch environment variables
DB_USER = os.getenv('DB_USER')
DB_PW = os.getenv('DB_PW')
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
DB = os.getenv('DB')

project_root = "./project/"

# Initialize the data context
context = FileDataContext.create(project_root_dir=project_root)

# Confirm the Data Context is Ephemeral
if isinstance(context, FileDataContext):
    print("It's a filesystem!")

# Convert the Ephemeral Data Context into a Filesystem Data Context
# context = context.convert_to_file_context()

suite = context.add_expectation_suite(expectation_suite_name="my_suite")

context.list_expectation_suite_names()

validator = context.sources.pandas_default.read_parquet(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet"
)

# Postgres table asset
asset_name = "postgres_table_asset"
asset_table_name = "data_pipelines.raw_yellow_taxi"

# table_asset = pg_datasource.add_table_asset(name=asset_name, table_name=asset_table_name)

# File System Datasource
datasource_name = 'filesource'
base_dir = './'
datasource = context.sources.add_pandas_filesystem(name=datasource_name, base_directory=base_dir)

asset_name = "my_taxi_data_asset"
batching_regex = r"yellow_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.parquet"

file_asset = datasource.add_parquet_asset(name=asset_name, batching_regex=batching_regex)

# Postgres Datasource
# Variables in config_variables.yml can be referenced in the connection string
pg_datasource = context.sources.add_or_update_postgres(
    name="local_postgres_db", connection_string="${my_postgres_db_yaml_creds}"
)

asset_name = "my_query_asset"
asset_query = "SELECT * from data_pipelines.raw_yellow_taxi"

query_asset = pg_datasource.add_query_asset(name=asset_name, query=asset_query)

batch_request1 = query_asset.build_batch_request({"vendor_id": 1})
batch_request2 = query_asset.build_batch_request({"vendor_id": 2})

vendor1_validator = context.get_validator(batch_request=batch_request1, create_expectation_suite_with_name='vendor1_expectation_suite')
vendor2_validator = context.get_validator(batch_request=batch_request2, create_expectation_suite_with_name='vendor2_expectation_suite')

