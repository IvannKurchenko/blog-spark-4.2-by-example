from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.demo.declaratime_pipelines_sdc.setup.data import WAREHOUSES_TABLE

spark = SparkSession.builder.getOrCreate()

@dp.table
def warehouses_bronze_streaming_table() -> DataFrame:
    return spark.readStream.option("skipChangeCommits", "true").table(WAREHOUSES_TABLE)

# The target that Auto CDC keeps in sync. It must be a streaming table, in a
# catalog whose format supports row-level operations (see "Choosing a Target
# Format" below).
dp.create_streaming_table("bronze.dim_warehouses", format="delta")

# The Auto CDC flow that applies the change events to the target.
dp.create_auto_cdc_flow(
    target="bronze.dim_warehouses",
    source="warehouses_bronze_streaming_table",
    keys=["warehouse_id"],
    sequence_by="updated_at",
    #apply_as_deletes="op = 'DELETE'",
    #except_column_list=["op"],
    stored_as_scd_type=1,
)
