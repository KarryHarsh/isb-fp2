# This is an example feature definition file

from datetime import timedelta
import yaml
import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
    ValueType,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, Int32, String
# from feast.value_type import ValueType

# record = Entity(name = "record_id",
#                     value_type = ValueType.INT64,
#                 description = "ID of the record")
record = Entity(name="record_id", join_keys=['record_id'])

transformed_data_dir = "datadir\\transformed" 


import yaml
with open(f"C:\\Users\\Harsha\\Documents\\ISB_AMPBA\\Term5\\FP2\\GroupAssignment\\project\\params.yaml", "r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
COUNTRY = config["base"]["COUNTRY"]
# for COUNTRY in config["FEAST"]["COUNTRIES"]:
## Predictors Feature View
file_source = FileSource(path = f"C:\\Users\\Harsha\\Documents\\ISB_AMPBA\\Term5\\FP2\\GroupAssignment\\project\\{transformed_data_dir}\\{COUNTRY}\\{COUNTRY}_features.parquet",
                     event_timestamp_column = "event_timestamp",)

df1_fv = FeatureView(
    name = f"{COUNTRY}_predictors_df_feature_view",
    ttl = timedelta(seconds = 86400*2),
    entities = [record],
    schema = [
    Field(name = "oil_production", dtype = Float64),
    Field(name = "prev", dtype = Float64),
    Field(name = "sma2", dtype = Float64),
    Field(name = "sma5", dtype = Float64),
    Field(name = "sma10", dtype = Float64),
    Field(name = "sma20", dtype = Float64),
    Field(name = "ema12", dtype = Float64),
    Field(name = "ema26", dtype = Float64),
    Field(name = "sma2_diff", dtype = Float64),
    Field(name = "sma5_diff", dtype = Float64),
    Field(name = "sma10_diff", dtype = Float64),
    Field(name = "sma20_diff", dtype = Float64),
    Field(name = "ema12_diff", dtype = Float64),
    Field(name = "ema26_diff", dtype = Float64),       
    ],
    source = file_source,
    online = True,
    tags= {},
)

# ## Target FEature View

# target_source = FileSource(path = r"C:/Users/Harsha/Documents/neustar/ospoc-mlops/datadir/output/target_df.parquet",
#                          event_timestamp_column = "event_timestamp",)

# target_fv = FeatureView(
#     name = "target_df_feature_view",
#     ttl = timedelta(seconds = 86400*2),
#     entities = [record],
#     schema = [
#     Field(name = "label", dtype = Int32),       
#     ],
#     source = target_source,
#     online = True,
#     tags= {},
# )