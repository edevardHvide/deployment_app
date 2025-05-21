# Source System Options
SOURCE_SYSTEM_OPTIONS = [
    "Replicate_Full", 
    "Replicate_CDC", 
    "INS_temporal",
    "TIA",
    "Profisee",
    "Replicate_CDC_AllTransactions",
    "Replicate_CDC_AllTransactions_fromArchive"
]

# Default Values
DEFAULT_VALUES = {
    "source_system_initial": "Replicate_Full",
    "source_system_daily": "Replicate_CDC",
    "src_schema_name": "TIA",
    "tgt_schema_name_st": "ST",
    "tgt_schema_name_hs": "HS",
    "primary_key": "TC_ROW_ID",
    "incremental_filter_st": "__fullLoad",
    "incremental_filter_hs": "__fullLoad",
    "incremental_filter_timezone": "UTC",
    "scd_type": "SCD2",
    "scd2_columns_option": "Specify Columns",
    "helper_schema": "DF",
    "src_delete_column": "DELETED_FLAG",
    "src_delete_value": "Y",
    "partitions": 1,
    "use_source_column_for_valid_dates": True,
    "source_column_for_valid_from_date": "header__timestamp",
    "source_column_for_sorting": "header__change_seq",
    "main_table_schema": "DIM",
    "main_table_name": ""
}

# SCD Type Options
SCD_TYPE_OPTIONS = ["SCD2", "SCD1", "Transaction only", "Trunc Load", "SCD2 from CT"]

# SCD2 Columns Options
SCD2_COLUMNS_OPTIONS = ["Specify Columns", "__allColumns"]

# Delete Type Options
DELETE_TYPE_OPTIONS = [None, "SOFT", "HARD"]

# Timezone Options
TIMEZONE_OPTIONS = ["UTC", "W. Europe Standard Time"]

# Incremental Filter Options
INCREMENTAL_FILTER_OPTIONS = ["__fullLoad", "header__timestamp", "Custom"] 