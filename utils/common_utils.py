"""
Common Utilities for P&C Insurance Medallion Architecture

Shared utility functions used across Bronze, Silver, and Gold pipelines.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import re

# ============================================================================
# DATE AND TIME UTILITIES
# ============================================================================

def get_current_timestamp():
    """Get current timestamp as datetime object."""
    return datetime.now()

def format_timestamp(ts: datetime, format: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format timestamp to string."""
    return ts.strftime(format)

def parse_date(date_str: str, format: str = "%Y-%m-%d") -> datetime:
    """Parse date string to datetime object."""
    return datetime.strptime(date_str, format)

def get_date_range(start_date: str, end_date: str, format: str = "%Y-%m-%d") -> List[str]:
    """Generate list of dates between start and end date."""
    start = parse_date(start_date, format)
    end = parse_date(end_date, format)
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime(format))
        current += timedelta(days=1)
    
    return dates

def add_audit_columns(df: DataFrame, load_id: str = None) -> DataFrame:
    """Add standard audit columns to DataFrame."""
    if load_id is None:
        import uuid
        load_id = str(uuid.uuid4())
    
    return df.withColumn("load_id", F.lit(load_id)) \
             .withColumn("load_timestamp", F.current_timestamp()) \
             .withColumn("source_system", F.lit("P&C_INSURANCE"))

def add_scd2_columns(df: DataFrame) -> DataFrame:
    """Add SCD2 tracking columns to DataFrame."""
    return df.withColumn("is_current", F.lit(True)) \
             .withColumn("effective_from", F.current_timestamp()) \
             .withColumn("effective_to", F.lit("9999-12-31 23:59:59").cast("timestamp"))

# ============================================================================
# STRING UTILITIES
# ============================================================================

def clean_string(col_name: str) -> F.Column:
    """Clean string column: trim, upper case, remove extra spaces."""
    return F.trim(F.upper(F.regexp_replace(F.col(col_name), "\\s+", " ")))

def normalize_phone(col_name: str) -> F.Column:
    """Normalize phone number to format: XXX-XXX-XXXX."""
    return F.regexp_replace(
        F.regexp_replace(F.col(col_name), "[^0-9]", ""),
        "(\\d{3})(\\d{3})(\\d{4})",
        "$1-$2-$3"
    )

def normalize_zip(col_name: str) -> F.Column:
    """Normalize ZIP code to 5-digit format."""
    return F.lpad(F.regexp_replace(F.col(col_name), "[^0-9]", ""), 5, "0")

def mask_ssn(col_name: str) -> F.Column:
    """Mask SSN to format: XXX-XX-1234."""
    return F.concat(
        F.lit("XXX-XX-"),
        F.substring(F.col(col_name), -4, 4)
    )

def generate_hash_key(*col_names: str) -> F.Column:
    """Generate MD5 hash key from multiple columns."""
    concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in col_names])
    return F.md5(concat_expr)

# ============================================================================
# DATAFRAME UTILITIES
# ============================================================================

def get_row_count(df: DataFrame, description: str = "") -> int:
    """Get row count with optional logging."""
    count = df.count()
    if description:
        print(f"{description}: {count:,} rows")
    return count

def get_distinct_count(df: DataFrame, col_name: str, description: str = "") -> int:
    """Get distinct count for a column."""
    count = df.select(col_name).distinct().count()
    if description:
        print(f"{description} - Distinct {col_name}: {count:,}")
    return count

def show_sample(df: DataFrame, n: int = 10, description: str = ""):
    """Show sample rows from DataFrame."""
    if description:
        print(f"\n{description}")
        print("="*80)
    df.show(n, truncate=False)

def get_null_counts(df: DataFrame) -> Dict[str, int]:
    """Get null counts for all columns."""
    null_counts = {}
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_counts[col_name] = null_count
    return null_counts

def print_null_summary(df: DataFrame):
    """Print null count summary for all columns."""
    total_rows = df.count()
    null_counts = get_null_counts(df)
    
    print("\nNull Count Summary:")
    print("="*80)
    print(f"{'Column':<30} {'Null Count':>15} {'Null %':>10}")
    print("-"*80)
    
    for col_name, null_count in sorted(null_counts.items()):
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        print(f"{col_name:<30} {null_count:>15,} {null_pct:>9.2f}%")

def compare_dataframes(df1: DataFrame, df2: DataFrame, key_cols: List[str]) -> Dict:
    """Compare two DataFrames and return differences."""
    
    count1 = df1.count()
    count2 = df2.count()
    
    # Records only in df1
    only_in_df1 = df1.join(df2, key_cols, "left_anti").count()
    
    # Records only in df2
    only_in_df2 = df2.join(df1, key_cols, "left_anti").count()
    
    # Common records
    common = df1.join(df2, key_cols, "inner").count()
    
    return {
        "df1_count": count1,
        "df2_count": count2,
        "only_in_df1": only_in_df1,
        "only_in_df2": only_in_df2,
        "common": common,
        "match_rate": (common / max(count1, count2)) if max(count1, count2) > 0 else 0
    }

# ============================================================================
# LOGGING UTILITIES
# ============================================================================

def log_info(message: str, prefix: str = "INFO"):
    """Log info message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{prefix}] {message}")

def log_error(message: str, exception: Exception = None):
    """Log error message with optional exception."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [ERROR] {message}")
    if exception:
        print(f"[{timestamp}] [ERROR] Exception: {str(exception)}")

def log_warning(message: str):
    """Log warning message."""
    log_info(message, prefix="WARNING")

def print_section_header(title: str, width: int = 80):
    """Print formatted section header."""
    print("\n" + "="*width)
    print(title.center(width))
    print("="*width)

def print_subsection_header(title: str, width: int = 80):
    """Print formatted subsection header."""
    print("\n" + "-"*width)
    print(title)
    print("-"*width)

# ============================================================================
# ERROR HANDLING UTILITIES
# ============================================================================

def safe_divide(numerator: F.Column, denominator: F.Column, default_value: float = 0.0) -> F.Column:
    """Safely divide two columns, handling division by zero."""
    return F.when(denominator != 0, numerator / denominator).otherwise(F.lit(default_value))

def handle_null(col: F.Column, default_value: Any) -> F.Column:
    """Replace null values with default value."""
    return F.coalesce(col, F.lit(default_value))

def validate_not_empty(df: DataFrame, table_name: str):
    """Validate that DataFrame is not empty, raise error if empty."""
    count = df.count()
    if count == 0:
        raise ValueError(f"DataFrame for {table_name} is empty!")
    log_info(f"Validated {table_name}: {count:,} rows")

# ============================================================================
# CONFIGURATION UTILITIES
# ============================================================================

def get_catalog_name() -> str:
    """Get catalog name from configuration."""
    return "pc_insurance"

def get_schema_name(layer: str) -> str:
    """Get schema name for specified layer."""
    layer_map = {
        "bronze": "bronze",
        "silver": "silver",
        "gold": "gold",
        "reference": "reference"
    }
    return layer_map.get(layer.lower(), layer.lower())

def get_table_name(layer: str, table: str) -> str:
    """Get fully qualified table name."""
    catalog = get_catalog_name()
    schema = get_schema_name(layer)
    return f"{catalog}.{schema}.{table}"

def get_checkpoint_location(layer: str, table: str) -> str:
    """Get checkpoint location for streaming."""
    return f"/mnt/checkpoints/{layer}/{table}"

# ============================================================================
# SPARK SESSION UTILITIES
# ============================================================================

def get_spark() -> SparkSession:
    """Get or create Spark session."""
    return SparkSession.builder.getOrCreate()

def optimize_table(table_name: str, zorder_cols: List[str] = None):
    """Optimize Delta table with optional Z-ordering."""
    spark = get_spark()
    
    log_info(f"Optimizing table: {table_name}")
    spark.sql(f"OPTIMIZE {table_name}")
    
    if zorder_cols:
        zorder_clause = ", ".join(zorder_cols)
        log_info(f"Z-ordering by: {zorder_clause}")
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_clause})")
    
    log_info(f"Optimization complete for {table_name}")

def analyze_table(table_name: str):
    """Analyze table to compute statistics."""
    spark = get_spark()
    
    log_info(f"Analyzing table: {table_name}")
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
    log_info(f"Analysis complete for {table_name}")
