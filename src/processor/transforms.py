from pyspark.sql.functions import col

def get_gold_df(df):
    """Filters valid records for the Gold layer."""
    return df.filter(
        col("user_id").isNotNull()
        & (col("status_code") <= 599)
    )

def get_dlq_df(df):
    """Filters invalid records for the Dead Letter Queue."""
    return df.filter(
        col("user_id").isNull()
        | (col("status_code") > 599)
    )
