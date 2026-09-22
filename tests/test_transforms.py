from pyspark.sql import Row
from src.processor.transforms import get_gold_df, get_dlq_df

def test_gold_dlq_split(spark):
    # 1. Mock Data: 3 valid rows, 1 invalid row
    mock_data = [
        Row(log_id="1", user_id="user_1", status_code=200), # Valid
        Row(log_id="2", user_id="user_2", status_code=500), # Valid
        Row(log_id="3", user_id="user_3", status_code=404), # Valid
        Row(log_id="4", user_id=None,     status_code=999)  # Invalid (null user + 999 status)
    ]
    
    # Create the static DataFrame
    df = spark.createDataFrame(mock_data)
    
    # 2. Run the filtering logic
    gold_df = get_gold_df(df)
    dlq_df = get_dlq_df(df)
    
    # 3. Assertions
    # Exactly 3 records should go to gold
    assert gold_df.count() == 3
    # Exactly 1 record should go to DLQ
    assert dlq_df.count() == 1
    
    # Optional: Verify the exact invalid record went to DLQ
    dlq_records = dlq_df.collect()
    assert dlq_records[0].log_id == "4"
