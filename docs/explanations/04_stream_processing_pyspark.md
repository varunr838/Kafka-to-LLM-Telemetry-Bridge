# Part 4: Stream Processing with PySpark & Delta Lake

This is the core of the Data Engineering phase. We wrote a script (`stream_processor.py`) that constantly listens to Kafka, cleans the data, and saves it.

## The Tools We Are Using
1. **Apache Spark (PySpark):** Spark is a massive, lightning-fast data processing engine. It can process petabytes of data by splitting the work across many computers. We are using **Structured Streaming**, a feature of Spark that treats the continuous river of Kafka data like an infinitely growing table.
2. **Delta Lake:** When we save our processed data, we don't just dump it into a standard file. We save it as a "Delta Table". Delta Lake adds "ACID transactions" to our files. This means it protects our data from corruption if the system crashes midway through saving, and it allows us to do cool things like "Time Travel" (looking at older versions of the data) and "Schema Evolution" (adding new columns on the fly).

## Breaking down `stream_processor.py`

### 1. Reading and Parsing
We connect to Kafka and read the data. However, Kafka doesn't know what JSON is; it just sees raw binary bytes. We have to tell Spark: "Take this binary, convert it to a string, and then parse it using this specific Schema (blueprint) we defined."

### 2. Watermarking and Windowing (The Aggregation)
One of the goals of the project was to find out things like: "How many server errors did a user get in the last minute?"

Because data is streaming over a network, it might arrive late. 
* **Tumbling Window (1 minute):** We tell Spark to group the data into 1-minute buckets (e.g., 10:00 to 10:01). It calculates the average response time and counts the errors in that bucket.
* **Watermarking (5 minutes):** If a log from 10:00 gets delayed in the network and arrives at 10:04, Spark normally wouldn't know what to do with it. A watermark tells Spark: "Keep the 10:00 bucket open and wait for late data for up to 5 minutes. After 5 minutes, close the bucket permanently."

### 3. The `foreachBatch` function and The DLQ
We need to separate the good data from the corrupted data we purposefully injected. 

We created a function called `process_batch`. Spark processes streaming data in tiny chunks called "micro-batches". For every micro-batch that arrives, this function is triggered.

Inside the function:
1. **The Gold Stream:** We filter for records that have a `user_id` AND a valid `status_code`. We save these to a pristine folder called `gold_logs`.
2. **The DLQ (Dead Letter Queue):** We filter for the bad records (missing user IDs, status code 999). Instead of letting them crash our system or polluting our Gold data, we save them to a separate folder called `dlq_logs`. Data engineers can look at the DLQ later to figure out what went wrong.

### 4. Schema Evolution in Action
When we save the Gold data to Delta Lake, we added this crucial line:
```python
.option("mergeSchema", "true") 
```
When our generator hits 100 records and suddenly adds the `user_tier` column, Delta Lake sees it, realizes it's new, and automatically alters the underlying database structure on the hard drive to accept it, all without dropping a single record or crashing the stream.

## Next Steps
Now that the data is clean, aggregated, and safely stored in Delta Tables, the "Data Engineering" phase is complete. The final phase will be building the "AI Infrastructure" (the MCP server) so that a Large Language Model can read these tables and answer questions about them.
