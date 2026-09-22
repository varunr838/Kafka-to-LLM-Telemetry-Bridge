# Part 3: The Data Generator (Simulator)

Since we don't have a real website with millions of users to test our pipeline on, we have to simulate one. That is the purpose of the `generate_logs.py` script.

## The Goal
The script runs in an infinite loop (`while True:`), and every 0.5 to 2 seconds, it creates a fake server log and sends it to our Kafka broker.

## What does the data look like?
We are formatting our data as **JSON** (JavaScript Object Notation). It's a standard, human-readable way to organize data using keys and values. Here is what our base schema (blueprint) looks like:

```json
{
  "log_id": "a1b2c3d4",
  "user_id": "user_42",
  "timestamp": "2023-10-27T10:00:00Z",
  "endpoint": "/checkout",
  "status_code": 200,
  "response_time_ms": 150
}
```
* **log_id:** A unique identifier for the event.
* **user_id:** Who did it.
* **endpoint:** What page they visited.
* **status_code:** A web standard. 200 means "OK/Success". 400 means "Bad Request". 500 means "Server Error".
* **response_time_ms:** How long the server took to reply.

## Injecting Chaos (Data Quality)
In the real world, data pipelines break constantly because the incoming data is imperfect. A bug in a server might cause it to forget to send a `user_id`. If our pipeline isn't prepared for a missing `user_id`, it will crash.

To make our project robust and realistic, we intentionally inject "bad" data into the stream every 20 records:
```python
if record_count % 20 == 0:
    # We purposefully delete the user_id, or send an impossible status_code (999)
```
This forces us to build error-handling (the Dead Letter Queue) in our processor later.

## Schema Evolution
Another massive headache in data engineering is when the structure of the data changes suddenly. Imagine our pipeline has been running perfectly for months. Suddenly, the web development team updates the website and starts sending a new piece of information: `"user_tier": "premium"`.

Traditional databases would immediately crash and reject this new data because it doesn't match the rigid blueprint they were given. 

In our script, we simulate this scenario:
```python
if record_count > 100:
    payload["user_tier"] = random.choice(["premium", "free"])
```
After 100 logs, we suddenly start sending a new field. We will use a feature called "Schema Evolution" in our processor to handle this gracefully without crashing.

In the next part, we will look at how we catch and process this data.
