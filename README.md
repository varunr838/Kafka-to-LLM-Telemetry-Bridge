# Kafka-to-LLM Telemetry Bridge

This project is a real-time data engineering pipeline that acts as a bridge between messy, raw streaming telemetry data and an AI-ready data consumption layer. 

## Project Architecture
1. **Simulator (`src/generator/`)**: A Python script running in Docker that simulates a high-velocity stream of server telemetry logs, purposefully injecting corrupted records to simulate real-world data chaos.
2. **Message Broker (Kafka via Docker)**: Holds the incoming data stream temporarily and safely.
3. **Stream Processor (`src/processor/`)**: A PySpark Structured Streaming job that consumes the Kafka topic in real-time, splits good vs. bad records (DLQ), handles schema evolution, aggregates metrics using watermarking, and writes to local Delta Lake tables.
4. **AI Infrastructure (`src/mcp_server/`)**: A lightweight Model Context Protocol (MCP) server that safely exposes the aggregated Delta Lake data to Large Language Models (like Claude) without giving them raw database access.

---

## Prerequisites
- **Docker** and **Docker Compose** installed (to run Kafka and the Generator).
- **Python 3.9+** installed locally.
- **Java 17+** installed locally (Required for Apache Spark to run the stream processor).

---

## How to Run the Project (Step-by-Step)

### Step 1: Start the Infrastructure & Data Generator
This step starts Zookeeper, Kafka, and our custom Python data generator. The generator will immediately begin pumping fake telemetry data into the Kafka topic.

Open a terminal at the root of the project and run:
```bash
docker-compose up -d --build
```
*(You can verify the generator is working by running: `docker-compose logs -f generator`)*

### Step 2: Install Local Dependencies
Open a *new* terminal window at the root of the project and install the required Python libraries for PySpark and the MCP server:
```bash
pip install -r requirements.txt
```

### Step 3: Start the Stream Processor
We will run the PySpark job locally. Since Kafka is running inside Docker but our processor is running locally, we need to tell the processor to connect to `localhost:9092` instead of the internal Docker network.

Run this in your terminal (Windows PowerShell):
```powershell
$env:KAFKA_BROKER="localhost:9092"
python src/processor/stream_processor.py
```
*(For Mac/Linux, use: `export KAFKA_BROKER=localhost:9092 && python src/processor/stream_processor.py`)*

You will see output indicating that the stream is processing batches, creating the `data/gold_logs/` and `data/dlq_logs/` directories in your project folder. **Leave this terminal running.**

### Step 4: Start the MCP Server (For AI Integration)
The MCP server allows an AI to query your newly created Delta tables. You can run it manually, or configure an AI client like Claude Desktop to run it for you.

To run it manually:
Open a *third* terminal window and run:
```bash
python src/mcp_server/server.py
```

To configure Claude Desktop to use it automatically:
1. Open your Claude configuration file (usually `%APPDATA%\Claude\claude_desktop_config.json` on Windows).
2. Copy the contents of the `claude_desktop_config_example.json` file in this project into that file, ensuring the absolute path to `server.py` is correct for your system.
3. Restart Claude Desktop and ask: *"Which user experienced the most 500 errors today based on my server logs?"*

---

## Stopping the Project
To shut everything down, go to the terminal where you ran `docker-compose` and type:
```bash
docker-compose down
```
You can press `Ctrl+C` to stop the PySpark streaming job and the MCP server.
