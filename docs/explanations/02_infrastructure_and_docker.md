# Part 2: Infrastructure - Docker & Kafka

To run this complex data pipeline, we need several pieces of software running simultaneously. To manage this easily, we use **Docker**.

## What is Docker?
Imagine you wrote a program on your computer, and it works perfectly. You send it to a friend, but it crashes on their computer because they have a different version of Windows or are missing a specific file. 

**Docker** solves this by putting your program, and everything it needs to run (the OS, the files, the configurations), into a standardized, isolated box called a **Container**. A container will run exactly the same way on *any* computer in the world.

## What is Docker Compose?
Sometimes your application needs multiple containers to work together. For our project, we need a container for Kafka, a container for Zookeeper, and a container for our Data Generator. 

**Docker Compose** is a tool that allows us to write a single recipe (our `docker-compose.yml` file) to start, connect, and stop all these containers with a single command (`docker-compose up`).

## Breaking down `docker-compose.yml`
Let's look at the services we defined in our compose file:

### 1. Zookeeper
```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
```
**What it is:** Zookeeper is a centralized service for maintaining configuration information. 
**Why we need it:** Kafka is a complex distributed system (meaning it usually runs on multiple computers at once). Kafka relies on Zookeeper to keep track of its internal state and keep everything synchronized. 

### 2. Kafka
```yaml
  kafka:
    image: confluentinc/cp-zookeeper:7.5.0
    depends_on:
      - zookeeper
```
**What it is:** Apache Kafka is our "Message Broker". 
**Why we need it:** It acts as a massive shock absorber. If our servers generate 10,000 logs a second, but our data processor can only handle 5,000 a second, the processor would crash. Kafka catches all 10,000 logs instantly, holds them safely in a queue (called a **Topic**), and lets the processor pull them at its own pace.
* **Producer:** Something that sends data *into* Kafka (our Generator).
* **Consumer:** Something that reads data *out of* Kafka (our Spark Processor).

### 3. Generator
```yaml
  generator:
    build:
      context: ./src/generator
```
**What it is:** This is the custom Python script we wrote. Instead of downloading a pre-made image from the internet (like we did for Kafka), we tell Docker to `build` a new image using the instructions in our `Dockerfile`.
**Why we need it:** It acts as the "Producer", creating the fake telemetry logs and sending them to Kafka.

In the next part, we will dive into the code of our Generator.
