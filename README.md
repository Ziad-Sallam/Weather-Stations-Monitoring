# Weather Stations Monitoring — Central Station

A data-intensive application that consumes weather station readings from Kafka, routes messages through a DSL pipeline, archives data to Parquet files, and triggers real-time alerts.

---

## Prerequisites

- Java 21
- Maven
- Docker
- Kafka + Zookeeper (via Bitnami image)

---

## 1. Start Kafka

```bash
# Pull Bitnami Kafka image (includes Zookeeper)
docker pull bitnami/kafka

# Start Kafka + Zookeeper
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=localhost:2181 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  bitnami/kafka
```

---

## 2. Create Kafka Topics

All topics must be created before running the application. Run the following commands:

```bash
# Main input topic — raw messages from weather stations
kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic weather-station-topic --partitions 1 --replication-factor 1

# Valid messages — clean, unique messages ready for processing
kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic weather-valid-topic --partitions 1 --replication-factor 1

# Invalid messages — malformed JSON that cannot be parsed
kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic weather-invalid-message-topic --partitions 1 --replication-factor 1

# Dead letter — valid JSON but failed field validation
kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic weather-dead-letter-topic --partitions 1 --replication-factor 1

# Rain alert — triggered when humidity > 70%
kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic weather-rain-alert-topic --partitions 1 --replication-factor 1

# Low battery alert — triggered when battery_status == "low"
kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic weather-low-battery-topic --partitions 1 --replication-factor 1
```

### Topics Summary

| Topic | Purpose |
|-------|---------|
| `weather-station-topic` | Raw input from all weather stations |
| `weather-valid-topic` | Clean, unique messages for BitCask + Parquet |
| `weather-invalid-message-topic` | Malformed JSON (cannot be parsed) |
| `weather-dead-letter-topic` | Parseable but failed field validation |
| `weather-rain-alert-topic` | Humidity > 70% alert |
| `weather-low-battery-topic` | Battery status == "low" alert |

---

## 3. Build the Project

```bash
mvn clean package -DskipTests
```

---

## 4. Run the Central Station Consumer

The Consumer is the main entry point of the Central Station. It starts three components:

- **MessageRoutingPipeline** — Kafka Streams DSL that routes raw messages into valid / invalid / dead-letter / duplicate branches
- **AlertStreamsPipeline** — Kafka Streams DSL that filters valid messages and publishes rain and low-battery alerts
- **Consumer** — Kafka polling loop that reads from `weather-valid-topic` and writes to BitCask + Parquet

**Main class:** `com.example.consumer.Consumer`

```bash
mvn exec:java -Dexec.mainClass="com.example.consumer.Consumer"
```

Expected output:
```
Message routing pipeline started...
Alert streams pipeline started...
Central Station Consumer started...
[VALID] station=3 | s_no=42 | battery=low | humidity=80 | temp=25 | wind=10
[DUPLICATE] Dropped: {...}
```

---

## 5. Run the Producer Mock (Testing Only)

> ⚠️ The producer is a **testing tool only**. In the real system, weather stations produce messages directly to Kafka. Use this mock to test the consumer pipeline during development.

**Main class:** `com.example.producer.SimpleProducerMock`

```bash
mvn exec:java -Dexec.mainClass="com.example.producer.SimpleProducerMock"
```

You will see an interactive menu:

```
===== MENU =====
1 -> Send Hardcoded Valid Message
2 -> Send Random Valid Message
3 -> Send Invalid Message         (routes to weather-invalid-message-topic)
4 -> Send Dead Letter Message     (routes to weather-dead-letter-topic)
5 -> Send Rain Alert Message      (humidity=80, routes to weather-rain-alert-topic)
6 -> Send Low Battery Message     (battery=low, routes to weather-low-battery-topic)
7 -> Send 10k Messages            (triggers Parquet flush)
8 -> Exit
```

---

## 6. Message Flow

```
Weather Station (or Producer Mock)
        │
        └──► weather-station-topic
                      │
                      ▼
            MessageRoutingPipeline (Kafka Streams)
                      │
                      ├── malformed JSON   ──► weather-invalid-message-topic
                      ├── invalid fields   ──► weather-dead-letter-topic
                      ├── duplicate s_no   ──► dropped (logged only)
                      └── valid + unique   ──► weather-valid-topic
                                                      │
                              ┌───────────────────────┤
                              ▼                       ▼
                  AlertStreamsPipeline         Consumer (poll loop)
                  (Kafka Streams)                     │
                      │                       ├── BitCask.write()
                      ├── humidity > 70%      └── ParquetWriter.add()
                      │   ──► rain-alert-topic        │
                      │                       flushes every 10,000 records
                      └── battery == low              │
                          ──► low-battery-topic        ▼
                                               parquet-data/
                                                 station_id=1/
                                                   *.parquet
                                                 station_id=2/
                                                   *.parquet
```

---

## 7. Verify Topics (Optional)

Use the Kafka console consumer to inspect any topic:

```bash
# Check valid messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic weather-station-topic 

# Check valid messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic weather-valid-topic 

# Check rain alerts
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic weather-rain-alert-topic 

# Check low battery
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic weather-low-battery-topic 

# Check dead letter
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic weather-dead-letter-topic
```

---

## 8. Parquet Output

Parquet files are written to the `parquet-data/` directory, partitioned by station:

```
parquet-data/
    station_id=1/
        20240517_143022.parquet
    station_id=2/
        20240517_143022.parquet
    ...
    station_id=10/
        20240517_143022.parquet
```

Files are flushed in batches of **10,000 records**.

---

## 9. Enterprise Integration Patterns Used

| Pattern | Where |
|---------|-------|
| Polling Consumer | `Consumer.java` — Kafka poll loop |
| Invalid Message Channel | `MessageRoutingPipeline` — malformed JSON branch |
| Dead Letter Channel | `MessageRoutingPipeline` — invalid fields branch |
| Idempotent Receiver | `MessageRoutingPipeline` — duplicate `stationId:sNo` detection |
| Pipes and Filters | Full DSL pipeline chain |