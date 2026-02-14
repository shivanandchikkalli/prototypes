# Kafka Learning Project

A lightweight .NET-based Kafka producer and consumer for learning Kafka concepts.

## Project Structure

```
kafka-1/
├── docker-compose.yml          # Docker setup for Kafka and Zookeeper
├── KafkaProducer/
│   ├── KafkaProducer.csproj
│   └── Program.cs              # Interactive producer with key|message format
├── KafkaConsumer/
│   ├── KafkaConsumer.csproj
│   └── Program.cs              # Consumer with offset tracking
├── KafkaAdmin/
│   ├── KafkaAdmin.csproj
│   └── Program.cs              # Console Admin CLI for topic & broker management
├── KafkaAdminWeb/
│   ├── KafkaAdminWeb.csproj
│   ├── Program.cs              # ASP.NET Core web app with REST APIs
│   └── wwwroot/
│       └── index.html          # Interactive web dashboard
└── README.md
```

## Kafka Cluster Setup

The docker-compose file runs a **3-broker Kafka cluster** with Zookeeper:

| Component | Count | Ports | Details |
|-----------|-------|-------|---------|
| **Brokers** | 3 | 9092, 9093, 9094 | Broker ID: 1, 2, 3 |
| **Zookeeper** | 1 | 2181 | Handles broker coordination |
| **Replication Factor Max** | 3 | - | Can replicate across all 3 brokers |

All applications (Producer, Consumer, Admin) connect to: **`localhost:9092,localhost:9093,localhost:9094`**

## Prerequisites

- Docker and Docker Compose installed
- .NET 8 SDK or later
- PowerShell (for running commands)

## Getting Started

### 1. Start Kafka with Docker Compose

```powershell
docker-compose up -d
```

Wait for all 3 Kafka brokers and Zookeeper to be healthy (usually 15-20 seconds).

Verify with:
```powershell
docker-compose ps
```

### 2. Run the Admin Web Dashboard (Recommended)

Open a terminal and navigate to the KafkaAdminWeb directory:

```powershell
cd KafkaAdminWeb
dotnet run
```

Then open your browser and navigate to: **http://localhost:5000**

Use the dashboard to create your first topic:
- Topic Name: `test-topic`
- Partitions: `3`
- Replication Factor: `2` or `3`

This will distribute replicas across multiple brokers!

### 3. Run the Producer

Open another terminal and navigate to the KafkaProducer directory:

```powershell
cd KafkaProducer
dotnet run
```

Producer features:
- Sends messages to `test-topic`
- Input format: `key|message` (or just `message` for default key)
- Example: `user-123|Hello from Kafka!`

### 4. Run the Consumer

Open another terminal and navigate to the KafkaConsumer directory:

```powershell
cd KafkaConsumer
dotnet run
```

Consumer features:
- Subscribes to `test-topic`
- Shows partition, offset, key, and message value
- Uses consumer group: `dotnet-consumer-group`
- Receives messages from all partitions across all 3 brokers
- Try stopping a broker and see if messages still flow (fault tolerance!)

### 5. Optional: Run the Console Admin Tool

For a CLI alternative, use:

```powershell
cd KafkaAdmin
dotnet run
```

This provides the same admin operations as the web dashboard in a console interface.
## Typical Workflow

```
Terminal 1: Start Docker Compose (3 brokers)
$ docker-compose up -d

Terminal 2: Start Admin Web Dashboard
$ cd KafkaAdminWeb
$ dotnet run
# Open browser: http://localhost:5000
# Create test-topic with 3 partitions, RF=2

Terminal 3: Start Consumer
$ cd KafkaConsumer
$ dotnet run

Terminal 4: Start Producer
$ cd KafkaProducer
$ dotnet run

# Now you have:
# - Messages sent to test-topic across 3 partitions
# - Data replicated across 2 brokers (fault tolerance)
# - Consumer receiving from all partitions
# - Try stopping a broker: docker-compose pause kafka-broker-1
```

## Key Concepts to Explore

| Concept | How to Explore |
|---------|---|
| **Partitions** | Create topics with 3+ partitions, see messages distributed across them |
| **Replication** | Create topics with RF=2 or RF=3, see replicas on different brokers |
| **Consumer Groups** | Run multiple consumers with the same GroupId, see load balancing |
| **Leader Election** | Stop a broker, watch Kafka reassign leaders automatically |
| **Offset Management** | Check offsets in the Consumer, see how position advances |

## Testing & Experiments

### Experiment 1: Replication in Action
```
1. Create topic: my-replicated-topic (3 partitions, RF=3)
2. Produce 10 messages
3. Check in Admin: all partitions should have 3 replicas
4. Describe broker metadata: see leadership distributed
```

### Experiment 2: Fault Tolerance
```
1. Run producer → consumer pipeline
2. Stop one broker: docker-compose pause kafka-broker-2
3. Messages should still flow (replicas on other brokers)
4. Resume broker: docker-compose unpause kafka-broker-2
```

### Experiment 3: Partitioning & Load Balancing
```
1. Create topic with 3 partitions
2. Run 3 consumer instances (same group)
3. Each consumer gets 1 partition
4. Messages are balanced across consumers
```

## Next Steps (Planned)

- [x] Multi-broker Kafka cluster (3 brokers)
- [x] Replication support (replication factor 1-3)
- [x] Admin dashboard (web UI)
- [ ] Consumer groups deep dive (multiple consumers per group)
- [ ] Offset management and reset strategies
- [ ] Error handling and retries
- [ ] Schema management (Avro/Protobuf)
- [ ] Performance tuning and monitoring

## Cleanup

To stop and remove Kafka containers:

```powershell
docker-compose down
```

## References

- [Confluent Kafka .NET Client](https://github.com/confluentinc/confluent-kafka-dotnet)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Replication](https://kafka.apache.org/documentation/#brokers)
- [Kafka Partitioning](https://kafka.apache.org/documentation/#producerconfigs)

