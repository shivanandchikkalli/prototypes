using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";

while (true)
{
    Console.WriteLine("\n╔════════════════════════════════════════╗");
    Console.WriteLine("║     KAFKA ADMIN CLI                    ║");
    Console.WriteLine("╚════════════════════════════════════════╝");
    Console.WriteLine("1. List all topics");
    Console.WriteLine("2. Describe topic (partitions & configs)");
    Console.WriteLine("3. Create new topic");
    Console.WriteLine("4. Add partitions to topic");
    Console.WriteLine("5. List brokers");
    Console.WriteLine("6. Describe broker metadata");
    Console.WriteLine("0. Exit\n");
    Console.Write("Select option: ");

    var choice = Console.ReadLine();

    switch (choice)
    {
        case "1":
            await ListTopics();
            break;
        case "2":
            await DescribeTopic();
            break;
        case "3":
            await CreateTopic();
            break;
        case "4":
            await AddPartitions();
            break;
        case "5":
            await ListBrokers();
            break;
        case "6":
            await DescribeBrokers();
            break;
        case "0":
            Console.WriteLine("Goodbye!");
            return;
        default:
            Console.WriteLine("❌ Invalid option");
            break;
    }
}

// ========================= ADMIN OPERATIONS =========================

async Task ListTopics()
{
    Console.WriteLine("\n📋 Fetching topics...");
    try
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            var metadata = adminClient.GetMetadata(timeout: TimeSpan.FromSeconds(10));

            if (metadata.Topics.Count == 0)
            {
                Console.WriteLine("❌ No topics found");
                return;
            }

            Console.WriteLine($"\n✓ Found {metadata.Topics.Count} topic(s):\n");
            foreach (var topic in metadata.Topics.OrderBy(t => t.Topic))
            {
                Console.WriteLine($"  • {topic.Topic}");
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
    }
}

async Task DescribeTopic()
{
    Console.Write("\nEnter topic name: ");
    var topicName = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(topicName))
    {
        Console.WriteLine("❌ Topic name cannot be empty");
        return;
    }

    try
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

            if (topic == null)
            {
                Console.WriteLine($"❌ Topic '{topicName}' not found");
                return;
            }

            Console.WriteLine($"\n✓ Topic: {topic.Topic}");
            Console.WriteLine($"  Partitions: {topic.Partitions.Count}");
            Console.WriteLine($"  Error: {(string.IsNullOrEmpty(topic.Error.Reason) ? "None" : topic.Error.Reason)}");

            Console.WriteLine("\n  Partition Details:");
            foreach (var partition in topic.Partitions.OrderBy(p => p.PartitionId))
            {
                Console.WriteLine($"    Partition {partition.PartitionId}:");
                Console.WriteLine($"      Leader: {partition.Leader}");
                Console.WriteLine($"      Replicas: {string.Join(", ", partition.Replicas)}");
                Console.WriteLine($"      ISR (In-Sync Replicas): {string.Join(", ", partition.InSyncReplicas)}");
            }

            // Get topic configurations
            await DescribeTopicConfigs(adminClient, topicName);
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
    }
}

async Task DescribeTopicConfigs(IAdminClient adminClient, string topicName)
{
    try
    {
        var configResource = new ConfigResource { Type = ResourceType.Topic, Name = topicName };
        var configs = await adminClient.DescribeConfigsAsync(
            new List<ConfigResource> { configResource });

        Console.WriteLine($"\n  Configuration:");
        foreach (var configEntry in configs.FirstOrDefault()?.Configs ?? new List<ConfigEntry>())
        {
            if (!configEntry.Name.StartsWith("__") && configEntry.Value != null)
            {
                Console.WriteLine($"    {configEntry.Name}: {configEntry.Value}");
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  Could not fetch configs: {ex.Message}");
    }
}

async Task CreateTopic()
{
    Console.Write("\nEnter topic name: ");
    var topicName = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(topicName))
    {
        Console.WriteLine("❌ Topic name cannot be empty");
        return;
    }

    Console.Write("Number of partitions (default: 1): ");
    if (!int.TryParse(Console.ReadLine() ?? "1", out int partitions) || partitions < 1)
    {
        Console.WriteLine("❌ Invalid partition count");
        return;
    }

    Console.Write("Replication factor (default: 1): ");
    if (!int.TryParse(Console.ReadLine() ?? "1", out int replicas) || replicas < 1)
    {
        Console.WriteLine("❌ Invalid replication factor");
        return;
    }

    try
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            var topicSpec = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = partitions,
                ReplicationFactor = (short)replicas
            };

            await adminClient.CreateTopicsAsync(new List<TopicSpecification> { topicSpec });
            Console.WriteLine($"✓ Topic '{topicName}' created with {partitions} partition(s) and replication factor {replicas}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
    }
}

async Task AddPartitions()
{
    Console.Write("\nEnter topic name: ");
    var topicName = Console.ReadLine();

    if (string.IsNullOrWhiteSpace(topicName))
    {
        Console.WriteLine("❌ Topic name cannot be empty");
        return;
    }

    Console.Write("Number of partitions to add: ");
    if (!int.TryParse(Console.ReadLine(), out int newPartitions) || newPartitions < 1)
    {
        Console.WriteLine("❌ Invalid number");
        return;
    }

    try
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            // First, get current partition count
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

            if (topic == null)
            {
                Console.WriteLine($"❌ Topic '{topicName}' not found");
                return;
            }

            int currentPartitions = topic.Partitions.Count;
            int totalPartitions = currentPartitions + newPartitions;

            var topicSpec = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = totalPartitions
            };

            await adminClient.CreatePartitionsAsync(
                new List<TopicSpecification> { topicSpec });

            Console.WriteLine($"✓ Added {newPartitions} partition(s) to '{topicName}'");
            Console.WriteLine($"  Total partitions: {currentPartitions} → {totalPartitions}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
    }
}

async Task ListBrokers()
{
    Console.WriteLine("\n🖥️  Fetching brokers...");
    try
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            var metadata = adminClient.GetMetadata(timeout: TimeSpan.FromSeconds(10));

            if (metadata.Brokers.Count == 0)
            {
                Console.WriteLine("❌ No brokers found");
                return;
            }

            Console.WriteLine($"\n✓ Found {metadata.Brokers.Count} broker(s):\n");
            foreach (var broker in metadata.Brokers.OrderBy(b => b.Id))
            {
                Console.WriteLine($"  Broker ID: {broker.Id}");
                Console.WriteLine($"    Host: {broker.Host}");
                Console.WriteLine($"    Port: {broker.Port}");
                Console.WriteLine();
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
    }
}

async Task DescribeBrokers()
{
    Console.WriteLine("\n🖥️  Fetching broker details...");
    try
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            var metadata = adminClient.GetMetadata(timeout: TimeSpan.FromSeconds(10));

            if (metadata.Brokers.Count == 0)
            {
                Console.WriteLine("❌ No brokers found");
                return;
            }

            Console.WriteLine($"\n✓ Broker Summary:\n");
            Console.WriteLine($"  Total Brokers: {metadata.Brokers.Count}");
            Console.WriteLine($"  Total Topics: {metadata.Topics.Count}");

            // Calculate leadership distribution
            int totalLeaderPartitions = 0;
            foreach (var broker in metadata.Brokers)
            {
                int leaderCount = metadata.Topics
                    .SelectMany(t => t.Partitions)
                    .Count(p => p.Leader == broker.Id);

                totalLeaderPartitions += leaderCount;
                Console.WriteLine($"\n  Broker {broker.Id} ({broker.Host}:{broker.Port})");
                Console.WriteLine($"    Leader for {leaderCount} partition(s)");
            }

            Console.WriteLine($"\n  Total Leader Partitions: {totalLeaderPartitions}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
    }
}
