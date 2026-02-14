using Confluent.Kafka;
using Confluent.Kafka.Admin;

var builder = WebApplication.CreateBuilder(args);

// Configure services
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll", policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
});

builder.Services.AddScoped<KafkaAdminService>();
builder.Services.AddSingleton<MessageStoreService>();
builder.Services.AddScoped<ZookeeperBrowserService>();
builder.Services.AddHostedService<BackgroundConsumerService>();

var app = builder.Build();

app.UseCors("AllowAll");
app.UseStaticFiles();
app.UseRouting();

// Health check
app.MapGet("/api/health", () => Results.Ok(new { status = "OK" }))
    .WithName("Health");

// List topics
app.MapGet("/api/topics", async (KafkaAdminService adminService) =>
{
    try
    {
        var topics = await adminService.ListTopicsAsync();
        return Results.Ok(new { success = true, topics });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("ListTopics");

// Describe topic
app.MapGet("/api/topics/{topicName}", async (string topicName, KafkaAdminService adminService) =>
{
    try
    {
        var topicInfo = await adminService.DescribeTopicAsync(topicName);
        return Results.Ok(new { success = true, topicInfo });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("DescribeTopic");

// Create topic
app.MapPost("/api/topics", async (CreateTopicRequest request, KafkaAdminService adminService) =>
{
    try
    {
        await adminService.CreateTopicAsync(request.TopicName, request.Partitions, request.ReplicationFactor);
        return Results.Ok(new { success = true, message = $"Topic '{request.TopicName}' created successfully" });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("CreateTopic");

// Add partitions
app.MapPost("/api/topics/{topicName}/partitions", async (string topicName, AddPartitionsRequest request, KafkaAdminService adminService) =>
{
    try
    {
        var result = await adminService.AddPartitionsAsync(topicName, request.PartitionsToAdd);
        return Results.Ok(new { success = true, message = result });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("AddPartitions");

// List brokers
app.MapGet("/api/brokers", async (KafkaAdminService adminService) =>
{
    try
    {
        var brokers = await adminService.ListBrokersAsync();
        return Results.Ok(new { success = true, brokers });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("ListBrokers");

// Describe brokers
app.MapGet("/api/brokers/metadata", async (KafkaAdminService adminService) =>
{
    try
    {
        var metadata = await adminService.DescribeBrokersAsync();
        return Results.Ok(new { success = true, metadata });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("DescribeBrokers");

// Zookeeper browser - browse nodes
app.MapGet("/api/zookeeper/browse", async (string path, ZookeeperBrowserService zkBrowser) =>
{
    try
    {
        var children = await zkBrowser.BrowseAsync(path);
        return Results.Ok(new { success = true, path = path, children });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("ZookeeperBrowse");

// Zookeeper browser - get node data
app.MapGet("/api/zookeeper/node", async (string path, ZookeeperBrowserService zkBrowser) =>
{
    try
    {
        var nodeData = await zkBrowser.GetZnodeAsync(path);
        if (nodeData == null)
            return Results.NotFound(new { success = false, error = $"Node '{path}' not found" });
        return Results.Ok(new { success = true, nodeData });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("ZookeeperGetNode");

// Zookeeper browser - get cluster state
app.MapGet("/api/zookeeper/cluster", async (ZookeeperBrowserService zkBrowser) =>
{
    try
    {
        var clusterState = await zkBrowser.GetClusterStateAsync();
        return Results.Ok(new { success = true, clusterState });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("ZookeeperClusterState");

// Root - serve index.html
app.MapGet("/", () => Results.File("wwwroot/index.html", "text/html"))
    .WithName("Index");

// Zookeeper browser page
app.MapGet("/zookeeper", () => Results.File("wwwroot/zookeeper.html", "text/html"))
    .WithName("ZookeeperPage");

// Visualization page
app.MapGet("/visualization", () => Results.File("wwwroot/visualization.html", "text/html"))
    .WithName("VisualizationPage");

// Visualization endpoint - get broker/partition/message data
app.MapGet("/api/visualization/brokers", (MessageStoreService messageStore, KafkaAdminService adminService) =>
{
    try
    {
        var messagesByBroker = messageStore.GetMessagesByBroker();
        
        var result = new
        {
            success = true,
            brokers = new object?[]
            {
                GetBrokerData(messagesByBroker, 1, adminService),
                GetBrokerData(messagesByBroker, 2, adminService),
                GetBrokerData(messagesByBroker, 3, adminService)
            }
        };

        return Results.Ok(result);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { success = false, error = ex.Message });
    }
})
.WithName("VisualizationAPI");

static async Task<object?> GetBrokerData(Dictionary<int, List<MessageStoreService.PartitionMessages>> messagesByBroker, int brokerId, KafkaAdminService adminService)
{
    // Get replica information for all partitions
    var topicReplicaInfo = new Dictionary<string, Dictionary<int, List<int>>>();
    try
    {
        var topics = await adminService.ListTopicsAsync();
        foreach (var topicName in topics)
        {
            var topicInfo = await adminService.DescribeTopicAsync(topicName);
            if (!topicReplicaInfo.ContainsKey(topicName))
                topicReplicaInfo[topicName] = new Dictionary<int, List<int>>();
            
            foreach (var partition in topicInfo.Partitions)
            {
                topicReplicaInfo[topicName][partition.PartitionId] = partition.Replicas;
            }
        }
    }
    catch { /* Ignore errors in fetching replica info */ }

    if (!messagesByBroker.ContainsKey(brokerId))
    {
        return new
        {
            brokerId = brokerId,
            partitions = new object[] { }
        };
    }

    var partitions = messagesByBroker[brokerId].Select(p => new
    {
        topic = p.TopicName,
        partitionId = p.PartitionId,
        latestOffset = p.LatestOffset,
        messageCount = p.Messages.Count,
        replicas = topicReplicaInfo.ContainsKey(p.TopicName) && topicReplicaInfo[p.TopicName].ContainsKey(p.PartitionId) 
            ? topicReplicaInfo[p.TopicName][p.PartitionId]
            : new List<int>(),
        messages = p.Messages.Select(m => new
        {
            key = m.Key,
            value = m.Value.Length > 100 ? m.Value[..100] + "..." : m.Value,
            offset = m.Offset,
            timestamp = m.Timestamp
        }).ToList()
    }).ToList();

    return new
    {
        brokerId = brokerId,
        partitionCount = partitions.Count,
        totalMessages = partitions.Sum(p => (long)((dynamic)p).messageCount),
        partitions = partitions
    };
}

app.Run();

// ========================= REQUEST/RESPONSE MODELS =========================

public class CreateTopicRequest
{
    public string TopicName { get; set; } = string.Empty;
    public int Partitions { get; set; } = 1;
    public int ReplicationFactor { get; set; } = 1;
}

public class AddPartitionsRequest
{
    public int PartitionsToAdd { get; set; }
}

// ========================= KAFKA ADMIN SERVICE =========================

public class KafkaAdminService
{
    private const string BootstrapServers = "localhost:9092,localhost:9093,localhost:9094";

    public async Task<List<string>> ListTopicsAsync()
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build())
        {
            var metadata = adminClient.GetMetadata(timeout: TimeSpan.FromSeconds(10));
            return metadata.Topics.Select(t => t.Topic).OrderBy(t => t).ToList();
        }
    }

    public async Task<TopicDetailsDto> DescribeTopicAsync(string topicName)
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build())
        {
            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == topicName);

            if (topic == null)
                throw new Exception($"Topic '{topicName}' not found");

            var partitions = topic.Partitions.OrderBy(p => p.PartitionId).Select(p => new PartitionDetailsDto
            {
                PartitionId = p.PartitionId,
                Leader = p.Leader,
                Replicas = p.Replicas.ToList(),
                InSyncReplicas = p.InSyncReplicas.ToList(),
                Error = p.Error.Reason ?? "None"
            }).ToList();

            return new TopicDetailsDto
            {
                TopicName = topic.Topic,
                PartitionCount = topic.Partitions.Count,
                Partitions = partitions
            };
        }
    }

    public async Task CreateTopicAsync(string topicName, int partitions, int replicationFactor)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new Exception("Topic name cannot be empty");
        if (partitions < 1)
            throw new Exception("Partitions must be at least 1");
        if (replicationFactor < 1)
            throw new Exception("Replication factor must be at least 1");

        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build())
        {
            var topicSpec = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = partitions,
                ReplicationFactor = (short)replicationFactor
            };

            await adminClient.CreateTopicsAsync(new List<TopicSpecification> { topicSpec });
        }
    }

    public async Task<string> AddPartitionsAsync(string topicName, int newPartitions)
    {
        // This functionality is limited with the web API
        // Adding partitions requires using the Kafka CLI or console admin tools
        // For now, we show an error message
        throw new Exception("Adding partitions is not available in the web interface. " +
            "Please use the KafkaAdmin console tool or Kafka CLI to add partitions. " +
            "Run: kafka-topics --bootstrap-server localhost:9092 --alter --topic " + topicName + " --partitions " + (1 + newPartitions));
    }

    public async Task<List<BrokerDetailsDto>> ListBrokersAsync()
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build())
        {
            var metadata = adminClient.GetMetadata(timeout: TimeSpan.FromSeconds(10));
            return metadata.Brokers.OrderBy(b => b.BrokerId).Select(b => new BrokerDetailsDto
            {
                BrokerId = b.BrokerId,
                Host = b.Host,
                Port = b.Port
            }).ToList();
        }
    }

    public async Task<BrokerMetadataDto> DescribeBrokersAsync()
    {
        using (var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build())
        {
            var metadata = adminClient.GetMetadata(timeout: TimeSpan.FromSeconds(10));

            var brokerMetadata = new List<BrokerLeadershipDto>();
            foreach (var broker in metadata.Brokers)
            {
                int leaderCount = metadata.Topics
                    .SelectMany(t => t.Partitions)
                    .Count(p => p.Leader == broker.BrokerId);

                brokerMetadata.Add(new BrokerLeadershipDto
                {
                    BrokerId = broker.BrokerId,
                    Host = broker.Host,
                    Port = broker.Port,
                    LeaderPartitionCount = leaderCount
                });
            }

            return new BrokerMetadataDto
            {
                TotalBrokers = metadata.Brokers.Count,
                TotalTopics = metadata.Topics.Count,
                TotalLeaderPartitions = brokerMetadata.Sum(b => b.LeaderPartitionCount),
                Brokers = brokerMetadata.OrderBy(b => b.BrokerId).ToList()
            };
        }
    }
}

// ========================= DTOs =========================

public class TopicDetailsDto
{
    public string TopicName { get; set; } = string.Empty;
    public int PartitionCount { get; set; }
    public List<PartitionDetailsDto> Partitions { get; set; } = new();
}

public class PartitionDetailsDto
{
    public int PartitionId { get; set; }
    public int Leader { get; set; }
    public List<int> Replicas { get; set; } = new();
    public List<int> InSyncReplicas { get; set; } = new();
    public string Error { get; set; } = string.Empty;
}

public class BrokerDetailsDto
{
    public int BrokerId { get; set; }
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; }
}

public class BrokerLeadershipDto
{
    public int BrokerId { get; set; }
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; }
    public int LeaderPartitionCount { get; set; }
}

public class BrokerMetadataDto
{
    public int TotalBrokers { get; set; }
    public int TotalTopics { get; set; }
    public int TotalLeaderPartitions { get; set; }
    public List<BrokerLeadershipDto> Brokers { get; set; } = new();
}
