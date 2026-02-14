using Confluent.Kafka;
using Confluent.Kafka.Admin;

public class BackgroundConsumerService : IHostedService
{
    private readonly MessageStoreService _messageStore;
    private readonly KafkaAdminService _adminService;
    private IConsumer<string, string>? _consumer;
    private Task? _consumerTask;
    private CancellationTokenSource? _cts;
    private const string BootstrapServers = "localhost:9092,localhost:9093,localhost:9094";
    private Dictionary<string, Metadata>? _cachedMetadata;

    public BackgroundConsumerService(MessageStoreService messageStore, KafkaAdminService adminService)
    {
        _messageStore = messageStore;
        _adminService = adminService;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _consumerTask = ConsumeAllMessagesAsync(_cts.Token);
        await Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cts != null)
        {
            _cts.Cancel();
            if (_consumerTask != null)
            {
                await _consumerTask;
            }
        }

        _consumer?.Dispose();
    }

    private async Task ConsumeAllMessagesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(5000, cancellationToken); // Wait for Kafka to start

            // Get all topics
            var topics = await _adminService.ListTopicsAsync();
            if (topics.Count == 0)
            {
                // No topics yet, will retry
                _ = Task.Run(() => ConsumeAllMessagesAsync(cancellationToken), cancellationToken);
                return;
            }

            var config = new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = "admin-visualizer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                SessionTimeoutMs = 6000,
                IsolationLevel = IsolationLevel.ReadCommitted
            };

            _consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((consumer, error) =>
                {
                    if (!error.IsFatal)
                        Console.WriteLine($"Background consumer error: {error.Reason}");
                })
                .Build();

            _consumer.Subscribe(topics);

            int metadataRefreshCounter = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(TimeSpan.FromSeconds(1));

                    if (cr == null)
                        continue;

                    // Refresh metadata periodically
                    int brokerId = 1; // Default
                    if (metadataRefreshCounter % 50 == 0) // Refresh every 50 messages
                    {
                        try
                        {
                            using (var adminClient = new AdminClientBuilder(
                                new AdminClientConfig { BootstrapServers = BootstrapServers }).Build())
                            {
                                var metadata = adminClient.GetMetadata(cr.Topic, TimeSpan.FromSeconds(5));
                                _cachedMetadata = new Dictionary<string, Metadata> { { cr.Topic, metadata } };
                            }
                        }
                        catch { /* Ignore metadata fetch errors */ }
                    }

                    // Get broker ID for this partition from cached metadata
                    if (_cachedMetadata?.ContainsKey(cr.Topic) == true)
                    {
                        var metadata = _cachedMetadata[cr.Topic];
                        var topic = metadata.Topics.FirstOrDefault(t => t.Topic == cr.Topic);

                        if (topic != null)
                        {
                            var partition = topic.Partitions.FirstOrDefault(p => p.PartitionId == cr.Partition.Value);
                            if (partition != null)
                            {
                                brokerId = partition.Leader;
                            }
                        }
                    }

                    // Store the message
                    _messageStore.AddMessage(
                        cr.Topic,
                        cr.Partition.Value,
                        brokerId,
                        cr.Message.Key ?? "(no-key)",
                        cr.Message.Value ?? "(empty)",
                        cr.Offset.Value,
                        cr.Message.Timestamp.UtcDateTime
                    );

                    metadataRefreshCounter++;
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"Consume error: {ex.Error.Reason}");
                }
                catch (Exception)
                {
                    // Ignore transient errors
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        await Task.Delay(1000, cancellationToken);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when service is stopping
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Background consumer error: {ex.Message}");
        }
        finally
        {
            _consumer?.Close();
        }
    }
}
