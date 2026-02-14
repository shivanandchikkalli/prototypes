using Confluent.Kafka;

public class MessageStoreService
{
    private readonly object _lockObject = new object();
    private readonly Dictionary<string, PartitionMessages> _partitionMessages = new();
    private const int MaxMessagesPerPartition = 50; // Keep last 50 messages per partition

    public class PartitionMessages
    {
        public int BrokerId { get; set; }
        public string TopicName { get; set; } = string.Empty;
        public int PartitionId { get; set; }
        public Queue<MessageInfo> Messages { get; set; } = new();
        public long LatestOffset { get; set; }
        public DateTime LastUpdated { get; set; } = DateTime.UtcNow;
    }

    public class MessageInfo
    {
        public string Key { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
        public long Offset { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public void AddMessage(string topic, int partition, int brokerId, string key, string value, long offset, DateTime timestamp)
    {
        lock (_lockObject)
        {
            string key_str = $"{topic}_{partition}";

            if (!_partitionMessages.ContainsKey(key_str))
            {
                _partitionMessages[key_str] = new PartitionMessages
                {
                    TopicName = topic,
                    PartitionId = partition,
                    BrokerId = brokerId
                };
            }

            var partition_msgs = _partitionMessages[key_str];
            partition_msgs.Messages.Enqueue(new MessageInfo
            {
                Key = key,
                Value = value ?? "(empty)",
                Offset = offset,
                Timestamp = timestamp
            });

            // Keep only last N messages
            while (partition_msgs.Messages.Count > MaxMessagesPerPartition)
            {
                partition_msgs.Messages.Dequeue();
            }

            partition_msgs.LatestOffset = offset;
            partition_msgs.LastUpdated = DateTime.UtcNow;
        }
    }

    public PartitionMessages? GetPartitionMessages(string topic, int partition)
    {
        lock (_lockObject)
        {
            string key = $"{topic}_{partition}";
            if (_partitionMessages.TryGetValue(key, out var msgs))
            {
                return msgs;
            }
            return null;
        }
    }

    public Dictionary<int, List<PartitionMessages>> GetMessagesByBroker()
    {
        lock (_lockObject)
        {
            var result = new Dictionary<int, List<PartitionMessages>>();
            
            foreach (var msg in _partitionMessages.Values.OrderBy(m => m.BrokerId).ThenBy(m => m.TopicName).ThenBy(m => m.PartitionId))
            {
                if (!result.ContainsKey(msg.BrokerId))
                {
                    result[msg.BrokerId] = new List<PartitionMessages>();
                }
                result[msg.BrokerId].Add(msg);
            }

            return result;
        }
    }

    public void Clear()
    {
        lock (_lockObject)
        {
            _partitionMessages.Clear();
        }
    }
}
