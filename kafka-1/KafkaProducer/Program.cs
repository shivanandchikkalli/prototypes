using Confluent.Kafka;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092,localhost:9093,localhost:9094",
    Acks = Acks.All,
    AllowAutoCreateTopics = false
};

using (var producer = new ProducerBuilder<string, string>(config).Build())
{
    try
    {
        Console.WriteLine("Kafka Producer Started. Press Ctrl+C to stop.");
        Console.WriteLine("Enter messages in format: key|message (or just message for no key)\n");

        int count = 0;
        while (true)
        {
            Console.Write("Enter message: ");
            var input = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(input))
                continue;

            string key = "event-key";
            string message = input;

            // Check if input contains a key separator
            if (input.Contains("|"))
            {
                var parts = input.Split("|", 2);
                key = parts[0];
                message = parts[1];
            }

            var dr = await producer.ProduceAsync("topic-1",
                new Message<string, string> { Key = key, Value = message });

            count++;
            Console.WriteLine($"✓ Message {count} delivered to topic '{dr.Topic}' " +
                            $"[{dr.Partition}-{dr.Offset}]\n");
        }
    }
    catch (ProduceException<string, string> e)
    {
        Console.WriteLine($"✗ Delivery failed: {e.Error.Reason}");
    }
}
