using System.Text.Json;
using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092,localhost:9093,localhost:9094",
    GroupId = "dotnet-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using (var consumer = new ConsumerBuilder<string, string>(config).Build())
{
    consumer.Subscribe("topic-1");

    Console.WriteLine("Kafka Consumer Started. Listening to 'topic-1'...");
    Console.WriteLine("Press Ctrl+C to stop.\n");

    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    try
    {
        int count = 0;
        while (!cts.Token.IsCancellationRequested)
        {
            var cr = consumer.Consume(cts.Token);

            if (cr == null)
                continue;

            count++;
            Console.WriteLine($"✓ Message {count}:");
            Console.WriteLine($"  Topic: {cr.Topic}");
            Console.WriteLine($"  Partition: {cr.Partition}");
            Console.WriteLine($"  Offset: {cr.Offset}");
            Console.WriteLine($"  Key: {cr.Message.Key}");
            Console.WriteLine($"  Value: {cr.Message.Value}");
            Console.WriteLine($"  Everything: {JsonSerializer.Serialize(cr)}");
            Console.WriteLine();
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("\nConsumer stopped gracefully.");
    }
    finally
    {
        consumer.Close();
    }
}
