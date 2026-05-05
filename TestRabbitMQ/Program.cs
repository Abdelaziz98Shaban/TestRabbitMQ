using RabbitMQ.Client;
using System.Text;

namespace TestRabbitMQ;

public class Program
{
    static async Task Main(string[] args)
    {
        await PublishMessages();
        Console.WriteLine("Hello, World!");
        Console.ReadLine();
    }


    static async Task PublishMessages()
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var conn = await factory.CreateConnectionAsync();
        using var channel = await conn.CreateChannelAsync();

        string exchangeName = "test.topic_logs";

        // Publisher only 'ensures' the Exchange exists
        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, autoDelete: true);

        for (int i = 0; i < 50; i++)
        {
            string routingKey = (i % 2 == 0) ? "log.info" : "log.error";
            string msg = $"[{routingKey}] Message {i}";
            var body = Encoding.UTF8.GetBytes(msg);

            // We publish to the EXCHANGE. We don't care if a queue exists yet.
            await channel.BasicPublishAsync(exchangeName, routingKey, false, new BasicProperties(), body);

            Console.WriteLine($"Sent: {msg}");
            await Task.Delay(100);
        }

       await PublishCriticalLog(channel, "fail");
    }

    public static async Task PublishCriticalLog(IChannel channel, string message)
    {
        await channel.ExchangeDeclareAsync("quorum.main.topic", ExchangeType.Topic, autoDelete: true);


        var body = Encoding.UTF8.GetBytes(message);

        // Set persistence to work with the Quorum Queue's disk-based nature
        var props = new BasicProperties
        {
            Persistent = true, // DeliveryMode = 2
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
        };

        await channel.BasicPublishAsync(
            exchange: "quorum.main.topic",
            routingKey: "log.critical.ui",
            mandatory: true, // Ensure it hits a queue
            basicProperties: props,
            body: body);
    }
}
