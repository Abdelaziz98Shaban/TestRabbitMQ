using RabbitMQ.Client;

namespace RabbitConsumer.Consumers;

public class StreamConsumer(IChannel channel, string queueName, string tag) : BaseConsumer(channel, queueName, tag)
{
    protected override Task<bool> ProcessMessageAsync(string message, string routingKey)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"[STREAM-READER] {ConsumerTag} read: {message}");
        Console.ResetColor();
        return Task.FromResult(true);
    }
}
