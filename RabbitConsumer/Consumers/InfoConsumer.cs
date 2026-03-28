using RabbitMQ.Client;

namespace RabbitConsumer.Consumers;

public class LogConsumer(IChannel channel, string queue, string tag) : BaseConsumer(channel, queue, tag)
{
    protected override Task<bool> ProcessMessageAsync(string message, string routingKey)
    {
        Console.ForegroundColor = tag.Contains("ALL") ? ConsoleColor.Gray : ConsoleColor.Green;
        Console.WriteLine($"[{tag}] Processed: {message}");
        Console.ResetColor();
        return Task.FromResult(true);
    }
}
