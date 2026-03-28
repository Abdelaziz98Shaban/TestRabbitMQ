using RabbitConsumer.Consumers;
using RabbitMQ.Client;

namespace RabbitConsumer.Consumers;

public class DeadLetterConsumer : BaseConsumer
{
    public DeadLetterConsumer(IChannel channel, string queue, string tag) : base(channel, queue, tag) { }
    protected override Task<bool> ProcessMessageAsync(string message, string routingKey)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"[DLX] Recovered failed message: {message}");
        Console.ResetColor();
        return Task.FromResult(true);
    }
}
