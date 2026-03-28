using RabbitConsumer.Consumers;
using RabbitMQ.Client;

namespace RabbitConsumer.Consumers;

public class ErrorConsumer(IChannel channel, string queue, string tag) : BaseConsumer(channel, queue, tag)
{
    protected override Task<bool> ProcessMessageAsync(string message, string routingKey)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"[!] ErrorWorker: Rejecting poisonous message -> {message}");
        Console.ResetColor();
        return Task.FromResult(false); // Triggers Nack
    }
}

