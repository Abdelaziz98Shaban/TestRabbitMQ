namespace RabbitConsumer.Consumers;

using RabbitMQ.Client;

public class QuorumLogConsumer : BaseConsumer
{
    public QuorumLogConsumer(IChannel channel, string queueName, string tag)
        : base(channel, queueName, tag) { }

    protected override async Task<bool> ProcessMessageAsync(string message, string routingKey)
    {
        // This is where your business logic lives (e.g., saving to DB)
        try
        {
            Console.WriteLine($"[QUORUM] Processing: {message}");

            // Simulate a failure for demonstration if message contains "fail"
            if (message.Contains("fail"))
                throw new Exception("Simulated processing error.");

            await Task.Delay(50); // Simulate work
            return true; // Success -> Sends BasicAck
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] {ConsumerTag} failed to process message: {ex.Message}");
            return false; // Failure -> Sends BasicNack with Requeue
        }
    }

  
}