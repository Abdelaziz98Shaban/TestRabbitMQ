using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbitConsumer.Consumers;

public abstract class BaseConsumer(IChannel channel, string queueName, string consumerTag)
{
    protected readonly IChannel Channel = channel;
    protected readonly string QueueName = queueName;
    protected readonly string ConsumerTag = consumerTag;

    public async Task StartAsync()
    {
        var consumer = new AsyncEventingBasicConsumer(Channel);
        consumer.ReceivedAsync += OnMessageReceived;
        await Channel.BasicConsumeAsync(QueueName, false, ConsumerTag, consumer);
    }

    private async Task OnMessageReceived(object sender, BasicDeliverEventArgs @event)
    {
        var message = Encoding.UTF8.GetString(@event.Body.ToArray());
        try
        {
            // Quorum Feature: Check how many times this has been delivered
            long deliveryCount = 0;
            if (@event.BasicProperties.Headers?.ContainsKey("x-delivery-count") == true)
            {
                deliveryCount = (long)@event.BasicProperties.Headers["x-delivery-count"];
            }

            bool success = await ProcessMessageAsync(message, @event.RoutingKey);

            if (success)
                await Channel.BasicAckAsync(@event.DeliveryTag, false);
            else
                // await Channel.BasicNackAsync(@event.DeliveryTag, false, false);                // Send to Dead Letter Exchange
                await Channel.BasicNackAsync(@event.DeliveryTag, false, true); // requeeue for retry (Quorum will track delivery count and drop after limit)
        }
        catch (Exception)
        {
            await Channel.BasicNackAsync(@event.DeliveryTag, false, false);
        }
    }

    protected abstract Task<bool> ProcessMessageAsync(string message, string routingKey);
}
