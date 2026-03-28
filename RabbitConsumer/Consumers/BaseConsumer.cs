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

    private async Task OnMessageReceived(object sender, BasicDeliverEventArgs ea)
    {
        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
        try
        {
            bool success = await ProcessMessageAsync(message, ea.RoutingKey);

            if (success)
                await Channel.BasicAckAsync(ea.DeliveryTag, false);
            else
                // Send to Dead Letter Exchange
                await Channel.BasicNackAsync(ea.DeliveryTag, false, false);
        }
        catch (Exception)
        {
            await Channel.BasicNackAsync(ea.DeliveryTag, false, false);
        }
    }

    protected abstract Task<bool> ProcessMessageAsync(string message, string routingKey);
}
