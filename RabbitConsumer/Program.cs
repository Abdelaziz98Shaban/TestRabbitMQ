using RabbitConsumer.Consumers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;


namespace RabbitConsumer;

public class Program
{
    static async Task Main(string[] args)
    {
        await ConsumeMessages();
        Console.WriteLine("Waiting for messages. Press [Enter] to exit.");
        Console.ReadLine();

    }

    static async Task ConsumeMessages()
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        var conn = await factory.CreateConnectionAsync();
        var channel = await conn.CreateChannelAsync();

        string exchange = "test.topic_logs";
        string dlxExchange = "dlx.exchange";
        string dlxQueue = "dead.letters";
        string deadKey = "dead.key";

        // 1. Setup Infrastructure

        #region DLX Setup (Exchange + queue)
        await channel.ExchangeDeclareAsync(dlxExchange, ExchangeType.Direct, autoDelete: true);
        await channel.QueueDeclareAsync(dlxQueue, false, true, false); // Exclusive for test
        await channel.QueueBindAsync(dlxQueue, dlxExchange, deadKey);
        #endregion

        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Topic, false, true); //enusre main exchange exists

        // 2. Setup Queues with DLX Arguments
        var dlxArgs = new Dictionary<string, object>
        {
        { "x-dead-letter-exchange", dlxExchange },
        { "x-dead-letter-routing-key", deadKey }
        };

        async Task Setup(string q, string rk)
        {
            await channel.QueueDeclareAsync(q, false, true, false, dlxArgs);
            await channel.QueueBindAsync(q, exchange, rk);
        }

        await Setup("log.error", "log.error");
        await Setup("log.info", "log.info");
        await Setup("log.all", "log.#");

        // 3. Start Classes
        await new ErrorConsumer(channel, "log.error", "ERROR_WORKER").StartAsync();
        await new LogConsumer(channel, "log.info", "INFO_WORKER").StartAsync();
        await new LogConsumer(channel, "log.all", "ALL_WORKER").StartAsync();
        await new DeadLetterConsumer(channel, dlxQueue, "DLX_WORKER").StartAsync();

        Console.WriteLine("All consumers started. Waiting for logs...");
        await Task.Delay(-1);
    }
}

