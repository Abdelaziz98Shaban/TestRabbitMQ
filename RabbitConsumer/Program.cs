using RabbitConsumer.Consumers;
using RabbitMQ.Client;


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

        await InitializeQuorumQueue(channel);


        // 3. Start Classes
        await new ErrorConsumer(channel, "log.error", "ERROR_WORKER").StartAsync();
        await new LogConsumer(channel, "log.info", "INFO_WORKER").StartAsync();
        await new LogConsumer(channel, "log.all", "ALL_WORKER").StartAsync();
        await new DeadLetterConsumer(channel, dlxQueue, "DLX_WORKER").StartAsync();
        await new QuorumLogConsumer(channel, "quorum.logs.critical", "QUOROM_WORKER").StartAsync();

        Console.WriteLine("All consumers started. Waiting for logs...");
        await Task.Delay(-1);
    }

    public static async Task InitializeQuorumQueue(IChannel channel)
    {
        const string queueName = "quorum.logs.critical";
        const string exchangeName = "quorum.main.topic";

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Topic, false, true); //enusre main exchange exists


        var quorumArgs = new Dictionary<string, object>
    {
        { "x-queue-type", "quorum" },                // Required for Quorum
        { "x-quorum-initial-group-size", 3 },       // Create 3 replicas (Leader + 2 Followers)
        { "x-delivery-limit", 5 },                  // Drop/DLX after 5 failed tries
        { "x-dead-letter-exchange", "dlx.exchange" },// Where to send "poison" messages
        { "x-dead-letter-routing-key", "dead.logs" }
     };

        // Quorum queues MUST be durable: true, exclusive: false
        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: quorumArgs);

        await channel.QueueBindAsync(queueName, exchangeName, "log.critical.#");
    }
}

