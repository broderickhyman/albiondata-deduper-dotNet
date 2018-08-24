using McMaster.Extensions.CommandLineUtils;
using NATS.Client;
using StackExchange.Redis;
using System;
using System.Text;

namespace albiondata_deduper_dotNet
{
    public class Program
    {
        private static void Main(string[] args) => CommandLineApplication.Execute<Program>(args);

        [Option(Description = "Redis URL", ShortName = "r", ShowInHelpText = true)]
        public static string RedisAddress { get; } = "localhost:6379";

        [Option(Description = "Redis Password", ShortName = "p", ShowInHelpText = true)]
        public static string RedisPassword { get; } = "";

        [Option(Description = "NATS Url", ShortName = "n", ShowInHelpText = true)]
        public static string NatsUrl { get; } = "nats://localhost:4222";

        private static readonly Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            ConfigurationOptions config = new ConfigurationOptions();
            config.EndPoints.Add(RedisAddress);
            config.Password = RedisPassword;
            config.AbortOnConnectFail = false;
            config.ConnectRetry = 5;
            config.ConnectTimeout = 1000;
            return ConnectionMultiplexer.Connect(config);
        });

        public static ConnectionMultiplexer RedisConnection
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        private void OnExecute()
        {
            Console.WriteLine(RedisAddress);
            Console.WriteLine(RedisPassword);
            Console.WriteLine(NatsUrl);

            var cache = RedisConnection.GetDatabase(db: 0);
            Console.WriteLine("Redis Connected");
            Console.WriteLine(cache.Execute("PING"));

            var natsFactory = new ConnectionFactory();
            var natsConnection = natsFactory.CreateConnection(NatsUrl);
            Console.WriteLine("NATS Connected");

            var fooSubscription = natsConnection.SubscribeAsync("foo");
            fooSubscription.MessageHandler += (sender, args) =>
            {
                Console.WriteLine(args.Message);
            };
            fooSubscription.Start();

            natsConnection.Publish("foo", Encoding.UTF8.GetBytes("hello world"));

            Console.ReadKey();
            natsConnection.Close();
            RedisConnection.Dispose();
        }
    }
}
