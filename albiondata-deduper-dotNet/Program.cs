using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using NATS.Client;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Security.Cryptography;
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
    public static string NatsUrl { get; } = "nats://public:thenewalbiondata@albion-online-data.com:4222";// = "nats://localhost:4222";

    public static ILoggerFactory LoggerFactory { get; } = new LoggerFactory().AddConsole();
    public static ILogger CreateLogger<T>() => LoggerFactory.CreateLogger<T>();

    private static readonly Lazy<ConnectionMultiplexer> lazyRedis = new Lazy<ConnectionMultiplexer>(() =>
    {
      var config = new ConfigurationOptions();
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
        return lazyRedis.Value;
      }
    }

    public static IDatabase RedisCache
    {
      get
      {
        return RedisConnection.GetDatabase(0);
      }
    }

    private static readonly Lazy<IConnection> lazyNats = new Lazy<IConnection>(() =>
    {
      var natsFactory = new ConnectionFactory();
      return natsFactory.CreateConnection(NatsUrl);
    });

    public static IConnection NatsConnection
    {
      get
      {
        return lazyNats.Value;
      }
    }

    private void OnExecute()
    {
      var logger = CreateLogger<Program>();
      logger.LogInformation(RedisAddress);
      logger.LogInformation(NatsUrl);

      logger.LogInformation($"Redis Connected: {RedisConnection.IsConnected}");

      logger.LogInformation($"NATS Connected, ID: {NatsConnection.ConnectedId}");
      var incomingMarketOrders = NatsConnection.SubscribeAsync("marketorders.ingest");

      incomingMarketOrders.MessageHandler += HandleMarketOrder;
      incomingMarketOrders.Start();

      Console.ReadKey();
      NatsConnection.Close();
      RedisConnection.Dispose();
    }

    private static void HandleMarketOrder(object sender, MsgHandlerEventArgs args)
    {
      var logger = CreateLogger<Program>();
      var message = args.Message;
      var marketUpload = JsonConvert.DeserializeObject<MarketUpload>(Encoding.UTF8.GetString(message.Data));
      using (var md5 = MD5.Create())
      {
        logger.LogInformation($"Processing {marketUpload.Orders.Count} Market Orders");
        foreach (var order in marketUpload.Orders)
        {
          var hash = Encoding.UTF8.GetString(md5.ComputeHash(Encoding.UTF8.GetBytes(order.ToString())));
          var key = $"{message.Subject}-{hash}";
          if (!IsDupedMessage(key))
          {
            NatsConnection.Publish("marketorders.dedupedtest", Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(order)));
          }
        }
      }
    }

    private static bool IsDupedMessage(string key)
    {
      var value = RedisCache.StringGet(key);
      if (value.IsNullOrEmpty)
      {
        // No value means we have not seen it before
        SetKey(key);
        return false;
      }
      else
      {
        return true;
      }
    }

    private static void SetKey(string key)
    {
      RedisCache.StringSet(key, 1, TimeSpan.FromSeconds(600));
    }
  }
}
