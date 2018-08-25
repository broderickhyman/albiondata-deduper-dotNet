using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using NATS.Client;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

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
    public static string NatsUrl { get; } = "nats://public:thenewalbiondata@localhost:4222";

    [Option(Description = "Enable Debug Logging", ShortName = "d", LongName = "debug", ShowInHelpText = true)]
    public static bool Debug { get; }

    public static ILoggerFactory LoggerFactory { get; } = new LoggerFactory().AddConsole(Debug ? LogLevel.Debug : LogLevel.Information);
    public static ILogger CreateLogger<T>() => LoggerFactory.CreateLogger<T>();

    private static ManualResetEvent quitEvent = new ManualResetEvent(false);

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
      Console.CancelKeyPress += (sender, args) =>
      {
        quitEvent.Set();
        args.Cancel = true;
      };

      var logger = CreateLogger<Program>();
      logger.LogInformation(RedisAddress);
      logger.LogInformation(NatsUrl);

      logger.LogInformation($"Redis Connected: {RedisConnection.IsConnected}");

      logger.LogInformation($"NATS Connected, ID: {NatsConnection.ConnectedId}");
      var incomingMarketOrders = NatsConnection.SubscribeAsync(MarketOrder.MarketOrdersIngest);
      var incomingMapData = NatsConnection.SubscribeAsync("mapdata.ingest");
      var incomingGoldData = NatsConnection.SubscribeAsync("goldprices.ingest");

      incomingMarketOrders.MessageHandler += HandleMarketOrder;
      incomingMapData.MessageHandler += HandleMapData;
      incomingGoldData.MessageHandler += HandleGoldData;

      incomingMarketOrders.Start();
      logger.LogInformation("Listening for Market Order Data");
      incomingMapData.Start();
      logger.LogInformation("Listening for Map Data");
      incomingGoldData.Start();
      logger.LogInformation("Listening for Gold Data");

      quitEvent.WaitOne();
      NatsConnection.Close();
      RedisConnection.Dispose();
    }

    private static void HandleMarketOrder(object sender, MsgHandlerEventArgs args)
    {
      var logger = CreateLogger<Program>();
      var message = args.Message;
      try
      {
        var marketUpload = JsonConvert.DeserializeObject<MarketUpload>(Encoding.UTF8.GetString(message.Data));
        using (var md5 = MD5.Create())
        {
          logger.LogDebug($"Processing {marketUpload.Orders.Count} Market Orders");
          foreach (var order in marketUpload.Orders)
          {
            var hash = Encoding.UTF8.GetString(md5.ComputeHash(Encoding.UTF8.GetBytes(order.ToString())));
            var key = $"{message.Subject}-{hash}";
            if (!IsDupedMessage(logger, key))
            {
              NatsConnection.Publish(MarketOrder.MarketOrdersDeduped, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(order)));
            }
          }
        }
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error handling market order");
      }
    }

    private static void HandleMapData(object sender, MsgHandlerEventArgs args)
    {
      var logger = CreateLogger<Program>();
      var message = args.Message;
      try
      {
        using (var md5 = MD5.Create())
        {
          logger.LogInformation("Processing Map Data");
          var hash = Encoding.UTF8.GetString(md5.ComputeHash(message.Data));
          var key = $"{message.Subject}-{hash}";
          if (!IsDupedMessage(logger, key))
          {
            NatsConnection.Publish("mapdata.dedupedtest", message.Data);
          }
        }
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error handling map data");
      }
    }

    private static void HandleGoldData(object sender, MsgHandlerEventArgs args)
    {
      var logger = CreateLogger<Program>();
      var message = args.Message;
      try
      {
        using (var md5 = MD5.Create())
        {
          logger.LogInformation("Processing Gold Data");
          var hash = Encoding.UTF8.GetString(md5.ComputeHash(message.Data));
          var key = $"{message.Subject}-{hash}";
          if (!IsDupedMessage(logger, key))
          {
            NatsConnection.Publish("goldprices.dedupedtest", message.Data);
          }
        }
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error handling gold data");
      }
    }

    private static bool IsDupedMessage(ILogger logger, string key)
    {
      try
      {
        var value = RedisCache.StringGet(key);
        if (value.IsNullOrEmpty)
        {
          // No value means we have not seen it before
          SetKey(logger, key);
          return false;
        }
        else
        {
          return true;
        }
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error checking redis cache");
      }
      return true;
    }

    private static void SetKey(ILogger logger, string key)
    {
      try
      {
        RedisCache.StringSet(key, 1, TimeSpan.FromSeconds(600));
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error setting redis cache");
      }
    }
  }
}
