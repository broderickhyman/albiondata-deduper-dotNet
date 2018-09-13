using AlbionData.Models;
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

    [Option(Description = "Incoming NATS Url", ShortName = "n", ShowInHelpText = true)]
    public static string IncomingNatsUrl { get; } = "nats://public:thenewalbiondata@albion-online-data.com:4222";

    [Option(Description = "Outgoing NATS Url", ShortName = "o", ShowInHelpText = true)]
    public static string OutgoingNatsUrl { get; } = "nats://public:thenewalbiondata@localhost:4222";

    [Option(Description = "Enable Debug Logging", ShortName = "d", LongName = "debug", ShowInHelpText = true)]
    public static bool Debug { get; }

    public static ILoggerFactory LoggerFactory { get; } = new LoggerFactory().AddConsole(Debug ? LogLevel.Debug : LogLevel.Information);
    public static ILogger CreateLogger<T>() => LoggerFactory.CreateLogger<T>();

    private static readonly ManualResetEvent quitEvent = new ManualResetEvent(false);

    #region Connections
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

    private static readonly Lazy<IConnection> lazyIncomingNats = new Lazy<IConnection>(() =>
    {
      var natsFactory = new ConnectionFactory();
      return natsFactory.CreateConnection(IncomingNatsUrl);
    });

    public static IConnection IncomingNatsConnection
    {
      get
      {
        return lazyIncomingNats.Value;
      }
    }

    private static readonly Lazy<IConnection> lazyOutgoingNats = new Lazy<IConnection>(() =>
    {
      var natsFactory = new ConnectionFactory();
      return natsFactory.CreateConnection(OutgoingNatsUrl);
    });

    public static IConnection OutgoingNatsConnection
    {
      get
      {
        return lazyOutgoingNats.Value;
      }
    }
    #endregion
    #region Subjects
    private const string marketOrdersIngest = "marketorders.ingest";
    private const string mapDataIngest = "mapdata.ingest";
    private const string goldDataIngest = "goldprices.ingest";
    private const string marketOrdersDeduped = "marketorders.deduped";
    private const string mapDataDeduped = "mapdata.deduped";
    private const string goldDataDeduped = "goldprices.deduped";
    #endregion

    private void OnExecute()
    {
      Console.CancelKeyPress += (sender, args) =>
      {
        quitEvent.Set();
        args.Cancel = true;
      };

      var logger = CreateLogger<Program>();
      logger.LogInformation($"Redis URL: {RedisAddress}");
      logger.LogInformation($"Incoming Nats URL: {IncomingNatsUrl}");
      logger.LogInformation($"Outgoing Nats URL: {OutgoingNatsUrl}");

      if (Debug)
        logger.LogInformation("Debugging enabled");

      logger.LogInformation($"Redis Connected: {RedisConnection.IsConnected}");

      logger.LogInformation($"Incoming NATS Connected, ID: {IncomingNatsConnection.ConnectedId}");
      logger.LogInformation($"Outgoing NATS Connected, ID: {OutgoingNatsConnection.ConnectedId}");
      var incomingMarketOrders = IncomingNatsConnection.SubscribeAsync(marketOrdersIngest);
      var incomingMapData = IncomingNatsConnection.SubscribeAsync(mapDataIngest);
      var incomingGoldData = IncomingNatsConnection.SubscribeAsync(goldDataIngest);

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
      IncomingNatsConnection.Close();
      OutgoingNatsConnection.Close();
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
          logger.LogInformation($"Processing {marketUpload.Orders.Count} Market Orders - {DateTime.Now.ToLongTimeString()}");
          foreach (var order in marketUpload.Orders)
          {
            // Hack since albion seems to be multiplying every price by 10000?
            order.UnitPriceSilver /= 10000;
            var hash = Encoding.UTF8.GetString(md5.ComputeHash(Encoding.UTF8.GetBytes(order.ToString())));
            var key = $"{message.Subject}-{hash}";
            if (!IsDupedMessage(logger, key))
            {
              OutgoingNatsConnection.Publish(marketOrdersDeduped, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(order)));
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
            OutgoingNatsConnection.Publish(mapDataDeduped, message.Data);
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
            OutgoingNatsConnection.Publish(goldDataDeduped, message.Data);
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
        return false;
      }
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
