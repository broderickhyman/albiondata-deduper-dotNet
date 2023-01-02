using AlbionData.Models;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using NATS.Client;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace albiondata_deduper_dotNet
{
  public class Program
  {
    private static void Main(string[] args) => CommandLineApplication.Execute<Program>(args);

    [Option(Description = "Redis URL", ShortName = "r", ShowInHelpText = true)]
    public static string RedisAddress { get; set; } = "localhost:6379";

    [Option(Description = "Redis Password", ShortName = "p", ShowInHelpText = true)]
    public static string RedisPassword { get; set; } = "";

    [Option(Description = "Incoming NATS Url", ShortName = "n", ShowInHelpText = true)]
    public static string IncomingNatsUrl { get; set; } = "nats://public:thenewalbiondata@albion-online-data.com:4222";

    [Option(Description = "Outgoing NATS Url", ShortName = "o", ShowInHelpText = true)]
    public static string OutgoingNatsUrl { get; set; } = "nats://public:thenewalbiondata@localhost:4222";

    [Option(Description = "Enable Debug Logging", ShortName = "d", LongName = "debug", ShowInHelpText = true)]
    public static bool Debug { get; set; }

    public static ILoggerFactory Logger { get; } = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(Debug ? LogLevel.Debug : LogLevel.Information));
    public static ILogger CreateLogger<T>() => Logger.CreateLogger<T>();

    private static readonly ManualResetEvent quitEvent = new ManualResetEvent(false);

    private static readonly Dictionary<int, string> itemIdMapping = new Dictionary<int, string>();

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
    private const string marketHistoriesIngest = "markethistories.ingest";
    private const string mapDataIngest = "mapdata.ingest";
    private const string goldDataIngest = "goldprices.ingest";
    private const string marketOrdersDeduped = "marketorders.deduped";
    private const string marketOrdersDedupedBulk = "marketorders.deduped.bulk";
    private const string marketHistoriesDeduped = "markethistories.deduped";
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
      {
        logger.LogInformation("Debugging enabled");
      }

      logger.LogInformation($"Redis Connected: {RedisConnection.IsConnected}");

      var itemIdFile = new HttpClient().GetStringAsync("https://raw.githubusercontent.com/ao-data/ao-bin-dumps/master/formatted/items.txt").Result;
      foreach (var line in itemIdFile.Split("\n", StringSplitOptions.RemoveEmptyEntries))
      {
        var split = line.Split(':').Select(x => x.Trim());
        if (split.Any())
        {
          itemIdMapping.Add(int.Parse(split.First()), split.Skip(1).First());
        }
      }

      logger.LogInformation($"Incoming NATS Connected, ID: {IncomingNatsConnection.ConnectedId}");
      logger.LogInformation($"Outgoing NATS Connected, ID: {OutgoingNatsConnection.ConnectedId}");
      var incomingMarketOrders = IncomingNatsConnection.SubscribeAsync(marketOrdersIngest);
      var incomingHistories = IncomingNatsConnection.SubscribeAsync(marketHistoriesIngest);
      var incomingMapData = IncomingNatsConnection.SubscribeAsync(mapDataIngest);
      var incomingGoldData = IncomingNatsConnection.SubscribeAsync(goldDataIngest);

      incomingMarketOrders.MessageHandler += HandleMarketOrder;
      incomingHistories.MessageHandler += HandleHistory;
      incomingMapData.MessageHandler += HandleMapData;
      incomingGoldData.MessageHandler += HandleGoldData;

      incomingMarketOrders.Start();
      logger.LogInformation("Listening for Market Order Data");
      incomingHistories.Start();
      logger.LogInformation("Listening for Market History Data");
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
        List<MarketOrder> orderArray = new List<MarketOrder>();
        logger.LogInformation($"Processing {marketUpload.Orders.Count} Market Orders - {DateTime.Now.ToLongTimeString()}");
        foreach (var order in marketUpload.Orders)
        {
          // Hack since albion seems to be multiplying every price by 10000?
          order.UnitPriceSilver /= 10000;
          // Make sure all caerleon markets are registered with the same ID since they have the same contents
          if (order.LocationId == (ushort)Location.Caerleon2)
          {
            order.LocationId = (ushort)Location.Caerleon;
          }
          // Into the Fray Portal Towns
          // Make sure all portal markets are registered with the same ID as their corresponding city as they have the same contents
	  switch(order.LocationId)
          {
          case (ushort)Location.ThetfordPortal:
               order.LocationId = (ushort)Location.Thetford;
               break;
          case (ushort)Location.LymhurstPortal:
               order.LocationId = (ushort)Location.Lymhurst;
               break;
          case (ushort)Location.BridgewatchPortal:
               order.LocationId = (ushort)Location.Bridgewatch;
               break;
          case (ushort)Location.MartlockPortal:
               order.LocationId = (ushort)Location.Martlock;
               break;
          case (ushort)Location.FortSterlingPortal:
               order.LocationId = (ushort)Location.FortSterling;
               break;
          }
          // Make the hash unique while also including anything that could change
          var hash = $"{order.Id}|{order.LocationId}|{order.Amount}|{order.UnitPriceSilver}|{order.Expires.ToString("s")}";
          var key = $"{message.Subject}-{hash}";
          if (!IsDupedMessage(logger, key))
          {
            OutgoingNatsConnection.Publish(marketOrdersDeduped, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(order)));
            orderArray.Add(order);
          }
        }

        if (orderArray.Count > 0)
        {
          logger.LogInformation($"Found {orderArray.Count} New Market Orders - {DateTime.Now.ToLongTimeString()}");
          OutgoingNatsConnection.Publish(marketOrdersDedupedBulk, Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(orderArray)));
        }
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error handling market order");
      }
    }

    private static void HandleHistory(object sender, MsgHandlerEventArgs args)
    {
      var logger = CreateLogger<Program>();
      var message = args.Message;
      try
      {
        var marketHistoriesUpload = JsonConvert.DeserializeObject<MarketHistoriesUpload>(Encoding.UTF8.GetString(message.Data));

        // Sort history by descending time so the newest is always first in the list
        marketHistoriesUpload.MarketHistories.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp) * -1);

        using var md5 = MD5.Create();
        logger.LogInformation($"Processing {marketHistoriesUpload.MarketHistories.Count} Market Histories - {DateTime.Now.ToLongTimeString()}");
        foreach (var marketHistory in marketHistoriesUpload.MarketHistories)
        {
          // Hack since albion seems to be multiplying every price by 10000?
          marketHistory.SilverAmount /= 10000;
        }

        // Make sure all caerleon markets are registered with the same ID since they have the same contents
        if (marketHistoriesUpload.LocationId == (ushort)Location.Caerleon2)
        {
          marketHistoriesUpload.LocationId = (ushort)Location.Caerleon;
        }

        // Lookup the unique name based on the numeric ID
        itemIdMapping.TryGetValue((int)marketHistoriesUpload.AlbionId, out marketHistoriesUpload.AlbionIdString);

        var newUploadStringBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(marketHistoriesUpload));

        var hash = Encoding.UTF8.GetString(md5.ComputeHash(newUploadStringBytes));
        var key = $"{message.Subject}-{hash}";

        var expire = TimeSpan.FromHours(6);
        if (marketHistoriesUpload.Timescale == Timescale.Day)
        {
          expire = TimeSpan.FromHours(1);
        }

        if (!IsDupedMessage(logger, key, expire))
        {
          OutgoingNatsConnection.Publish(marketHistoriesDeduped, newUploadStringBytes);
        }
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error handling market history");
      }
    }

    private static void HandleMapData(object sender, MsgHandlerEventArgs args)
    {
      var logger = CreateLogger<Program>();
      var message = args.Message;
      try
      {
        using var md5 = MD5.Create();
        logger.LogInformation("Processing Map Data");
        var hash = Encoding.UTF8.GetString(md5.ComputeHash(message.Data));
        var key = $"{message.Subject}-{hash}";
        if (!IsDupedMessage(logger, key))
        {
          OutgoingNatsConnection.Publish(mapDataDeduped, message.Data);
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
        using var md5 = MD5.Create();
        logger.LogInformation("Processing Gold Data");
        var hash = Encoding.UTF8.GetString(md5.ComputeHash(message.Data));
        var key = $"{message.Subject}-{hash}";
        if (!IsDupedMessage(logger, key))
        {
          OutgoingNatsConnection.Publish(goldDataDeduped, message.Data);
        }
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error handling gold data");
      }
    }

    /// <summary>
    /// Check if a message has been seen recently, otherwise set the redis key
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="key"></param>
    private static bool IsDupedMessage(ILogger logger, string key, TimeSpan expire = default)
    {
      try
      {
        var value = RedisCache.StringGet(key);
        if (value.IsNullOrEmpty)
        {
          // No value means we have not seen it before
          // Only set the key when new so that messages will be sent every X minutes so the item can be marked as still available
          SetKey(logger, key, expire);
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

    private static void SetKey(ILogger logger, string key, TimeSpan expire = default)
    {
      try
      {
        if (expire == default)
        {
          expire = TimeSpan.FromSeconds(600);
        }
        RedisCache.StringSet(key, 1, expire);
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Error setting redis cache");
      }
    }
  }
}
