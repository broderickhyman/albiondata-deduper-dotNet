using McMaster.Extensions.CommandLineUtils;
using System;

namespace albiondata_deduper_dotNet
{
    public class Program
    {
        private static void Main(string[] args) => CommandLineApplication.Execute<Program>(args);

        [Option(Description = "Redis URL", ShortName = "r", ShowInHelpText = true)]
        public string RedisAddress { get; } = "localhost:6379";

        [Option(Description = "Redis Password", ShortName = "p", ShowInHelpText = true)]
        public string RedisPassword { get; } = "";

        [Option(Description = "NATS Url", ShortName = "n", ShowInHelpText = true)]
        public string NatsUrl { get; } = "nats://localhost:4222";

        private void OnExecute()
        {
            Console.WriteLine(RedisAddress);
            Console.WriteLine(RedisPassword);
            Console.WriteLine(NatsUrl);

            Console.ReadKey();
        }
    }
}
