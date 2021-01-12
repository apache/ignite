using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Client.Cache;

namespace PutGet
{
    class Program
    {
        static void Main(string[] args)
        {
            var cfg = new IgniteClientConfiguration("127.0.0.1");

            using (IIgniteClient ignite = Ignition.StartClient(cfg))
            {
                ICacheClient<int, string> cache = ignite.GetOrCreateCache<int, string>("my-cache");

                cache.Put(1, "Hello World!");

                Console.WriteLine(">>> " + cache.Get(1));
            }
        }
    }
}
