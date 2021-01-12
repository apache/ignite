using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;

namespace PutGet
{
    class Program
    {
        static void Main(string[] args)
        {
            using (IIgnite ignite = Ignition.Start())
            {
                ICache<int, string> cache = ignite.GetOrCreateCache<int, string>("my-cache");

                cache.Put(1, "Hello World!");

                Console.WriteLine(">>> " + cache.Get(1));
            }
        }
    }
}
