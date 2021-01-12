using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;

namespace IgniteExamples.Thick.PutGet
{
    /// <summary>
    /// Put/Get example.
    /// </summary>
    public class Program
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start())
            {
                ICache<int, string> cache = ignite.GetOrCreateCache<int, string>("PutGetString");

                cache.Put(1, "Hello World!");

                Console.WriteLine(">>> " + cache.Get(1));
            }
        }
    }
}
