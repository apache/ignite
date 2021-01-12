using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Client.Cache;

namespace IgniteExamples.PutGet
{
    /// <summary>
    /// Put/Get example.
    /// <para />
    /// This example requires an Ignite server node. You can start the node in any of the following ways:
    /// * docker run -p 10800:10800 apacheignite/ignite
    /// * dotnet run -p ServerNode.csproj
    /// * ignite.sh/ignite.bat from the distribution
    /// </summary>
    public class Program
    {
        public static void Main()
        {
            var cfg = new IgniteClientConfiguration("127.0.0.1");

            using (IIgniteClient ignite = Ignition.StartClient(cfg))
            {
                ICacheClient<int, string> cache = ignite.GetOrCreateCache<int, string>("PutGetString");

                cache.Put(1, "Hello World!");

                Console.WriteLine(">>> " + cache.Get(1));
            }
        }
    }
}
