using System;
using System.Linq;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Client.Cache;
using IgniteExamples.Shared;
using IgniteExamples.Shared.Models;
using IgniteExamples.Shared.ScanQuery;

namespace IgniteExamples.ScanQuery
{
    /// <summary>
    /// ScanQuery example.
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
            // TODO: This example requires .NET with some classes on the server side - update documentation.
            var cfg = new IgniteClientConfiguration("127.0.0.1");

            using (IIgniteClient ignite = Ignition.StartClient(cfg))
            {
                ICacheClient<int, Employee> cache = ignite.GetOrCreateCache<int, Employee>("ThinScanQuery");

                cache[1] = SampleData.GetEmployees().First();

                var query = new ScanQuery<int, Employee>
                {
                    Filter = new EmployeeFilter
                    {
                        EmployeeName = "Wilson"
                    }
                };

                foreach (var cacheEntry in cache.Query(query))
                {
                    Console.WriteLine(cacheEntry);
                }
            }
        }
    }
}
