namespace IgniteExamples.Thin.ScanQuery
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using IgniteExamples.Shared;
    using IgniteExamples.Shared.Models;
    using IgniteExamples.Shared.ScanQuery;

    /// <summary>
    /// ScanQuery example.
    /// <para />
    /// This example requires an active Ignite server node with <see cref="EmployeeFilter"/> type loaded.
    /// Start ServerNode project one or more times before running this example.
    /// </summary>
    public class Program
    {
        public static void Main()
        {
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
