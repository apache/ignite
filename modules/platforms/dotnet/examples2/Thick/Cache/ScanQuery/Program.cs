using System;
using System.Linq;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Query;
using IgniteExamples.Shared;
using IgniteExamples.Shared.Models;
using IgniteExamples.Shared.ScanQuery;

namespace IgniteExamples.Thick.ScanQuery
{
    /// <summary>
    /// ScanQuery example.
    /// </summary>
    public class Program
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start())
            {
                ICache<int, Employee> cache = ignite.GetOrCreateCache<int, Employee>("ScanQuery");

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
                    Console.WriteLine(">>> " + cacheEntry);
                }
            }
        }
    }
}
