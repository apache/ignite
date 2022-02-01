/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using Apache.Ignite.Core.Resource;

namespace dotnet_helloworld
{
    public class CollocationgComputationsWithData
    {
        // tag::affinityRun[]

        class MyComputeAction : IComputeAction
        {
            [InstanceResource] private readonly IIgnite _ignite;

            public int Key { get; set; }

            public void Invoke()
            {
                var cache = _ignite.GetCache<int, string>("myCache");
                // Peek is a local memory lookup
                Console.WriteLine("Co-located [key= " + Key + ", value= " + cache.LocalPeek(Key) + ']');
            }
        }

        public static void AffinityRunDemo()
        {
            var cfg = new IgniteConfiguration();
            // end::affinityRun[]
            var discoverySpi = new TcpDiscoverySpi
            {
                LocalPort = 48500,
                LocalPortRange = 20,
                IpFinder = new TcpDiscoveryStaticIpFinder
                {
                    Endpoints = new[]
                    {
                        "127.0.0.1:48500..48520"
                    }
                }
            };
            cfg.DiscoverySpi = discoverySpi;
            // tag::affinityRun[]
            var ignite = Ignition.Start(cfg);

            var cache = ignite.GetOrCreateCache<int, string>("myCache");
            cache.Put(0, "foo");
            cache.Put(1, "bar");
            cache.Put(2, "baz");
            var keyCnt = 3;
            
            var compute = ignite.GetCompute();

            for (var key = 0; key < keyCnt; key++)
            {
                // This closure will execute on the remote node where
                // data for the given 'key' is located.
                compute.AffinityRun("myCache", key, new MyComputeAction {Key = key});
            }
        }
        // end::affinityRun[]
        
        // tag::calculate-average[]
        // this task sums up the values of the salary field for the given set of keys
        // TODO: APIs are not released yet
        /*
        private class SumTask : IComputeFunc<decimal>
        {
            private readonly ICollection<long> _keys;
            
            [InstanceResource] private IIgnite _ignite;

            public SumTask(ICollection<long> keys)
            {
                _keys = keys;
            }

            public decimal Invoke()
            {
                ICache<long, IBinaryObject> cache = _ignite.GetCache<long, object>("person")
                    .WithKeepBinary<long, IBinaryObject>();

                return _keys.Sum(k => cache[k].GetField<decimal>("salary"));
            }
        }

        public static void CalculateAverage(IIgnite ignite, ICollection<long> keys)
        {
            // get the affinity function configured for the cache
            const string cacheName = "person";
            ICacheAffinity affinity = ignite.GetAffinity(cacheName);

            IEnumerable<IGrouping<int, long>> keysByPartition = keys.GroupBy(affinity.GetPartition);

            decimal total = 0;

            ICompute compute = ignite.GetCompute();

            foreach (IGrouping<int, long> grouping in keysByPartition)
            {
                int partition = grouping.Key;
                long[] partitionKeys = grouping.ToArray();
                decimal sum = compute.AffinityCall(cacheName, partition, new SumTask(partitionKeys));
                total += sum;
            }

            Console.WriteLine("the average salary is " + total / keys.Count);
        }*/

        // end::calculate-average[]


        private class Person
        {
            public string Name { get; set; }
            public decimal Salary { get; set; }
        }
        
        class SumFunc : IComputeFunc<decimal>
        {
            public int PartId { get; set; }
            
            [InstanceResource] private readonly IIgnite _ignite;
            
            public decimal Invoke()
            {
                //use binary objects to avoid deserialization
                var cache = _ignite.GetCache<long, Person>("person").WithKeepBinary<long, IBinaryObject>();

                using (var cursor = cache.Query(new ScanQuery<long, IBinaryObject>{Partition = PartId, Local = true}))
                {
                    return cursor.Sum(entry => entry.Value.GetField<decimal>("salary"));
                }
            }
        }
    }
}
