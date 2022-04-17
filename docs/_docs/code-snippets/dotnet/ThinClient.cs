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
using System.Linq;
using System.Threading;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Client.Cache;
using Apache.Ignite.Core.Client.Compute;
using Apache.Ignite.Core.Client.Services;
using Apache.Ignite.Core.Configuration;
using Apache.Ignite.Core.Log;
using Apache.Ignite.Core.Services;

namespace dotnet_helloworld
{
    public class ThinClient
    {
        public static void ThinClientConnecting()
        {
            //tag::connecting[]
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new[] {"127.0.0.1:10800"}
            };

            using (var client = Ignition.StartClient(cfg))
            {
                var cache = client.GetOrCreateCache<int, string>("cache");
                cache.Put(1, "Hello, World!");
            }

            //end::connecting[]
        }

        public static void ThinClientCacheOperations()
        {
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new[] {"127.0.0.1:10800"}
            };
            using (var client = Ignition.StartClient(cfg))
            {
                //tag::createCache[]
                var cacheCfg = new CacheClientConfiguration
                {
                    Name = "References",
                    CacheMode = CacheMode.Replicated,
                    WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync
                };
                var cache = client.GetOrCreateCache<int, string>(cacheCfg);
                //end::createCache[]

                //tag::basicOperations[]
                var data = Enumerable.Range(1, 100).ToDictionary(e => e, e => e.ToString());

                cache.PutAll(data);

                var replace = cache.Replace(1, "2", "3");
                Console.WriteLine(replace); //false

                var value = cache.Get(1);
                Console.WriteLine(value); //1

                replace = cache.Replace(1, "1", "3");
                Console.WriteLine(replace); //true

                value = cache.Get(1);
                Console.WriteLine(value); //3

                cache.Put(101, "101");

                cache.RemoveAll(data.Keys);
                var sizeIsOne = cache.GetSize() == 1;
                Console.WriteLine(sizeIsOne); //true

                value = cache.Get(101);
                Console.WriteLine(value); //101

                cache.RemoveAll();
                var sizeIsZero = cache.GetSize() == 0;
                Console.WriteLine(sizeIsZero); //true
                //end::basicOperations[]
            }
        }

        //tag::scanQry[]
        class NameFilter : ICacheEntryFilter<int, Person>
        {
            public bool Invoke(ICacheEntry<int, Person> entry)
            {
                return entry.Value.Name.Contains("Smith");
            }
        }
        //end::scanQry[]

        public static void ScanQueryFilterDemo()
        {
            using (var ignite = Ignition.Start())
            {
                var cfg = new IgniteClientConfiguration
                {
                    Endpoints = new[] {"127.0.0.1:10800"}
                };
                using (var client = Ignition.StartClient(cfg))
                {
                    //tag::scanQry2[]
                    var cache = client.GetOrCreateCache<int, Person>("personCache");

                    cache.Put(1, new Person {Name = "John Smith"});
                    cache.Put(2, new Person {Name = "John Johnson"});

                    using (var cursor = cache.Query(new ScanQuery<int, Person>(new NameFilter())))
                    {
                        foreach (var entry in cursor)
                        {
                            Console.WriteLine("Key = " + entry.Key + ", Name = " + entry.Value.Name);
                        }
                    }

                    //end::scanQry2[]

                    //tag::handleNodeFailure[]
                    var scanQry = new ScanQuery<int, Person>(new NameFilter());
                    using (var cur = cache.Query(scanQry))
                    {
                        var res = cur.GetAll().ToDictionary(entry => entry.Key, entry => entry.Value);
                    }

                    //end::handleNodeFailure[]
                }
            }
        }

        public static void WorkingWithBinaryObjects()
        {
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new[] {"127.0.0.1:10800"}
            };
            using (var client = Ignition.StartClient(cfg))
            {
                //tag::binaryObj[]
                var binary = client.GetBinary();

                var val = binary.GetBuilder("Person")
                    .SetField("id", 1)
                    .SetField("name", "Joe")
                    .Build();

                var cache = client.GetOrCreateCache<int, object>("persons").WithKeepBinary<int, IBinaryObject>();

                cache.Put(1, val);

                var value = cache.Get(1);
                //end::binaryObj[]
            }
        }

        public static void ExecutingSql()
        {
            using (var ignite = Ignition.Start())
            {
                var cfg = new IgniteClientConfiguration
                {
                    Endpoints = new[] {"127.0.0.1:10800"}
                };
                using (var client = Ignition.StartClient(cfg))
                {
                    //tag::executingSql[]
                    var cache = client.GetOrCreateCache<int, Person>("Person");
                    cache.Query(new SqlFieldsQuery(
                            $"CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR) WITH \"VALUE_TYPE={typeof(Person)}\"")
                        {Schema = "PUBLIC"}).GetAll();

                    var key = 1;
                    var val = new Person {Id = key, Name = "Person 1"};

                    cache.Query(
                        new SqlFieldsQuery("INSERT INTO Person(id, name) VALUES(?, ?)")
                        {
                            Arguments = new object[] {val.Id, val.Name},
                            Schema = "PUBLIC"
                        }
                    ).GetAll();

                    var cursor = cache.Query(
                        new SqlFieldsQuery("SELECT name FROM Person WHERE id = ?")
                        {
                            Arguments = new object[] {key},
                            Schema = "PUBLIC"
                        }
                    );

                    var results = cursor.GetAll();

                    var first = results.FirstOrDefault();
                    if (first != null)
                    {
                        Console.WriteLine("name = " + first[0]);
                    }

                    //end::executingSql[]
                }
            }
        }

        public static void EnablingSsl()
        {
            //tag::ssl[]
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new[] {"127.0.0.1:10800"},
                SslStreamFactory = new SslStreamFactory
                {
                    CertificatePath = ".../certs/client.pfx",
                    CertificatePassword = "password",
                }
            };
            using (var client = Ignition.StartClient(cfg))
            {
                //...
            }

            //end::ssl[]
        }

        public static void Authentication()
        {
            //tag::auth[]
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new[] {"127.0.0.1:10800"},
                UserName = "ignite",
                Password = "ignite"
            };
            using (var client = Ignition.StartClient(cfg))
            {
                //...
            }

            //end::auth[]
        }

        public static void ClusterConfig()
        {
            //tag::clusterConfiguration[]
            var cfg = new IgniteConfiguration
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    // Set a port range from 10000 to 10005
                    Port = 10000,
                    PortRange = 5
                }
            };

            var ignite = Ignition.Start(cfg);
            //end::clusterConfiguration[]
        }

		public static void Discovery()
		{
            //tag::discovery[]
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new[] {"127.0.0.1:10800"},
                EnablePartitionAwareness = true,

                // Enable trace logging to observe discovery process.
                Logger = new ConsoleLogger { MinLevel = LogLevel.Trace }
            };

            var client = Ignition.StartClient(cfg);

            // Perform any operation and sleep to let the client discover
            // server nodes asynchronously.
            client.GetCacheNames();
            Thread.Sleep(1000);

            foreach (IClientConnection connection in client.GetConnections())
            {
                Console.WriteLine(connection.RemoteEndPoint);
            }
            //end::discovery[]
        }

        public static void ClientCluster()
        {
            var cfg = new IgniteClientConfiguration();
            //tag::client-cluster[]
            IIgniteClient client = Ignition.StartClient(cfg);
            IClientCluster cluster = client.GetCluster();
            cluster.SetActive(true);
            cluster.EnableWal("my-cache");
            //end::client-cluster[]
        }

        public static void ClientClusterGroups()
        {
            var cfg = new IgniteClientConfiguration();
            //tag::client-cluster-groups[]
            IIgniteClient client = Ignition.StartClient(cfg);
            IClientClusterGroup serversInDc1 = client.GetCluster().ForServers().ForAttribute("dc", "dc1");
            foreach (IClientClusterNode node in serversInDc1.GetNodes())
                Console.WriteLine($"Node ID: {node.Id}");
            //end::client-cluster-groups[]
        }

        public static void Compute()
        {
            //tag::client-compute-setup[]
            var igniteCfg = new IgniteConfiguration
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    ThinClientConfiguration = new ThinClientConfiguration
                    {
                        MaxActiveComputeTasksPerConnection = 10
                    }
                }
            };

            IIgnite ignite = Ignition.Start(igniteCfg);
            //end::client-compute-setup[]

            var cfg = new IgniteClientConfiguration();
            //tag::client-compute-task[]
            IIgniteClient client = Ignition.StartClient(cfg);
            IComputeClient compute = client.GetCompute();
            int result = compute.ExecuteJavaTask<int>("org.foo.bar.AddOneTask", 1);
            //end::client-compute-task[]
        }

        public static void Services()
        {
            var cfg = new IgniteClientConfiguration();
            //tag::client-services[]
            IIgniteClient client = Ignition.StartClient(cfg);
            IServicesClient services = client.GetServices();
            IMyService serviceProxy = services.GetServiceProxy<IMyService>("MyService");
            serviceProxy.MyServiceMethod("hello");
            //end::client-services[]

        }

        private interface IMyService
        {
            void MyServiceMethod(string val);
        }
    }
}
