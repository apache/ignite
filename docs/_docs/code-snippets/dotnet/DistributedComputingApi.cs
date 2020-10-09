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
using Apache.Ignite.Core;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using Apache.Ignite.Core.Resource;

namespace dotnet_helloworld
{
    public class DistributedComputingApi
    {
        public static void ForRemotesDemo()
        {
            // tag::forRemotes[]
            var ignite = Ignition.Start();
            var compute = ignite.GetCluster().ForRemotes().GetCompute();
            // end::forRemotes[]
        }

        public static void GetCompute()
        {
            // tag::gettingCompute[]
            var ignite = Ignition.Start();
            var compute = ignite.GetCompute();
            // end::gettingCompute[]
        }

        // tag::computeAction[]
        class PrintWordAction : IComputeAction
        {
            public void Invoke()
            {
                foreach (var s in "Print words on different cluster nodes".Split(" "))
                {
                    Console.WriteLine(s);
                }
            }
        }

        public static void ComputeRunDemo()
        {
            var ignite = Ignition.Start(
                new IgniteConfiguration
                {
                    DiscoverySpi = new TcpDiscoverySpi
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
                    }
                }
            );
            ignite.GetCompute().Run(new PrintWordAction());
        }
        // end::computeAction[]

        // tag::computeFunc[]
        // tag::async[]
        class CharCounter : IComputeFunc<int>
        {
            private readonly string arg;

            public CharCounter(string arg)
            {
                this.arg = arg;
            }

            public int Invoke()
            {
                return arg.Length;
            }
        }
        // end::async[]

        public static void ComputeFuncDemo()
        {
            var ignite = Ignition.Start(
                new IgniteConfiguration
                {
                    DiscoverySpi = new TcpDiscoverySpi
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
                    }
                }
            );

            // Iterate through all words in the sentence and create callable jobs.
            var calls = "How many characters".Split(" ").Select(s => new CharCounter(s)).ToList();

            // Execute the collection of calls on the cluster.
            var res = ignite.GetCompute().Call(calls);

            // Add all the word lengths received from cluster nodes.
            var total = res.Sum();
        }
        // end::computeFunc[]


        // tag::computeFuncApply[]
        class CharCountingFunc : IComputeFunc<string, int>
        {
            public int Invoke(string arg)
            {
                return arg.Length;
            }
        }

        public static void Foo()
        {
            var ignite = Ignition.Start(
                new IgniteConfiguration
                {
                    DiscoverySpi = new TcpDiscoverySpi
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
                    }
                }
            );

            var res = ignite.GetCompute().Apply(new CharCountingFunc(), "How many characters".Split());

            int total = res.Sum();
        }
        // end::computeFuncApply[]

        // tag::broadcast[]
        class PrintNodeIdAction : IComputeAction
        {
            public void Invoke()
            {
                Console.WriteLine("Hello node: " +
                                  Ignition.GetIgnite().GetCluster().GetLocalNode().Id);
            }
        }

        public static void BroadcastDemo()
        {
            var ignite = Ignition.Start(
                new IgniteConfiguration
                {
                    DiscoverySpi = new TcpDiscoverySpi
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
                    }
                }
            );

            // Limit broadcast to remote nodes only.
            var compute = ignite.GetCluster().ForRemotes().GetCompute();
            // Print out hello message on remote nodes in the cluster group.
            compute.Broadcast(new PrintNodeIdAction());
        }
        // end::broadcast[]

        // tag::async[]
        public static void AsyncDemo()
        {
            var ignite = Ignition.Start(
                new IgniteConfiguration
                {
                    DiscoverySpi = new TcpDiscoverySpi
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
                    }
                }
            );

            var calls = "Count character using async compute"
                .Split(" ").Select(s => new CharCounter(s)).ToList();

            var future = ignite.GetCompute().CallAsync(calls);

            future.ContinueWith(fut =>
            {
                var total = fut.Result.Sum();
                Console.WriteLine("Total number of characters: " + total);
            });
        }
        // end::async[]

        // tag::instanceResource[]
        class FuncWithDataAccess : IComputeFunc<int>
        {
            [InstanceResource] private IIgnite _ignite;

            public int Invoke()
            {
                var cache = _ignite.GetCache<int, string>("someCache");

                // get the data you need
                string cached = cache.Get(1);
                
                // do with data what you need to do, for example:
                Console.WriteLine(cached);

                return 1;
            }
        }
        // end::instanceResource[]

        public static void InstanceResourceDemo()
        {
            var ignite = Ignition.Start(
                new IgniteConfiguration
                {
                    DiscoverySpi = new TcpDiscoverySpi
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
                    }
                }
            );

            var cache = ignite.GetOrCreateCache<int, string>("someCache");
            cache.Put(1, "foo");
            ignite.GetCompute().Call(new FuncWithDataAccess());


        }
    }
}
