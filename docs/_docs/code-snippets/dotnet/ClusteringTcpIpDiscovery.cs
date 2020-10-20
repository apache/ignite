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

using Apache.Ignite.Core;
using Apache.Ignite.Core.Communication.Tcp;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Multicast;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    class ClusteringTcpIpDiscovery
    {
        public static void MulticastIpFinderDemo()
        {
            //tag::multicast[]
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryMulticastIpFinder
                    {
                        MulticastGroup = "228.10.10.157"
                    }
                }
            };
            Ignition.Start(cfg);
            //end::multicast[]
        }

        public static void StaticIpFinderDemo()
        {
            //tag::static[]
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"1.2.3.4", "1.2.3.5:47500..47509" }
                    }
                }
            };
            //end::static[]
            Ignition.Start(cfg);
        }

        public static void MulticastAndStaticDemo()
        {
            //tag::multicastAndStatic[]
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryMulticastIpFinder
                    {
                        MulticastGroup = "228.10.10.157",
                        Endpoints = new[] {"1.2.3.4", "1.2.3.5:47500..47509" }
                    }
                }
            };
            Ignition.Start(cfg);
            //end::multicastAndStatic[]
        }
        
        public static void IsolatedClustersDemo()
        {
            // tag::isolated1[]
            var firstCfg = new IgniteConfiguration
            {
                IgniteInstanceName = "first",
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
                },
                CommunicationSpi = new TcpCommunicationSpi
                {
                    LocalPort = 48100
                }
            };
            Ignition.Start(firstCfg);
            // end::isolated1[]

            // tag::isolated2[]
            var secondCfg = new IgniteConfiguration
            {
                IgniteInstanceName = "second",
                DiscoverySpi = new TcpDiscoverySpi
                {
                    LocalPort = 49500,
                    LocalPortRange = 20,
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[]
                        {
                            "127.0.0.1:49500..49520"
                        }
                    }
                },
                CommunicationSpi = new TcpCommunicationSpi
                {
                    LocalPort = 49100
                }
            };
            Ignition.Start(secondCfg);
            // end::isolated2[]
            
        }
    }
}
