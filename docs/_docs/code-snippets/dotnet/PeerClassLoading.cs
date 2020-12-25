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
using Apache.Ignite.Core.Deployment;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class PeerClassLoading
    {
        public static void PeerClassLoadingEnabled()
        {
            //tag::enable[]
            var cfg = new IgniteConfiguration
            {
                PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain
            };
            //end::enable[]
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
            //tag::enable[]
            var ignite = Ignition.Start(cfg);
            //end::enable[]
        }
    }
}
