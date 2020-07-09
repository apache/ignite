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

namespace Apache.Ignite.BenchmarkDotNet
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;

    /// <summary>
    /// Benchmark utils.
    /// </summary>
    internal static class Utils
    {
        /// <summary>
        /// Gets Ignite config.
        /// </summary>
        public static IgniteConfiguration GetIgniteConfiguration()
        {
            Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_CLASSPATH", "true");
            Environment.SetEnvironmentVariable("IGNITE_NET_SUPPRESS_JAVA_ILLEGAL_ACCESS_WARNINGS", "true");

            return new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] { "127.0.0.1:47500" }
                    },
                    SocketTimeout = TimeSpan.FromSeconds(0.3)
                },
                Localhost = "127.0.0.1",
                JvmOptions = new[]
                {
                    "-Xms1g",
                    "-Xmx4g",
                    "-DIGNITE_QUIET=true",
                },
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    ThinClientConfiguration = new ThinClientConfiguration
                    {
                        MaxActiveComputeTasksPerConnection = 100
                    }
                }
            };
        }

        /// <summary>
        /// Gets Ignite client config.
        /// </summary>
        public static IgniteClientConfiguration GetIgniteClientConfiguration()
        {
            return new IgniteClientConfiguration("127.0.0.1");
        }
    }
}