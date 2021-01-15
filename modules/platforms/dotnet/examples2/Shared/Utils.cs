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
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Multicast;
using Apache.Ignite.Core.Log;

namespace IgniteExamples.Shared
{
    using System;

    public static class Utils
    {
        static Utils()
        {
            Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_CLASSPATH", "true");
        }

        public static IgniteConfiguration GetServerNodeConfiguration()
        {
            // None of the options below are mandatory for the examples to work.
            // * Discovery and localhost settings improve startup time
            // * Logging options reduce console output
            return new IgniteConfiguration
            {
                Localhost = "127.0.0.1",
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryMulticastIpFinder
                    {
                        Endpoints = new[]
                        {
                            "127.0.0.1:47500..47502"
                        }
                    }
                },
                JvmOptions = new[]
                {
                    "-DIGNITE_QUIET=true",
                    "-DIGNITE_PERFORMANCE_SUGGESTIONS_DISABLED=true"
                },
                Logger = new ConsoleLogger
                {
                    MinLevel = LogLevel.Error
                }
            };
        }

        public static IgniteClientConfiguration GetThinClientConfiguration()
        {
            return new IgniteClientConfiguration
            {
                Endpoints = new[]
                {
                    "127.0.0.1"
                }
            };
        }
    }
}
