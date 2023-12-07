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

namespace Apache.Ignite.Examples.Shared
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Deployment;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Multicast;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Examples.Shared.Models;
    using Apache.Ignite.Examples.Shared.Services;

    /// <summary>
    /// Common configuration and sample data.
    /// </summary>
    public static class Utils
    {
        /// <summary>
        /// Initializes the <see cref="Utils"/> class.
        /// </summary>
        static Utils()
        {
            // Only necessary during Ignite development.
            Environment.SetEnvironmentVariable("IGNITE_NATIVE_TEST_CLASSPATH", "true");
        }

        /// <summary>
        /// Gets the server node configuration.
        /// </summary>
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
                    "-DIGNITE_PERFORMANCE_SUGGESTIONS_DISABLED=true",
                    "-DIGNITE_UPDATE_NOTIFIER=false"
                },
                Logger = new ConsoleLogger
                {
                    MinLevel = LogLevel.Error
                },
                PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain
            };
        }

        /// <summary>
        /// Gets the thick client node configuration.
        /// </summary>
        public static IgniteConfiguration GetClientNodeConfiguration()
        {
            return new IgniteConfiguration(GetServerNodeConfiguration())
            {
                ClientMode = true
            };
        }

        /// <summary>
        /// Gets the thin client node configuration.
        /// </summary>
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

        /// <summary>
        /// Populates the cache with employee data.
        /// </summary>
        public static void PopulateCache(ICache<int, Employee> cache)
        {
            var id = 0;

            foreach (var employee in GetSampleEmployees())
                cache.Put(id++, employee);
        }

        /// <summary>
        /// Populates the cache with employee data.
        /// </summary>
        public static void PopulateCache(ICacheClient<int, Employee> cache)
        {
            var id = 0;

            foreach (var employee in GetSampleEmployees())
                cache.Put(id++, employee);
        }

        /// <summary>
        /// Gets the sample employee data.
        /// </summary>
        public static IEnumerable<Employee> GetSampleEmployees()
        {
            yield return new Employee(
                "James Wilson",
                12500,
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                new[] {"Human Resources", "Customer Service"},
                1);

            yield return new Employee(
                "Daniel Adams",
                11000,
                new Address("184 Fidler Drive, San Antonio, TX", 78130),
                new[] {"Development", "QA"},
                1);

            yield return new Employee(
                "Cristian Moss",
                12500,
                new Address("667 Jerry Dove Drive, Florence, SC", 29501),
                new[] {"Logistics"},
                1);

            yield return new Employee(
                "Allison Mathis",
                25300,
                new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                new[] {"Development"},
                2);

            yield return new Employee(
                "Breana Robbin",
                6500,
                new Address("3960 Sundown Lane, Austin, TX", 78130),
                new[] {"Sales"},
                2);

            yield return new Employee(
                "Philip Horsley",
                19800,
                new Address("2803 Elsie Drive, Sioux Falls, SD", 57104),
                new[] {"Sales"},
                2);

            yield return new Employee(
                "Brian Peters",
                10600,
                new Address("1407 Pearlman Avenue, Boston, MA", 12110),
                new[] {"Development", "QA"},
                2);
        }

        /// <summary>
        /// Deploys default services.
        /// </summary>
        public static void DeployDefaultServices(IIgnite ignite)
        {
            ignite.GetServices().DeployNodeSingleton("default-map-service", new MapService<int, string>());
        }
    }
}
