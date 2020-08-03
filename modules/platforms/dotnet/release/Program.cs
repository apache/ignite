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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Client;
using Apache.Ignite.Core.Configuration;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using Apache.Ignite.Linq;
using Apache.Ignite.Log4Net;
using Apache.Ignite.NLog;

namespace test_proj
{
    public static class Program
    {
        public static void Main()
        {
            // Don't use async Main - compatibility with C# 7.0
            MainAsync().GetAwaiter().GetResult();
        }

        private static async Task MainAsync()
        {
            InitLoggers();

            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"127.0.0.1:47500"}
                    },
                    SocketTimeout = TimeSpan.FromSeconds(0.3)
                },
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    Port = 10842 
                },
                Localhost = "127.0.0.1",
                Logger = new IgniteNLogLogger()
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cacheCfg = new CacheConfiguration(
                    "cache1",
                    new QueryEntity(typeof(int), typeof(Person)));
                
                var cache = ignite.CreateCache<int, Person>(cacheCfg);
                
                cache.Put(1, new Person(1));
                Debug.Assert(1 == cache[1].Age);

                var resPerson = cache.AsCacheQueryable()
                    .Where(e => e.Key > 0 && e.Value.Name.StartsWith("Person"))
                    .Select(e => e.Value)
                    .Single();
                Debug.Assert(1 == resPerson.Age);

                var clientCfg = new IgniteClientConfiguration("127.0.0.1:10842")
                {
                    Logger = new IgniteLog4NetLogger()
                };
                
                using (var igniteThin = Ignition.StartClient(clientCfg))
                {
                    var cacheThin = igniteThin.GetCache<int, Person>(cacheCfg.Name);
                    var personThin = await cacheThin.GetAsync(1);
                    Debug.Assert("Person-1" == personThin.Name);

                    var personNames = cacheThin.AsCacheQueryable()
                        .Where(e => e.Key != 2 && e.Value.Age < 10)
                        .Select(e => e.Value.Name)
                        .ToArray();
                    Debug.Assert(personNames.SequenceEqual(new[] {"Person-1"}));
                }
            }
        }

        private static void InitLoggers()
        {
            var config = new NLog.Config.LoggingConfiguration();

            var target = new NLog.Targets.ColoredConsoleTarget();
            config.AddTarget("logfile", target);

            config.LoggingRules.Add(new NLog.Config.LoggingRule("*", NLog.LogLevel.Info, target));
            NLog.LogManager.Configuration = config;
        }
    }

    public class Person
    {
        public Person(int age)
        {
            Age = age;
            Name = $"Person-{age}";
        }

        [QuerySqlField]
        public string Name { get; }
        
        [QuerySqlField]
        public int Age { get; }
    }
}
