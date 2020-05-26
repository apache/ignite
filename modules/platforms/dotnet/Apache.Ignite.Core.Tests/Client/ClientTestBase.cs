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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Security.Authentication;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Base class for client tests.
    /// </summary>
    public class ClientTestBase
    {
        /** Cache name. */
        protected const string CacheName = "cache";

        /** */
        protected const string RequestNamePrefixCache = "cache.ClientCache";

        /** */
        protected const string RequestNamePrefixBinary = "binary.ClientBinary";

        /** Grid count. */
        private readonly int _gridCount = 1;

        /** SSL. */
        private readonly bool _enableSsl;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientTestBase"/> class.
        /// </summary>
        public ClientTestBase()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientTestBase"/> class.
        /// </summary>
        public ClientTestBase(int gridCount, bool enableSsl = false)
        {
            _gridCount = gridCount;
            _enableSsl = enableSsl;
        }

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void FixtureSetUp()
        {
            var cfg = GetIgniteConfiguration();
            Ignition.Start(cfg);

            for (var i = 1; i < _gridCount; i++)
            {
                cfg = GetIgniteConfiguration();
                cfg.IgniteInstanceName = i.ToString();

                Ignition.Start(cfg);
            }

            Client = GetClient();
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
            Client.Dispose();
        }

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public virtual void TestSetUp()
        {
            var cache = GetCache<int>();
            cache.RemoveAll();
            cache.Clear();

            Assert.AreEqual(0, cache.GetSize(CachePeekMode.All));
            Assert.AreEqual(0, GetClientCache<int>().GetSize(CachePeekMode.All));

            ClearLoggers();
        }

        /// <summary>
        /// Gets the client.
        /// </summary>
        public IIgniteClient Client { get; set; }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        protected static ICache<int, T> GetCache<T>()
        {
            return Ignition.GetAll().First().GetOrCreateCache<int, T>(CacheName);
        }

        /// <summary>
        /// Gets the client cache.
        /// </summary>
        protected ICacheClient<int, T> GetClientCache<T>()
        {
            return GetClientCache<int, T>();
        }

        /// <summary>
        /// Gets the client cache.
        /// </summary>
        protected virtual ICacheClient<TK, TV> GetClientCache<TK, TV>(string cacheName = CacheName)
        {
            return Client.GetCache<TK, TV>(cacheName ?? CacheName);
        }

        /// <summary>
        /// Gets the client.
        /// </summary>
        protected IIgniteClient GetClient()
        {
            return Ignition.StartClient(GetClientConfiguration());
        }

        /// <summary>
        /// Gets the client configuration.
        /// </summary>
        protected virtual IgniteClientConfiguration GetClientConfiguration()
        {
            var port = _enableSsl ? 11110 : IgniteClientConfiguration.DefaultPort;
            
            return new IgniteClientConfiguration
            {
                Endpoints = new List<string> {IPAddress.Loopback + ":" + port},
                SocketTimeout = TimeSpan.FromSeconds(15),
                Logger = new ListLogger(new ConsoleLogger {MinLevel = LogLevel.Trace}),
                SslStreamFactory = _enableSsl
                    ? new SslStreamFactory
                    {
                        CertificatePath = Path.Combine("Config", "Client", "thin-client-cert.pfx"),
                        CertificatePassword = "123456",
                        SkipServerCertificateValidation = true,
                        CheckCertificateRevocation = true,
#if !NETCOREAPP
                        SslProtocols = SslProtocols.Tls
#else
                        SslProtocols = SslProtocols.Tls12
#endif
                    }
                    : null
            };
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        protected virtual IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                Logger = new ListLogger(new ConsoleLogger()),
                SpringConfigUrl = _enableSsl ? Path.Combine("Config", "Client", "server-with-ssl.xml") : null
            };
        }

        /// <summary>
        /// Converts object to binary form.
        /// </summary>
        private IBinaryObject ToBinary(object o)
        {
            return Client.GetBinary().ToBinary<IBinaryObject>(o);
        }

        /// <summary>
        /// Gets the binary cache.
        /// </summary>
        protected ICacheClient<int, IBinaryObject> GetBinaryCache()
        {
            return Client.GetCache<int, Person>(CacheName).WithKeepBinary<int, IBinaryObject>();
        }

        /// <summary>
        /// Gets the binary key cache.
        /// </summary>
        protected ICacheClient<IBinaryObject, int> GetBinaryKeyCache()
        {
            return Client.GetCache<Person, int>(CacheName).WithKeepBinary<IBinaryObject, int>();
        }

        /// <summary>
        /// Gets the binary key-val cache.
        /// </summary>
        protected ICacheClient<IBinaryObject, IBinaryObject> GetBinaryKeyValCache()
        {
            return Client.GetCache<Person, Person>(CacheName).WithKeepBinary<IBinaryObject, IBinaryObject>();
        }

        /// <summary>
        /// Gets the binary person.
        /// </summary>
        protected IBinaryObject GetBinaryPerson(int id)
        {
            return ToBinary(new Person(id) { DateTime = DateTime.MinValue.ToUniversalTime() });
        }

        /// <summary>
        /// Gets the logs.
        /// </summary>
        protected static List<ListLogger.Entry> GetLogs(IIgniteClient client)
        {
            var igniteClient = (IgniteClient) client;
            var logger = igniteClient.GetConfiguration().Logger;
            var listLogger = (ListLogger) logger;
            return listLogger.Entries;
        }
        
        /// <summary>
        /// Gets client request names for a given server node.
        /// </summary>
        protected static IEnumerable<string> GetServerRequestNames(int serverIndex = 0, string prefix = null)
        {
            var instanceName = serverIndex == 0 ? null : serverIndex.ToString();
            var grid = Ignition.GetIgnite(instanceName);
            var logger = (ListLogger) grid.Logger;
         
            return GetServerRequestNames(logger, prefix);
        }

        /// <summary>
        /// Gets client request names from a given logger.
        /// </summary>
        protected static IEnumerable<string> GetServerRequestNames(ListLogger logger, string prefix = null)
        {
            // Full request class name examples:
            // org.apache.ignite.internal.processors.platform.client.binary.ClientBinaryTypeGetRequest
            // org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetRequest
            var messageRegex = new Regex(
                @"Client request received \[reqId=\d+, addr=/127.0.0.1:\d+, " +
                @"req=org\.apache\.ignite\.internal\.processors\.platform\.client\..*?" +
                prefix +
                @"(\w+)Request@");

            return logger.Entries
                .Select(m => messageRegex.Match(m.Message))
                .Where(m => m.Success)
                .Select(m => m.Groups[1].Value);
        }

        /// <summary>
        /// Gets client request names from all server nodes.
        /// </summary>
        protected static IEnumerable<string> GetAllServerRequestNames(string prefix = null)
        {
            return GetLoggers().SelectMany(l => GetServerRequestNames(l, prefix));
        }

        /// <summary>
        /// Gets loggers from all server nodes.
        /// </summary>
        protected static IEnumerable<ListLogger> GetLoggers()
        {
            return Ignition.GetAll()
                .OrderBy(i => i.Name)
                .Select(i => i.Logger)
                .Cast<ListLogger>();
        }

        /// <summary>
        /// Clears loggers of all server nodes.
        /// </summary>
        protected static void ClearLoggers()
        {
            foreach (var logger in GetLoggers())
            {
                logger.Clear();
            }
        }

        /// <summary>
        /// Asserts the client configs are equal.
        /// </summary>
        public static void AssertClientConfigsAreEqual(CacheClientConfiguration cfg, CacheClientConfiguration cfg2)
        {
            if (cfg2.QueryEntities != null)
            {
                // Remove identical aliases which are added during config roundtrip.
                foreach (var e in cfg2.QueryEntities)
                {
                    e.Aliases = e.Aliases.Where(x => x.Alias != x.FullName).ToArray();
                }
            }

            HashSet<string> ignoredProps = null;

            if (cfg.ExpiryPolicyFactory != null && cfg2.ExpiryPolicyFactory != null)
            {
                ignoredProps = new HashSet<string> {"ExpiryPolicyFactory"};

                AssertExtensions.ReflectionEqual(cfg.ExpiryPolicyFactory.CreateInstance(),
                    cfg2.ExpiryPolicyFactory.CreateInstance());
            }

            AssertExtensions.ReflectionEqual(cfg, cfg2, ignoredProperties : ignoredProps);
        }
    }
}
