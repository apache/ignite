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
    using System.Net;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Base class for client tests.
    /// </summary>
    public class ClientTestBase
    {
        /** Cache name. */
        protected const string CacheName = "cache";

        /** Grid count. */
        private readonly int _gridCount = 1;

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
        public ClientTestBase(int gridCount)
        {
            _gridCount = gridCount;
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = GetIgniteConfiguration();
            Ignition.Start(cfg);

            cfg.AutoGenerateIgniteInstanceName = true;

            for (var i = 1; i < _gridCount; i++)
            {
                Ignition.Start(cfg);
            }
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void TestSetUp()
        {
            GetCache<int>().RemoveAll();
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        protected static ICache<int, T> GetCache<T>()
        {
            return Ignition.GetIgnite().GetOrCreateCache<int, T>(CacheName);
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
        protected IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration
            {
                Host = IPAddress.Loopback.ToString()
            };
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        protected virtual IgniteConfiguration GetIgniteConfiguration()
        {
            return TestUtils.GetTestConfiguration();
        }
    }
}
