/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Linq;
    using System.Net;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Tests.Client.Cache;
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

            Client = GetClient();
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
        public virtual void TestSetUp()
        {
            var cache = GetCache<int>();
            cache.RemoveAll();
            cache.Clear();

            Assert.AreEqual(0, cache.GetSize(CachePeekMode.All));
            Assert.AreEqual(0, GetClientCache<int>().GetSize(CachePeekMode.All));
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
            return Ignition.GetIgnite().GetOrCreateCache<int, T>(CacheName);
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
            return new IgniteClientConfiguration
            {
                Endpoints = new[] {IPAddress.Loopback.ToString()}
            };
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        protected virtual IgniteConfiguration GetIgniteConfiguration()
        {
            return TestUtils.GetTestConfiguration();
        }

        /// <summary>
        /// Converts object to binary form.
        /// </summary>
        protected IBinaryObject ToBinary(object o)
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

            AssertExtensions.ReflectionEqual(cfg, cfg2);
        }
    }
}
