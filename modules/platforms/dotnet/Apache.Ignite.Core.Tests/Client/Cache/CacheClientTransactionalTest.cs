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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Transactional cache client tests.
    /// </summary>
    public class CacheClientTransactionalTest : ClientTestBase
    {
        /** */
        private const int ServerCount = 3;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClientTransactionalTest" /> class.
        /// </summary>
        public CacheClientTransactionalTest() : base(ServerCount)
        {
            // No-op.
        }

        /// <summary>
        /// Tests that commit applies cache changes.
        /// </summary>
        [Test]
        public void TestTxCommit([Values(true /*, false*/)]
            bool async)
        {
            var cache = TransactionalCache();

            cache.Put(1, 1);

            using (var tx = Client.Transactions.TxStart())
            {
                cache.Put(1, 10);

                tx.Commit();
            }

            Assert.AreEqual(10, cache.Get(1));
        }

        /// <summary>
        /// Tests that rollback reverts cache changes.
        /// </summary>
        [Test]
        public void TestTxRollback([Values(true /*, false*/)]
            bool async)
        {
            var cache = TransactionalCache();

            cache.Put(1, 1);

            using (var tx = Client.Transactions.TxStart())
            {
                cache.Put(1, 20);

                tx.Rollback();
            }

            Assert.AreEqual(1, cache.Get(1));
        }

        /// <summary>
        /// Tests that rollback reverts cache changes.
        /// </summary>
        [Test]
        public void TestTxClose()
        {
            var cache = TransactionalCache();

            cache.Put(1, 1);

            using (var tx = Client.Transactions.TxStart())
            {
                cache.Put(1, 30);
            }

            Assert.AreEqual(1, cache.Get(1));
        }

        /// <summary>
        /// Gets or creates transactional cache
        /// </summary>
        private ICacheClient<int, int> TransactionalCache(string name = "client_transactional")
        {
            return Client.GetOrCreateCache<int, int>(new CacheClientConfiguration
            {
                Name = name,
                AtomicityMode = CacheAtomicityMode.Transactional
            });
        }
    }
}