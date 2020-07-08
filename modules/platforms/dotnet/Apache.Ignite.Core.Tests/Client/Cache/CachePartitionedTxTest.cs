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
    using System.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests client transactions for multiple nodes with partition awareness.
    /// </summary>
    public class CachePartitionedTxTest : CacheClientAbstractTxTest
    {
        /// <summary>
        ///  Initializes a new instance of the <see cref="CachePartitionedTxTest"/> class.
        /// </summary>
        public CachePartitionedTxTest() : base(3, true)
        {
            // No-op.
        }

        /// <summary>
        /// Test transaction for partition aware client. 
        /// </summary>
        [Test]
        public void TestTxPartitioned()
        {
            var cache = TransactionalCache();
            var ignite1 = GetIgnite();
            var ignite2 = GetIgnite(1);
            var key1 = TestUtils.GetPrimaryKey(ignite1, GetCacheName());
            var key2 = TestUtils.GetPrimaryKey(ignite2, GetCacheName());

            cache.Put(key1, 1);
            cache.Put(key2, 2);

            using (Client.GetTransactions().TxStart())
            {
                cache.Put(key1, 10);
                cache.Put(key2, 20);

                Assert.AreEqual(10, cache.Get(key1));
                Assert.AreEqual(20, cache.Get(key2));
            }

            Assert.AreEqual(1, cache.Get(key1));
            Assert.AreEqual(2, cache.Get(key2));
        }
        
        /// <summary>
        /// Test transaction scope for partition aware client. 
        /// </summary>
        [Test]
        public void TestTransactionScopePartitioned()
        {
            var cache = TransactionalCache();
            var ignite1 = GetIgnite();
            var ignite2 = GetIgnite(1);
            var key1 = TestUtils.GetPrimaryKey(ignite1, GetCacheName());
            var key2 = TestUtils.GetPrimaryKey(ignite2, GetCacheName());

            cache.Put(key1, 1);
            cache.Put(key2, 2);

            using (new TransactionScope())
            {
                cache.Put(key1, 10);
                cache.Put(key2, 20);

                Assert.AreEqual(10, cache.Get(key1));
                Assert.AreEqual(20, cache.Get(key2));
            }

            Assert.AreEqual(1, cache.Get(key1));
            Assert.AreEqual(2, cache.Get(key2));
        }

        protected override string GetCacheName()
        {
            return "partitioned_" + base.GetCacheName();
        }
    }
}
