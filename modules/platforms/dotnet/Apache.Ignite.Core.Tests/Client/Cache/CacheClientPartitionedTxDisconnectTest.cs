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
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using NUnit.Framework;
    using NUnit.Framework.Constraints;

    /// <summary>
    /// Tests client transactions for multiple nodes with partition awareness.
    /// </summary>
    public class CacheClientPartitionedTxDisconnectTest : ClientTestBase
    {
        /// <summary>
        ///  Initializes a new instance of the <see cref="CacheClientPartitionedTxDisconnectTest"/> class.
        /// </summary>
        public CacheClientPartitionedTxDisconnectTest() : base(3, enablePartitionAwareness: true)
        {
            // No-op.
        }

        /// <summary>
        /// Tests that transaction handles reconnect.
        /// </summary>
        [Test]
        public void TestDisconnect()
        {
            var cache = GetTransactionalCache();

            var constraint = new ReusableConstraint(Is.TypeOf<IgniteClientException>()
                .And.Message.EqualTo("Transaction context has been lost due to connection errors."));
            try
            {
                using (Client.GetTransactions().TxStart())
                {
                    var igniteToStop = new[] {(int?) null, 1, 2}
                        .Select(i => GetIgnite(i))
                        .FirstOrDefault(ign => ign.GetTransactions().GetLocalActiveTransactions().Any());

                    Assert.IsNotNull(igniteToStop);
                    Ignition.Stop(igniteToStop.Name, true);

                    Assert.Catch(() => cache.Put(1, 1));
                    Assert.Throws(constraint, () => cache.Put(1, 1));
                }
            }
            catch (IgniteClientException ex)
            {
                Assert.That(ex, constraint);
            }

            Assert.DoesNotThrow(() => cache.Put(1, 1));
            Assert.IsNull(Client.GetTransactions().Tx);
        }

        /// <summary>
        /// Gets or creates transactional cache
        /// </summary>
        private ICacheClient<int, int> GetTransactionalCache()
        {
            return Client.GetOrCreateCache<int, int>(new CacheClientConfiguration
            {
                Name = TestUtils.TestName,
                AtomicityMode = CacheAtomicityMode.Transactional
            });
        }
    }
}
