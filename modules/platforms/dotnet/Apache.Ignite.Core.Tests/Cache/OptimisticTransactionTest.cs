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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System.Threading.Tasks;
    using System.Transactions;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="TransactionConcurrency.Optimistic"/> mode.
    /// </summary>
    public class OptimisticTransactionTest : TestBase
    {
        /// <summary>
        /// Tests explicit optimistic transactions.
        /// </summary>
        [Test]
        public void TestExplicitOptimisticTransactionThrowsExceptionOnConflict()
        {
            // TODO
            var cache = Ignite.GetOrCreateCache<int, int>(TestUtils.TestName);

            cache[1] = 1;

            using (var tx = Ignite.GetTransactions().TxStart())
            {
                Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
                Assert.AreEqual(TransactionIsolation.Serializable, tx.Isolation);

                cache[1] = 2;

                Task.Factory.StartNew(() =>
                {
                    Assert.IsNull(Ignite.GetTransactions().Tx);
                    return cache[1] = 3;
                }).Wait();

                tx.Commit();
            }

            Assert.AreEqual(2, cache[1]);
        }

        /// <summary>
        /// Tests ambient optimistic transactions (with <see cref="TransactionScope"/>.
        /// </summary>
        [Test]
        public void TestAmbientOptimisticTransactionThrowsExceptionOnConflict()
        {
            // TODO
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetConfig()
        {
            return new IgniteConfiguration(base.GetConfig())
            {
                TransactionConfiguration = new TransactionConfiguration
                {
                    DefaultTransactionConcurrency = TransactionConcurrency.Optimistic,
                    DefaultTransactionIsolation = TransactionIsolation.Serializable
                }
            };
        }
    }
}
