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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    [Category(TestUtils.CategoryIntensive)]
    public class CacheTimeoutOnPmeTransactionalTest : CacheAbstractTransactionalTest
    {
        /// <summary>
        /// Tests that setting transaction PME timeout works and changes are propagated to Transactions.
        /// </summary>
        [Test]
        public void TestSettingPartitionMapExchangeTimeout()
        {
            IIgnite ignite = GetIgnite(0);
            
            ignite.GetCluster().SetTxTimeoutOnPartitionMapExchange(TimeSpan.FromSeconds(12));

            Assert.AreEqual(ignite.GetTransactions().DefaultTimeoutOnPartitionMapExchange, TimeSpan.FromSeconds(12));
        }
        
        /// <summary>
        /// Tests local active transactions management.
        /// </summary>
        [Test]
        public void TestLocalActiveTransactions()
        {
            IIgnite ignite = GetIgnite(0);

            using (var tx = ignite.GetTransactions().TxStart(TransactionConcurrency.Optimistic, 
                TransactionIsolation.ReadCommitted, TimeSpan.FromSeconds(20), 1))
            using (var activeTxCollection = ignite.GetTransactions().GetLocalActiveTransactions())
            {
                Assert.IsNotEmpty(activeTxCollection);
                
                var testTx = activeTxCollection.ElementAt(0);
                
                Assert.AreEqual(testTx.Concurrency, tx.Concurrency);

                Assert.AreEqual(testTx.Isolation, tx.Isolation);

                Assert.AreEqual(testTx.Timeout, tx.Timeout);
            
                tx.Commit();
                 
                Assert.AreEqual(testTx.State, TransactionState.Committed);
                
            }
            
            using (var tx = ignite.GetTransactions().TxStart(TransactionConcurrency.Optimistic, 
                TransactionIsolation.ReadCommitted, TimeSpan.FromSeconds(20), 1))
            using (var activeTxCollection = ignite.GetTransactions().GetLocalActiveTransactions())
            {
                Assert.IsNotEmpty(activeTxCollection);
                
                var testTx = activeTxCollection.ElementAt(0);

                testTx.Rollback();

                Assert.AreEqual(tx.State, TransactionState.RolledBack);
            }
        }
        
        protected override ITransactions Transactions
        {
            get { return GetIgnite(0).GetTransactions().WithLabel("test-tx"); }
        }
        
        protected override int GridCount()
        {
            return 3;
        }

        protected override string CacheName()
        {
            return "partitioned";
        }

        protected override bool NearEnabled()
        {
            return false;
        }

        protected override int Backups()
        {
            return 1;
        }
    }
}
