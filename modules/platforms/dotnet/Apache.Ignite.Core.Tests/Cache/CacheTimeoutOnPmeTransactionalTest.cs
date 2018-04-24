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
