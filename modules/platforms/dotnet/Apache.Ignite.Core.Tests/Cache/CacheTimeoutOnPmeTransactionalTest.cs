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
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests setting transactions timeout on partition map exchange.
    /// </summary>
    public class CacheTimeoutOnPmeTransactionalTest
    {
        /** */
        private static readonly TimeSpan TxPartitionMapExchangeTimeout = TimeSpan.FromSeconds(5);
        
        /** */
        private const string DefaultCacheName = "default";
        
        /** */
        private const string TxLabel = "tx-label";

        /** */
        private IIgnite _grid;

        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void StartGrids()
        {
            IgniteConfiguration cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                TransactionConfiguration = new TransactionConfiguration()
                {
                    DefaultTimeoutOnPartitionMapExchange = TxPartitionMapExchangeTimeout
                },
                CacheConfiguration = new List<CacheConfiguration>()
                {
                    new CacheConfiguration(new CacheConfiguration(DefaultCacheName))
                }
            };
            
            _grid = Ignition.Start(cfg);
        }
         
        /// <summary>
        /// Fixture teardown.
        /// </summary>
        [TestFixtureTearDown]
        public void StopGrids()
        {
            Ignition.StopAll(true);
        }
 
        /// <summary>
        /// Tests that setting transaction PME timeout works and changes are propagated to Transactions,
        /// also tests that passing timeout from <see cref="IgniteConfiguration"/> also works.
        /// </summary>
        [Test]
        public void TestSettingPartitionMapExchangeTimeout()
        {
            var txTimeoutFromConfig =
                _grid.GetConfiguration().TransactionConfiguration.DefaultTimeoutOnPartitionMapExchange;
            
            Assert.AreEqual(TxPartitionMapExchangeTimeout, txTimeoutFromConfig);
            
            _grid.GetCluster().SetTxTimeoutOnPartitionMapExchange(TimeSpan.FromSeconds(12));

            Assert.AreEqual(_grid.GetTransactions().DefaultTimeoutOnPartitionMapExchange, TimeSpan.FromSeconds(12));
        }
        
        /// <summary>
        /// Tests local active transactions management.
        /// </summary>
        [Test]
        public void TestLocalActiveTransactions()
        {
            using (var tx = _grid.GetTransactions().WithLabel(TxLabel).TxStart(TransactionConcurrency.Optimistic,
                TransactionIsolation.ReadCommitted, TimeSpan.FromSeconds(20), 1))
            using (var activeTxCollection = _grid.GetTransactions().GetLocalActiveTransactions())
            {
                Assert.IsNotEmpty(activeTxCollection);
                
                var testTx = activeTxCollection.ElementAt(0);
                
                Assert.AreEqual(testTx.Concurrency, tx.Concurrency);

                Assert.AreEqual(testTx.Isolation, tx.Isolation);

                Assert.AreEqual(testTx.Timeout, tx.Timeout);
                
                Assert.AreEqual(testTx.Label, tx.Label);
                
                Assert.AreEqual(testTx.Label, TxLabel);
            
                tx.Commit();
                 
                Assert.AreEqual(testTx.State, TransactionState.Committed);    
            }
            
            using (var tx = _grid.GetTransactions().TxStart(TransactionConcurrency.Optimistic, 
                TransactionIsolation.ReadCommitted, TimeSpan.FromSeconds(20), 1))
            using (var activeTxCollection = _grid.GetTransactions().GetLocalActiveTransactions())
            {
                Assert.IsNotEmpty(activeTxCollection);
                
                var testTx = activeTxCollection.ElementAt(0);

                testTx.Rollback();
                
                Assert.AreEqual(tx.Label, null);
                
                Assert.AreEqual(testTx.Label, null);

                Assert.AreEqual(tx.State, TransactionState.RolledBack);
            }
        }
    }
}
