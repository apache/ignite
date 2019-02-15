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
