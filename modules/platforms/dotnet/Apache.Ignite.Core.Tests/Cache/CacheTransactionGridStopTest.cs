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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests that stopping grid does not prevent <see cref="ITransaction"/> from ending without exception.
    /// </summary>
    public class CacheTransactionGridStopTest : TestBase
    {
        /// <summary>
        /// Test that finalization does not throw.
        /// </summary>
        [Test]
        public void TestFinalizeDoesNotThrow()
        {
            Assert.DoesNotThrow(() =>
            {
                var ignite = Ignition.Start(new IgniteConfiguration(GetConfig())
                {
                    IgniteInstanceName = TestUtils.TestName
                });
                ignite.GetTransactions().TxStart();
                Ignition.Stop(ignite.Name, true);
                Task.Factory.StartNew(() =>
                    {
                        for (int i = 0; i < 1; i++)
                        {
                            var igniteTr = Ignition.Start(new IgniteConfiguration(GetConfig())
                            {
                                IgniteInstanceName = TestUtils.TestName + i
                            });
                            igniteTr.GetTransactions().TxStart();
                            Ignition.Stop(ignite.Name, true);
                        }
                    })
                    .Wait();

                var collectionCount = GC.CollectionCount(2);
                GC.Collect(GC.MaxGeneration);
                GC.WaitForPendingFinalizers();
                var collectionCount1 = GC.CollectionCount(2);
            });
        }

        /// <summary>
        /// Test that dispose does not throw.
        /// </summary>
        [Test]
        public void TestDisposeDoesNotThrow()
        {
            var ignite = Ignition.Start(new IgniteConfiguration(GetConfig())
            {
                IgniteInstanceName = TestUtils.TestName
            });

            Assert.DoesNotThrow(() =>
            {
                using (ignite.GetTransactions().TxStart())
                {
                    Ignition.Stop(ignite.Name, true);
                }
            });
        }
    }
}
