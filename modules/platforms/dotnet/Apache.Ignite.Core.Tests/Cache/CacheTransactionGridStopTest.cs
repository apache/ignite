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
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests that stopping grid does not prevent <see cref="ITransaction"/> from ending without exception.
    /// </summary>
    public class CacheTransactionGridStopTest : TestBase
    {
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

            var tx = ignite.GetTransactions().TxStart();
            var transactions = ignite.GetTransactions().GetLocalActiveTransactions();

            Ignition.Stop(ignite.Name, true);

            Assert.DoesNotThrow(() => tx.Dispose());
            Assert.DoesNotThrow(() => transactions.Dispose());
        }
    }
}
