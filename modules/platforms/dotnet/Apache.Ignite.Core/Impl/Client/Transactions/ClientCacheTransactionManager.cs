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

namespace Apache.Ignite.Core.Impl.Client.Transactions
{
    using System.Threading;
    using System.Transactions;
    using Apache.Ignite.Core.Client.Transactions;

    /// <summary>
    /// Cache transaction enlistment manager, 
    /// allows using Ignite transactions via standard <see cref="TransactionScope"/>.
    /// </summary>
    public class ClientCacheTransactionManager : IEnlistmentNotification
    {
        /** */
        private readonly IClientTransactions _transactions;

        /** */
        private readonly ThreadLocal<Enlistment> _enlistment = new ThreadLocal<Enlistment>();

        /// <summary>
        /// Initializes a new instance of <see cref="ClientCacheTransactionManager"/> class.
        /// </summary>
        /// <param name="transactions">Transactions.</param>
        public ClientCacheTransactionManager(IClientTransactions transactions)
        {
            _transactions = transactions;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            throw new System.NotImplementedException();
        }

        public void Commit(Enlistment enlistment)
        {
            throw new System.NotImplementedException();
        }

        public void Rollback(Enlistment enlistment)
        {
            throw new System.NotImplementedException();
        }

        public void InDoubt(Enlistment enlistment)
        {
            throw new System.NotImplementedException();
        }
    }
}