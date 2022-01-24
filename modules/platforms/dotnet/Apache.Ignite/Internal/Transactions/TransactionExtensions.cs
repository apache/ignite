/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Transactions
{
    using System.Transactions;
    using Ignite.Transactions;

    /// <summary>
    /// Transaction extension methods.
    /// </summary>
    internal static class TransactionExtensions
    {
        /// <summary>
        /// Gets transaction as internal <see cref="Transaction"/> class.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Internal transaction.</returns>
        /// <exception cref="TransactionException">When provided transaction is not supported.</exception>
        public static Transaction? ToInternal(this ITransaction? tx)
        {
            if (tx == null)
            {
                return null;
            }

            if (tx is Transaction t)
            {
                return t;
            }

            throw new TransactionException("Unsupported transaction implementation: " + tx.GetType());
        }
    }
}
