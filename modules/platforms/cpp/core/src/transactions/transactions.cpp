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

#include "ignite/transactions/transactions.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl::transactions;

namespace ignite
{
    namespace transactions
    {
        Transactions::Transactions(SharedPointer<TransactionsImpl> impl) :
            impl(impl)
        {
            // No-op.
        }

        Transactions::Transactions(const Transactions & other) :
            impl(other.impl)
        {
            // No-op.
        }

        Transactions& Transactions::operator=(const Transactions& other)
        {
            impl = other.impl;

            return *this;
        }

        Transactions::~Transactions()
        {
            // No-op.
        }

        Transaction Transactions::GetTx()
        {
            return Transaction(TransactionImpl::GetCurrent());
        }

        Transaction Transactions::TxStart()
        {
            IgniteError err;

            Transaction tx = TxStart(err);

            IgniteError::ThrowIfNeeded(err);

            return tx;
        }

        Transaction Transactions::TxStart(IgniteError& err)
        {
            return TxStart(TransactionConcurrency::PESSIMISTIC,
                TransactionIsolation::READ_COMMITTED, 0, 0, err);
        }

        Transaction Transactions::TxStart(TransactionConcurrency::Type concurrency,
            TransactionIsolation::Type isolation)
        {
            IgniteError err;

            Transaction tx = TxStart(concurrency, isolation, err);

            IgniteError::ThrowIfNeeded(err);

            return tx;
        }

        Transaction Transactions::TxStart(TransactionConcurrency::Type concurrency,
            TransactionIsolation::Type isolation, IgniteError& err)
        {
            return TxStart(concurrency, isolation, 0, 0, err);
        }

        Transaction Transactions::TxStart(TransactionConcurrency::Type concurrency,
            TransactionIsolation::Type isolation, int64_t timeout, int32_t txSize)
        {
            IgniteError err;

            Transaction tx = TxStart(concurrency,
                isolation, timeout, txSize, err);

            IgniteError::ThrowIfNeeded(err);

            return tx;
        }

        Transaction Transactions::TxStart(TransactionConcurrency::Type concurrency,
            TransactionIsolation::Type isolation, int64_t timeout, int32_t txSize,
            IgniteError& err)
        {
            err = IgniteError();

            SharedPointer<TransactionImpl> tx = TransactionImpl::Create(impl,
                concurrency, isolation, timeout, txSize, err);

            return Transaction(tx);
        }

        TransactionMetrics Transactions::GetMetrics()
        {
            IgniteError err;

            TransactionMetrics metrics = GetMetrics(err);

            IgniteError::ThrowIfNeeded(err);

            return metrics;
        }

        TransactionMetrics Transactions::GetMetrics(IgniteError& err)
        {
            TransactionsImpl* txImpl = impl.Get();

            if (txImpl)
                return txImpl->GetMetrics(err);
            else
            {
                err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                    "Instance is not usable (did you check for error?).");
            }

            return TransactionMetrics();
        }
    }
}
