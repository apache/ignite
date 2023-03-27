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

#include "ignite/transactions/transactions.h"
#include "ignite/transactions/transaction.h"

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
