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

/**
 * @file
 * Declares ignite::transactions::Transactions class.
 */

#ifndef _IGNITE_TRANSACTIONS
#define _IGNITE_TRANSACTIONS

#include <ignite/common/concurrent.h>
#include <ignite/common/java.h>

#include "ignite/transactions/transaction.h"
#include "ignite/impl/transactions/transactions_impl.h"

namespace ignite
{
    namespace transactions
    {
        /**
         * Transactions.
         */
        class IGNITE_FRIEND_EXPORT Transactions
        {
        public:
            /**
             * Constructor.
             */
            Transactions(ignite::common::concurrent::SharedPointer<impl::transactions::TransactionsImpl> impl) :
                impl(impl)
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            Transactions(const Transactions& other) :
                impl(other.impl)
            {
                // No-op.
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return This.
             */
            Transactions& operator=(const Transactions& other)
            {
                impl = other.impl;

                return *this;
            }

            /**
             * Destructor.
             */
            ~Transactions()
            {
                // No-op.
            }

            /**
             * Get active transaction for the current thread.
             *
             * @return Active transaction for current thread.
             * Returned instance is not valid if there is no active transaction
             * for the thread.
             */
            Transaction GetTx()
            {
                using impl::transactions::TransactionImpl;

                return Transaction(TransactionImpl::GetCurrent());
            }

            /**
             * Start new transaction.
             *
             * @return New transaction instance.
             */
            Transaction TxStart()
            {
                IgniteError err;

                Transaction tx = TxStart(err);

                IgniteError::ThrowIfNeeded(err);

                return tx;
            }

            /**
             * Start new transaction.
             *
             * @param err Error.
             * @return New transaction instance.
             */
            Transaction TxStart(IgniteError& err)
            {
                return TxStart(IGNITE_TX_CONCURRENCY_OPTIMISTIC,
                    IGNITE_TX_ISOLATION_READ_COMMITTED, 0, 0, err);
            }

            /**
             * Start new transaction.
             *
             * @param concurrency Concurrency.
             * @param isolation Isolation.
             * @return New transaction instance.
             */
            Transaction TxStart(TransactionConcurrency concurrency,
                TransactionIsolation isolation)
            {
                IgniteError err;

                Transaction tx = TxStart(concurrency, isolation, err);

                IgniteError::ThrowIfNeeded(err);

                return tx;
            }

            /**
             * Start new transaction.
             *
             * @param concurrency Concurrency.
             * @param isolation Isolation.
             * @param err Error.
             * @return New transaction instance.
             */
            Transaction TxStart(TransactionConcurrency concurrency,
                TransactionIsolation isolation, IgniteError& err)
            {
                return TxStart(concurrency, isolation, 0, 0, err);
            }

            /**
             * Start new transaction.
             *
             * @param concurrency Concurrency.
             * @param isolation Isolation.
             * @param timeout Timeout.
             * @param txSize Number of entries participating in transaction (may be approximate).
             * @return New transaction instance.
             */
            Transaction TxStart(TransactionConcurrency concurrency,
                TransactionIsolation isolation, int64_t timeout,
                int32_t txSize)
            {
                IgniteError err;

                Transaction tx = TxStart(concurrency, isolation, timeout, txSize, err);

                IgniteError::ThrowIfNeeded(err);

                return tx;
            }

            /**
             * Start new transaction.
             *
             * @param concurrency Concurrency.
             * @param isolation Isolation.
             * @param timeout Timeout.
             * @param txSize Number of entries participating in transaction (may be approximate).
             * @param err Error.
             * @return New transaction instance.
             */
            Transaction TxStart(TransactionConcurrency concurrency,
                TransactionIsolation isolation, int64_t timeout,
                int32_t txSize, IgniteError& err)
            {
                using impl::transactions::TransactionImpl;
                using ignite::common::concurrent::SharedPointer;

                SharedPointer<TransactionImpl> tx = TransactionImpl::Create(impl,
                    concurrency, isolation, timeout, txSize, err);

                return Transaction(tx);
            }

        private:
            /** Implementation delegate. */
            ignite::common::concurrent::SharedPointer<impl::transactions::TransactionsImpl> impl;
        };
    }
}

#endif