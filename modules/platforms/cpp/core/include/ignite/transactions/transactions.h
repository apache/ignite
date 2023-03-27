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

#ifndef _IGNITE_TRANSACTIONS_TRANSACTIONS
#define _IGNITE_TRANSACTIONS_TRANSACTIONS

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>

#include "ignite/transactions/transaction.h"
#include "ignite/transactions/transaction_metrics.h"
#include "ignite/impl/transactions/transactions_impl.h"

namespace ignite
{
    namespace transactions
    {
        /**
         * %Transactions facade.
         *
         * This class is implemented as a reference to an implementation so copying
         * of this class instance will only create another reference to the same
         * underlying object. Underlying object will be released automatically once all
         * the instances are destructed.
         */
        class IGNITE_FRIEND_EXPORT Transactions
        {
        public:
            /**
             * Constructor.
             *
             * Internal method. Should not be used by user.
             *
             * @param impl Implementation.
             */
            Transactions(ignite::common::concurrent::SharedPointer<impl::transactions::TransactionsImpl> impl);

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            Transactions(const Transactions& other);

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return This.
             */
            Transactions& operator=(const Transactions& other);

            /**
             * Destructor.
             */
            ~Transactions();

            /**
             * Get active transaction for the current thread.
             *
             * @return Active transaction for current thread.
             * Returned instance is not valid if there is no active transaction
             * for the thread.
             */
            Transaction GetTx();

            /**
             * Start new transaction with default isolation, concurrency
             * and timeout.
             *
             * @return New transaction instance.
             */
            Transaction TxStart();

            /**
             * Start new transaction with default isolation, concurrency
             * and timeout.
             *
             * @param err Error.
             * @return New transaction instance.
             */
            Transaction TxStart(IgniteError& err);

            /**
             * Starts new transaction with the specified concurrency and
             * isolation.
             *
             * @param concurrency Concurrency.
             * @param isolation Isolation.
             * @return New transaction instance.
             */
            Transaction TxStart(TransactionConcurrency::Type concurrency,
                TransactionIsolation::Type isolation);

            /**
             * Starts new transaction with the specified concurrency and
             * isolation.
             *
             * @param concurrency Concurrency.
             * @param isolation Isolation.
             * @param err Error.
             * @return New transaction instance.
             */
            Transaction TxStart(TransactionConcurrency::Type concurrency,
                TransactionIsolation::Type isolation, IgniteError& err);

            /**
             * Starts transaction with specified isolation, concurrency,
             * timeout, and number of participating entries.
             *
             * @param concurrency Concurrency.
             * @param isolation Isolation.
             * @param timeout Timeout. Zero if for infinite timeout.
             * @param txSize Number of entries participating in transaction
             *     (may be approximate).
             * @return New transaction instance.
             */
            Transaction TxStart(TransactionConcurrency::Type concurrency,
                TransactionIsolation::Type isolation, int64_t timeout,
                int32_t txSize);

            /**
             * Start new transaction.
             *
             * @param concurrency Concurrency.
             * @param isolation Isolation.
             * @param timeout Timeout. Zero if for infinite timeout.
             * @param txSize Number of entries participating in transaction
             *     (may be approximate).
             * @param err Error.
             * @return New transaction instance.
             */
            Transaction TxStart(TransactionConcurrency::Type concurrency,
                TransactionIsolation::Type isolation, int64_t timeout,
                int32_t txSize, IgniteError& err);

            /**
             * Get transaction metrics.
             *
             * @return Metrics instance.
             */
            TransactionMetrics GetMetrics();

            /**
             * Get transaction metrics.
             *
             * @param err Error.
             * @return Metrics instance.
             * Returned instance is not valid if an error occurred during
             * the operation.
             */
            TransactionMetrics GetMetrics(IgniteError& err);

        private:
            /** Implementation delegate. */
            ignite::common::concurrent::SharedPointer<impl::transactions::TransactionsImpl> impl;
        };
    }
}

#endif //_IGNITE_TRANSACTIONS_TRANSACTIONS