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
 * Declares ignite::transactions::Transaction class.
 */

#ifndef _IGNITE_TRANSACTIONS_TRANSACTION
#define _IGNITE_TRANSACTIONS_TRANSACTION

#include <ignite/common/concurrent.h>
#include <ignite/common/java.h>

#include "ignite/impl/transactions/transaction_impl.h"

namespace ignite
{
    namespace transactions
    {
        /**
         * Transaction concurrency control.
         */
        enum TransactionConcurrency
        {
            /** Optimistic concurrency control. */
            IGNITE_TX_CONCURRENCY_OPTIMISTIC = 0,

            /** Pessimistic concurrency control. */
            IGNITE_TX_CONCURRENCY_PESSIMISTIC = 1
        };

        /**
         * Defines different cache transaction isolation levels.
         */
        enum TransactionIsolation
        {
            /** Read committed isolation level. */
            IGNITE_TX_ISOLATION_READ_COMMITTED = 0,

            /** Repeatable read isolation level. */
            IGNITE_TX_ISOLATION_REPEATABLE_READ = 1,

            /** Serializable isolation level. */
            IGNITE_TX_ISOLATION_SERIALIZABLE = 2
        };

        /**
         * Transactions.
         */
        class IGNITE_FRIEND_EXPORT Transaction
        {
        public:
            /**
             * Constructor.
             */
            Transaction(common::concurrent::SharedPointer<impl::transactions::TransactionImpl> impl);

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            Transaction(const Transaction& other);

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return This.
             */
            Transaction& operator=(const Transaction& other);

            /**
             * Destructor.
             */
            ~Transaction();

            /**
             * Commit the transaction.
             */
            void Commit();

            /**
             * Commit the transaction.
             *
             * @param err Error.
             */
            void Commit(IgniteError& err);

            /**
             * Rollback the transaction.
             */
            void Rollback();

            /**
             * Rollback the transaction.
             *
             * @param err Error.
             */
            void Rollback(IgniteError& err);

            /**
             * Close the transaction.
             */
            void Close();

            /**
             * Close the transaction.
             *
             * @param err Error.
             */
            void Close(IgniteError& err);

            /**
             * Make transaction into rollback-only.
             *
             * After transaction have been marked as rollback-only it may
             * only be rolled back. Error occurs if such transaction is
             * being commited.
             */
            void SetRollbackOnly();

            /**
             * Make transaction into rollback-only.
             *
             * After transaction have been marked as rollback-only it may
             * only be rolled back. Error occurs if such transaction is
             * being commited.
             *
             * @param err Error.
             */
            void SetRollbackOnly(IgniteError& err);

            /**
             * Check if the transaction is rollback-only.
             *
             * After transaction have been marked as rollback-only it may
             * only be rolled back. Error occurs if such transaction is
             * being commited.
             *
             * @return True if the transaction is rollback-only.
             */
            bool IsRollbackOnly()
            {
                return impl.Get()->IsRollbackOnly();
            }

            /**
             * Get concurrency.
             *
             * @return Concurrency.
             */
            TransactionConcurrency GetConcurrency() const
            {
                return static_cast<TransactionConcurrency>(impl.Get()->GetConcurrency());
            }

            /**
             * Get isolation.
             *
             * @return Isolation.
             */
            TransactionIsolation GetIsolation() const
            {
                return static_cast<TransactionIsolation>(impl.Get()->GetIsolation());
            }

            /**
             * Get current state.
             *
             * @return Transaction state.
             */
            TransactionState GetState()
            {
                return impl.Get()->GetState();
            }

            /**
             * Get timeout.
             *
             * @return Timeout in milliseconds. Zero if timeout is infinite.
             */
            int64_t GetTimeout() const
            {
                return impl.Get()->GetTimeout();
            }

            /**
             * Check if the instance is valid and can be used.
             *
             * @return True if the instance is valid and can be used.
             */
            bool IsValid() const
            {
                return impl.IsValid();
            }

        private:
            /** Implementation delegate. */
            common::concurrent::SharedPointer<impl::transactions::TransactionImpl> impl;
        };
    }
}

#endif //_IGNITE_TRANSACTIONS_TRANSACTION