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

#ifndef _IGNITE_TRANSACTION
#define _IGNITE_TRANSACTION

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
            Transaction(common::concurrent::SharedPointer<impl::transactions::TransactionImpl> impl) :
                impl(impl)
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            Transaction(const Transaction& other) :
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
            Transaction& operator=(const Transaction& other)
            {
                impl = other.impl;

                return *this;
            }

            /**
             * Destructor.
             */
            ~Transaction()
            {
                // No-op.
            }

            /**
             * Commit the transaction.
             */
            void Commit()
            {
                IgniteError err;

                Commit(err);

                IgniteError::ThrowIfNeeded(err);
            }

            /**
             * Commit the transaction.
             *
             * @param err Error.
             */
            void Commit(IgniteError& err)
            {
                impl::transactions::TransactionImpl* txImpl = impl.Get();

                if (txImpl)
                    txImpl->Commit(err);
                else
                {
                    err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Instance is not usable (did you check for error?).");
                }
            }

            /**
             * Rollback the transaction.
             */
            void Rollback()
            {
                IgniteError err;

                Rollback(err);

                IgniteError::ThrowIfNeeded(err);
            }

            /**
             * Rollback the transaction.
             *
             * @param err Error.
             */
            void Rollback(IgniteError& err)
            {
                impl::transactions::TransactionImpl* txImpl = impl.Get();

                if (txImpl)
                    txImpl->Rollback(err);
                else
                {
                    err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Instance is not usable (did you check for error?).");
                }
            }

            /**
             * Close the transaction.
             */
            void Close()
            {
                IgniteError err;

                Close(err);

                IgniteError::ThrowIfNeeded(err);
            }

            /**
             * Close the transaction.
             *
             * @param err Error.
             */
            void Close(IgniteError& err)
            {
                impl::transactions::TransactionImpl* txImpl = impl.Get();

                if (txImpl)
                    txImpl->Close(err);
                else
                {
                    err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Instance is not usable (did you check for error?).");
                }
            }

            /**
             * Make transaction into rollback-only.
             *
             * After transaction have been marked as rollback-only it may
             * only be rolled back. Error occurs if such transaction is
             * being commited.
             */
            void SetRollbackOnly()
            {
                IgniteError err;

                SetRollbackOnly(err);

                IgniteError::ThrowIfNeeded(err);
            }

            /**
             * Make transaction into rollback-only.
             *
             * After transaction have been marked as rollback-only it may
             * only be rolled back. Error occurs if such transaction is
             * being commited.
             *
             * @param err Error.
             */
            void SetRollbackOnly(IgniteError& err)
            {
                impl::transactions::TransactionImpl* txImpl = impl.Get();

                if (txImpl)
                    txImpl->SetRollbackOnly(err);
                else
                {
                    err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Instance is not usable (did you check for error?).");
                }
            }

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

#endif