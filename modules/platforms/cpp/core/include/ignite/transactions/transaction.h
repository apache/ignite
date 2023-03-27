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

#include "ignite/impl/transactions/transaction_impl.h"
#include "ignite/transactions/transaction_consts.h"

namespace ignite
{
    namespace transactions
    {
        /**
         * %Ignite cache transaction.
         * Cache transactions have a default 2PC (two-phase-commit) behavior.
         *
         * @see TransactionConcurrency and TransactionIsolation for details on
         * the supported isolation levels and concurrency models.
         *
         * This class is implemented as a reference to an implementation so copying
         * of this class instance will only create another reference to the same
         * underlying object. Underlying object will be released automatically once all
         * the instances are destructed.
         */
        class IGNITE_FRIEND_EXPORT Transaction
        {
        public:
            /**
             * Constructor.
             *
             * Internal method. Should not be used by user.
             *
             * @param impl Implementation.
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
             *
             * This method should only be used on the valid instance.
             *
             * @throw IgniteError class instance in case of failure.
             */
            void Commit();

            /**
             * Commit the transaction.
             *
             * Properly sets error param in case of failure.
             *
             * This method should only be used on the valid instance.
             *
             * @param err Error.
             */
            void Commit(IgniteError& err);

            /**
             * Rollback the transaction.
             *
             * This method should only be used on the valid instance.
             *
             * @throw IgniteError class instance in case of failure.
             */
            void Rollback();

            /**
             * Rollback the transaction.
             *
             * Properly sets error param in case of failure.
             *
             * This method should only be used on the valid instance.
             *
             * @param err Error.
             */
            void Rollback(IgniteError& err);

            /**
             * Close the transaction.
             *
             * This method should only be used on the valid instance.
             *
             * @throw IgniteError class instance in case of failure.
             */
            void Close();

            /**
             * Close the transaction.
             *
             * Properly sets error param in case of failure.
             *
             * This method should only be used on the valid instance.
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
             *
             * This method should only be used on the valid instance.
             *
             * @throw IgniteError class instance in case of failure.
             */
            void SetRollbackOnly();

            /**
             * Make transaction into rollback-only.
             *
             * After transaction have been marked as rollback-only it may
             * only be rolled back. Error occurs if such transaction is
             * being commited.
             *
             * Properly sets error param in case of failure.
             *
             * This method should only be used on the valid instance.
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
             *
             * @throw IgniteError class instance in case of failure.
             */
            bool IsRollbackOnly();

            /**
             * Check if the transaction is rollback-only.
             *
             * After transaction have been marked as rollback-only it may
             * only be rolled back. Error occurs if such transaction is
             * being commited.
             *
             * Properly sets error param in case of failure.
             *
             * This method should only be used on the valid instance.
             *
             * @param err Error.
             * @return True if the transaction is rollback-only.
             */
            bool IsRollbackOnly(IgniteError& err);

            /**
             * Get current state.
             *
             * This method should only be used on the valid instance.
             *
             * @return Transaction state.
             *
             * @throw IgniteError class instance in case of failure.
             */
            TransactionState::Type GetState();

            /**
             * Get current state.
             *
             * Properly sets error param in case of failure.
             *
             * This method should only be used on the valid instance.
             *
             * @param err Error.
             * @return Transaction state.
             */
            TransactionState::Type GetState(IgniteError& err);

            /**
             * Get concurrency.
             *
             * This method should only be used on the valid instance.
             *
             * @return Concurrency.
             */
            TransactionConcurrency::Type GetConcurrency() const
            {
                return static_cast<TransactionConcurrency::Type>(impl.Get()->GetConcurrency());
            }

            /**
             * Get isolation.
             *
             * This method should only be used on the valid instance.
             *
             * @return Isolation.
             */
            TransactionIsolation::Type GetIsolation() const
            {
                return static_cast<TransactionIsolation::Type>(impl.Get()->GetIsolation());
            }

            /**
             * Get timeout.
             *
             * This method should only be used on the valid instance.
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
             * Invalid instance can be returned if some of the previous
             * operations have resulted in a failure. For example invalid
             * instance can be returned by not-throwing version of method
             * in case of error. Invalid instances also often can be
             * created using default constructor.
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