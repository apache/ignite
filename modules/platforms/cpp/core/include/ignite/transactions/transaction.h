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