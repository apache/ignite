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

#ifndef TRANSACTIONS_PROXY_H
#define TRANSACTIONS_PROXY_H

#include "ignite/common/concurrent.h"
#include "ignite/thin/transactions/transaction_consts.h"

using namespace ignite::common::concurrent;
using namespace ignite::thin::transactions;

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                /**
                 * Ignite transaction class proxy.
                 */
                class IGNITE_IMPORT_EXPORT TransactionProxy {

                public:
                    /**
                     * Default constructor.
                     */
                    TransactionProxy() {}

                    /**
                     * Constructor.
                     * @param impl Transaction implementation.
                     */
                    TransactionProxy(const SharedPointer<void>& impl) :
                        impl(impl)
                    {}

                    /**
                     * Assignment operator.
                     *
                     * @param other Another instance.
                     * @return *this.
                     */
                    TransactionProxy& operator=(const TransactionProxy& other)
                    {
                        impl = other.impl;

                        return *this;
                    }

                    /**
                     * Destructor.
                     */
                    ~TransactionProxy() {};

                    /**
                     * Commit the transaction.
                     */
                    void commit();

                    /**
                     * Rollback the transaction.
                     */
                    void rollback();

                    /**
                     * Close the transaction.
                     */
                    void close();

                private:
                    /** Implementation. */
                    SharedPointer<void> impl;
                };

                /**
                 * Ignite transactions class proxy.
                 */
                class IGNITE_IMPORT_EXPORT TransactionsProxy
                {
                    #define DEFAULT_CONCURRENCY TransactionConcurrency::PESSIMISTIC
                    #define DEFAULT_ISOLATION TransactionIsolation::READ_COMMITTED
                    #define DEFAULT_TIMEOUT 0
                    #define DEFAULT_TX_SIZE 0
                public:
                    /**
                     * Constructor.
                     */
                    TransactionsProxy(const SharedPointer<void>& impl) :
                        impl(impl)
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    ~TransactionsProxy() {}

                    /**
                     * Start new transaction with completely clarify parameters.
                     *
                     * @param concurrency Transaction concurrency.
                     * @param isolation Transaction isolation.
                     * @param timeout Transaction timeout.
                     * @param txSize Number of entries participating in transaction (may be approximate).
                     * @param lbl Transaction specific label.
                     *
                     * @return Proxy implementation.
                     */
                    TransactionProxy txStart(
                            TransactionConcurrency::Type concurrency = TransactionConcurrency::PESSIMISTIC,
                            TransactionIsolation::Type isolation = TransactionIsolation::READ_COMMITTED,
                            int64_t timeout = 0,
                            int32_t txSize = 0,
                            SharedPointer<const char> lbl = "");
                private:
                    /** Implementation. */
                    SharedPointer<void> impl;

                    /**
                     * Default constructor.
                     */
                    TransactionsProxy() {}
                };
            }
        }
    }
}

#endif // TRANSACTIONS_PROXY_H
