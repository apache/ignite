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

#ifndef TRANSACTIONS_H
#define TRANSACTIONS_H

#include <string>

#include <ignite/common/concurrent.h>
#include <ignite/impl/thin/transactions/transactions_proxy.h>

using namespace ignite::impl::thin::transactions;
using namespace ignite::common::concurrent;

namespace ignite
{
    namespace thin
    {
        namespace transactions
        {
            /**
             * Transaction client.
             *
             * Implements main transactionsl API.
             *
             * This class implemented as a reference to an implementation so copying of this class instance will only
             * create another reference to the same underlying object. Underlying object released automatically once all
             * the instances are destructed.
             */
            class ClientTransaction {

            public:                
                ClientTransaction(TransactionProxy impl) :
                    proxy(impl)
                {}

                /**
                 * Destructor.
                 */
                ~ClientTransaction() {}

                /**
                 * Commits this transaction.
                 */
                void Commit()
                {
                    proxy.commit();
                }

                /**
                 * Rolls back this transaction.
                 */
                void Rollback()
                {
                    proxy.rollback();
                }

                /**
                 * Ends the transaction. Transaction will be rolled back if it has not been committed.
                 */
                void Close()
                {
                    proxy.close();
                }

                /**
                 * Assignment operator.
                 *
                 * @param other Another instance.
                 * @return *this.
                 */
                ClientTransaction& operator=(const ClientTransaction& other)
                {
                    proxy = other.proxy;

                    return *this;
                }
            private:
                /** Implementation. */
                TransactionProxy proxy;

                /**
                 * Default constructor.
                 */
                ClientTransaction() {}
            };

            /**
             * Transactions client.
             *
             * This is an entry point for Thin C++ Ignite transactions.
             *
             * This class implemented as a reference to an implementation so copying of this class instance will only
             * create another reference to the same underlying object. Underlying object released automatically once all
             * the instances are destructed.
             */
            class ClientTransactions {
            public:
                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                ClientTransactions(common::concurrent::SharedPointer<void> impl) :
                    proxy(impl)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~ClientTransactions() {}

                /**
                 * Start new transaction with default isolation, concurrency
                 * and timeout.
                 *
                 * @return ClientTransaction implementation.
                 */
                ClientTransaction TxStart()
                {
                    return ClientTransaction(proxy.txStart());
                }

                /**
                 * Start new transaction with defined concurrency and isolation.
                 *
                 * @param concurrency Transaction concurrency.
                 * @param isolation Transaction isolation.
                 *
                 * @return ClientTransaction implementation.
                 */
                ClientTransaction TxStart(TransactionConcurrency::Type concurrency, TransactionIsolation::Type isolation)
                {
                    return ClientTransaction(proxy.txStart(concurrency, isolation));
                }

                /**
                 * Start new transaction with completely clarify parameters.
                 *
                 * @param concurrency Transaction concurrency.
                 * @param isolation Transaction isolation.
                 * @param timeout Transaction timeout.
                 * @param txSize Number of entries participating in transaction (may be approximate).
                 *
                 * @return ClientTransaction implementation.
                 */
                ClientTransaction TxStart(TransactionConcurrency::Type concurrency, TransactionIsolation::Type isolation, int64_t timeout, int32_t txSize)
                {
                    return ClientTransaction(proxy.txStart(concurrency, isolation, timeout, txSize));
                }

                /**
                 * Sets label.
                 *
                 * @param label Transaction label.
                 * @return ClientTransaction implementation.
                 */
                ClientTransactions withLabel(std::string& label);
            private:
                /** Implementation. */
                TransactionsProxy proxy;

                /**
                 * Default constructor.
                 */
                ClientTransactions() {}
            };
        }
    }
}

#endif // TRANSACTIONS_H
