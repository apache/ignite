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

#ifndef _IGNITE_THIN_TRANSACTIONS_CLIENT_TRANSACTIONS
#define _IGNITE_THIN_TRANSACTIONS_CLIENT_TRANSACTIONS

#include <string>

#include <ignite/common/concurrent.h>
#include <ignite/common/fixed_size_array.h>
#include <ignite/impl/thin/transactions/transactions_proxy.h>
#include "ignite/thin/transactions/transaction.h"

namespace ignite
{
    namespace thin
    {
        namespace transactions
        {
            /**
             * Transactions client.
             *
             * This is an entry point for Thin C++ Ignite transactions.
             *
             * This class is implemented as a reference to an implementation so copying of this class instance will only
             * create another reference to the same underlying object. Underlying object will be released automatically
             * once all the instances are destructed.
             */
            class ClientTransactions {
            public:
                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                ClientTransactions(ignite::common::concurrent::SharedPointer<void> impl) :
                    proxy(impl),
                    label(ignite::common::concurrent::SharedPointer<ignite::common::FixedSizeArray<char> >())
                {
                    // No-op.
                }

                /**
                 * Default constructor.
                 */
                ClientTransactions()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~ClientTransactions()
                {
                    // No-op.
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
                ClientTransaction TxStart(
                        TransactionConcurrency::Type concurrency = TransactionConcurrency::PESSIMISTIC,
                        TransactionIsolation::Type isolation = TransactionIsolation::READ_COMMITTED,
                        int64_t timeout = 0,
                        int32_t txSize = 0)
                {
                    return ClientTransaction(proxy.txStart(concurrency, isolation, timeout, txSize, label));
                }

                /**
                 * Returns instance of {@code ClientTransactions} to mark each new transaction with a specified label.
                 *
                 * @param label Transaction label.
                 * @return ClientTransactions implementation.
                 */
                ClientTransactions withLabel(const std::string& lbl)
                {
                    ClientTransactions copy = ClientTransactions(proxy, lbl);

                    return copy;
                }
            private:
                /** Implementation. */
                ignite::impl::thin::transactions::TransactionsProxy proxy;

                /** Transaction specific label. */
                ignite::common::concurrent::SharedPointer<ignite::common::FixedSizeArray<char> > label;

                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                ClientTransactions(ignite::impl::thin::transactions::TransactionsProxy& impl, const std::string& lbl) :
                    proxy(impl)
                {
                    ignite::common::FixedSizeArray<char> *label0 =
                        new ignite::common::FixedSizeArray<char>(static_cast<int32_t>(lbl.size()) + 1);

                    strcpy(label0->GetData(), lbl.c_str());

                    label = ignite::common::concurrent::SharedPointer<ignite::common::FixedSizeArray<char> >(label0);
                }
            };
        }
    }
}

#endif // _IGNITE_THIN_TRANSACTIONS_CLIENT_TRANSACTION
