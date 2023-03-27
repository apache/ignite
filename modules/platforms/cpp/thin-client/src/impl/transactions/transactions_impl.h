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

#ifndef _IGNITE_IMPL_THIN_TRANSACTIONS_IMPL
#define _IGNITE_IMPL_THIN_TRANSACTIONS_IMPL

#include <ignite/common/fixed_size_array.h>
#include <ignite/thin/transactions/transaction_consts.h>

#include "impl/data_router.h"
#include "impl/transactions/transaction_impl.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                /**
                 * Thin client transaction.
                 */
                class TransactionsImpl
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param router Data router instance.
                     */
                    TransactionsImpl(const SP_DataRouter& router);

                    /**
                     * Destructor.
                     */
                    ~TransactionsImpl() {}

                    /**
                     * Start new transaction.
                     *
                     * @param concurrency Concurrency.
                     * @param isolation Isolation.
                     * @param timeout Timeout in milliseconds. Zero if for infinite timeout.
                     * @param txSize Number of entries participating in transaction (may be approximate).
                     * @param label Transaction specific label.
                     * @return Transaction ID on success.
                     */
                    SP_TransactionImpl TxStart(
                            ignite::thin::transactions::TransactionConcurrency::Type concurrency,
                            ignite::thin::transactions::TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t txSize,
                            ignite::common::concurrent::SharedPointer<common::FixedSizeArray<char> > label);

                    /**
                     * Get active transaction for the current thread.
                     *
                     * @return Active transaction implementation for current thread
                     * or null pointer if there is no active transaction for the thread.
                     */
                    SP_TransactionImpl GetCurrent();

                    /**
                     * Set active transaction for the current thread.
                     *
                     * @param impl Active transaction implementation for current thread
                     * or null pointer if there is no active transaction for the thread.
                     */
                    void SetCurrent(const SP_TransactionImpl& impl);

                    /**
                     * Reset active transaction for the current thread.
                     */
                    void ResetCurrent();

                private:
                    /** Data router. */
                    SP_DataRouter router;

                    /** Thread local instance of the transaction. */
                    ignite::common::concurrent::ThreadLocalInstance<SP_TransactionImpl> threadTx;

                    IGNITE_NO_COPY_ASSIGNMENT(TransactionsImpl);
                };

                typedef ignite::common::concurrent::SharedPointer<TransactionsImpl> SP_TransactionsImpl;
            }
        }
    }
}

#endif // _IGNITE_IMPL_THIN_TRANSACTIONS_IMPL
