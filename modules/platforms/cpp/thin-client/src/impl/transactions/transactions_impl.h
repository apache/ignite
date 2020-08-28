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

#include "impl/data_router.h"
#include <ignite/common/fixed_size_array.h>
#include "ignite/thin/transactions/transaction_consts.h"
#include "impl/transactions/transaction_impl.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                class TransactionsImpl;

                typedef ignite::common::concurrent::SharedPointer<TransactionImpl> SP_TransactionImpl;
                typedef ignite::common::concurrent::SharedPointer<TransactionsImpl> SP_TransactionsImpl;

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
                     * Commit Transaction.
                     *
                     * @param id Transaction ID.
                     * @return Resulting state.
                     */
                    int32_t TxCommit(int32_t id);

                    /**
                     * Rollback Transaction.
                     *
                     * @param id Transaction ID.
                     * @return Resulting state.
                     */
                    int32_t TxRollback(int32_t id);


                    /**
                     * Close the transaction.
                     *
                     * This method should only be used on the valid instance.
                     *
                     * @param id Transaction ID.
                     */
                    int32_t TxClose(int32_t id);

                    /**
                     * Get active transaction for the current thread.
                     *
                     * @return Active transaction implementation for current thread
                     * or null pointer if there is no active transaction for
                     * the thread.
                     */
                    SP_TransactionImpl GetCurrent();

                    /**
                     * Synchronously send message and receive response.
                     *
                     * @param req Request message.
                     * @param rsp Response message.
                     * @throw IgniteError on error.
                     */
                    template<typename ReqT, typename RspT>
                    void SendTxMessage(const ReqT& req, RspT& rsp);
                private:
                    /** Data router. */
                    SP_DataRouter router;

                    IGNITE_NO_COPY_ASSIGNMENT(TransactionsImpl)
                };
            }
        }
    }
}

#endif // _IGNITE_IMPL_THIN_TRANSACTIONS_IMPL
