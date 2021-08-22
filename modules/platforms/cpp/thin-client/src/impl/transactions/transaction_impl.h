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

#ifndef _IGNITE_IMPL_THIN_TRANSACTION_IMPL
#define _IGNITE_IMPL_THIN_TRANSACTION_IMPL

#include <ignite/common/fixed_size_array.h>
#include <ignite/thin/transactions/transaction_consts.h>

#include "impl/data_router.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                /* Forward declaration. */
                class TransactionImpl;

                /* Forward declaration. */
                class TransactionsImpl;

                typedef ignite::common::concurrent::SharedPointer<TransactionImpl> SP_TransactionImpl;

                /**
                 * Ignite transactions implementation.
                 *
                 * This is an entry point for Thin C++ Ignite transactions.
                 */
                class TransactionImpl
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param txImpl Transactions implementation.
                     * @param channel Channel linked to transaction.
                     * @param txId Transaction Id.
                     * @param concurrency Transaction concurrency.
                     * @param isolation Transaction isolation.
                     * @param timeout Transaction timeout.
                     * @param ioTimeout IO timeout for channel.
                     * @param size Number of entries participating in transaction (may be approximate).
                     */
                    TransactionImpl(
                            TransactionsImpl& txImpl,
                            SP_DataChannel channel,
                            int32_t txId,
                            ignite::thin::transactions::TransactionConcurrency::Type concurrency,
                            ignite::thin::transactions::TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t ioTimeout,
                            int32_t size) :
                        channel(channel),
                        txs(txImpl),
                        txId(txId),
                        concurrency(concurrency),
                        isolation(isolation),
                        timeout(timeout),
                        ioTimeout(ioTimeout),
                        txSize(size),
                        closed(false)
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    ~TransactionImpl()
                    {
                        if (!IsClosed())
                            Close();
                    }

                    /**
                     * Commits this transaction.
                     */
                    void Commit();

                    /**
                     * Rolls back this transaction.
                     */
                    void Rollback();

                    /**
                     * Ends the transaction. Transaction will be rolled back if it has not been committed.
                     */
                    void Close();

                    /**
                     * @return Current transaction Id.
                     */
                    int32_t TxId() const
                    {
                        return txId;
                    }

                    /**
                     * Check if the transaction has been closed.
                     *
                     * @return True if the transaction has been closed.
                     */
                    bool IsClosed() const;

                    /**
                     * Sets close flag to tx.
                     */
                    void SetClosed();

                    /**
                     * Starts transaction.
                     *
                     * @param txs Transactions implementation.
                     * @param router Router to use to start transaction.
                     * @param concurrency Transaction concurrency.
                     * @param isolation Transaction isolation.
                     * @param timeout Transaction timeout.
                     * @param txSize Number of entries participating in transaction (may be approximate).
                     * @param label Transaction specific label.
                     */
                    static SP_TransactionImpl Create(
                            TransactionsImpl& txs,
                            SP_DataRouter& router,
                            ignite::thin::transactions::TransactionConcurrency::Type concurrency,
                            ignite::thin::transactions::TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t txSize,
                            ignite::common::concurrent::SharedPointer<common::FixedSizeArray<char> > label);

                    /**
                     * Get channel for the transaction.
                     *
                     * @return Channel.
                     */
                    SP_DataChannel GetChannel()
                    {
                        return channel;
                    }

                private:
                    /** Checks current thread state. */
                    void ThreadCheck();

                    /** Completes tc and clear state from storage. */
                    void ThreadEnd();

                    /**
                     * Synchronously send message and receive response.
                     *
                     * @param req Request message.
                     * @param rsp Response message.
                     * @throw IgniteError on error.
                     */
                    template<typename ReqT, typename RspT>
                    void SendTxMessage(const ReqT& req, RspT& rsp);

                    /** Data channel to use. */
                    SP_DataChannel channel;

                    /** Transactions implementation. */
                    TransactionsImpl& txs;

                    /** Current transaction Id. */
                    int32_t txId;

                    /** Concurrency. */
                    int concurrency;

                    /** Isolation. */
                    int isolation;

                    /** Timeout in milliseconds. */
                    int64_t timeout;

                    /** Channel io timeout. */
                    int32_t ioTimeout;

                    /** Transaction size. */
                    int32_t txSize;

                    /** Closed flag. */
                    bool closed;

                    IGNITE_NO_COPY_ASSIGNMENT(TransactionImpl);
                };
            }
        }
    }
}

#endif // _IGNITE_IMPL_THIN_TRANSACTION_IMPL
