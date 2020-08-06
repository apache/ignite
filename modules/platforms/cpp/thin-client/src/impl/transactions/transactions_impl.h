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

#ifndef TRANSACTIONS_IMPL_H
#define TRANSACTIONS_IMPL_H

#include "impl/data_router.h"
#include "ignite/thin/transactions/transaction_consts.h"
#include "impl/response_status.h"

using namespace ignite::thin::transactions;
using namespace ignite::common::concurrent;

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {          
                /* Forward declaration. */
                class TransactionsImpl;

                typedef SharedPointer<TransactionsImpl> SP_TransactionsImpl;

                /**
                 * Ignite transactions implementation.
                 *
                 * This is an entry point for Thin C++ Ignite transactions.
                 */
                class TransactionImpl
                {
                    typedef SharedPointer<TransactionImpl> SP_TransactionImpl;
                    typedef SharedPointer<TransactionsImpl> SP_TransactionsImpl;
                    typedef ThreadLocalInstance<SP_TransactionImpl> TL_SP_TransactionsImpl;

                public:
                    /**
                     * Constructor.
                     *
                     * @param txImpl Transactions implementation.
                     * @param txid Transaction Id.
                     * @param concurrency Transaction concurrency.
                     * @param isolation Transaction isolation.
                     * @param timeout Transaction timeout.
                     * @param size Number of entries participating in transaction (may be approximate).
                     */
                    TransactionImpl(
                            SP_TransactionsImpl txImpl,
                            int32_t txid,
                            TransactionConcurrency::Type concurrency,
                            TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t size) :
                        txs(txImpl),
                        txId(txid),
                        concurrency(concurrency),
                        isolation(isolation),
                        timeout(timeout),
                        txSize(size),
                        state(TransactionState::UNKNOWN),
                        closed(false)
                    {}
                    
                    /**
                     * Destructor.
                     */
                    ~TransactionImpl() {}
                    
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
                    void Close() {}

                    /**
                     * @return Current transaction Id.
                     */
                    int32_t TxId()
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
                     * @return Current transaction.
                     */
                    static SP_TransactionImpl GetCurrent();

                    /**
                     * Starts transaction.
                     *
                     * @param txs Transactions implementation.
                     * @param concurrency Transaction concurrency.
                     * @param isolation Transaction isolation.
                     * @param timeout Transaction timeout.
                     * @param txSize Number of entries participating in transaction (may be approximate).
                     */
                    static SP_TransactionImpl Create(
                            SP_TransactionsImpl txs,
                            TransactionConcurrency::Type concurrency,
                            TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t txSize);
                private:
                    /** Transactions implementation. */
                    SP_TransactionsImpl txs;

                    /** Current transaction Id. */
                    int32_t txId;

                    /** Thread local instance of the transaction. */
                    static TL_SP_TransactionsImpl threadTx;

                    /** Concurrency. */
                    int concurrency;

                    /** Isolation. */
                    int isolation;

                    /** Timeout in milliseconds. */
                    int64_t timeout;

                    /** Transaction size. */
                    int32_t txSize;

                    /** Transaction state. */
                    TransactionState::Type state;

                    /** Closed flag. */
                    bool closed;

                    IGNITE_NO_COPY_ASSIGNMENT(TransactionImpl)
                };
            
                /**
                 * Thin client transaction.
                 */
                class TransactionsImpl
                {
                    typedef SharedPointer<TransactionImpl> SP_TransactionImpl;
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
                    ~TransactionsImpl();

                    /**
                     * Start new transaction with default isolation, concurrency
                     * and timeout.
                     *
                     * @return New transaction instance.
                     */
                    SP_TransactionImpl TxStart();

                    /**
                     * Commit Transaction.
                     *
                     * @param id Transaction ID.
                     * @param err Error.
                     * @return Resulting state.
                     */
                    int32_t TxCommit(int32_t);

                    /**
                     * Rollback Transaction.
                     *
                     * @param id Transaction ID.
                     * @param err Error.
                     * @return Resulting state.
                     */
                    int32_t TxRollback(int32_t);

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
                    void SyncMessage(const ReqT& req, RspT& rsp);
                private:
                    /** Data router. */
                    SP_DataRouter router;

                    IGNITE_NO_COPY_ASSIGNMENT(TransactionsImpl)
                };
            }
        }
    }
}

#endif // TRANSACTIONS_IMPL_H
