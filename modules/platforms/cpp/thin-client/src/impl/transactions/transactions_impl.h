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

                /* Forward declaration. */
                class TransactionImpl;

                typedef SharedPointer<TransactionsImpl> SP_TransactionsImpl;
                typedef SharedPointer<TransactionImpl> SP_TransactionImpl;

                /**
                 * Ignite transactions implementation.
                 *
                 * This is an entry point for Thin C++ Ignite transactions.
                 */
                class TransactionImpl
                {
                    typedef ThreadLocalInstance<int32_t> TL_TXID;

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
                            TransactionsImpl& txImpl,
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
                    void Closed();

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
                            TransactionsImpl& txs,
                            TransactionConcurrency::Type concurrency,
                            TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t txSize);
                protected:
                    /** Checks current thread state. */
                    static void txThreadCheck(const TransactionImpl& tx);

                    /** Completes tc and clear state from storage. */
                    static void txThreadEnd(TransactionImpl& tx);

                private:
                    /** Transactions implementation. */
                   TransactionsImpl& txs;

                    /** Current transaction Id. */
                    int32_t txId;

                    /** Thread local instance of the transaction. */
                    static TL_TXID threadTx;

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

                    /** Cache affinity mapping read-write lock. */
                    static ReadWriteLock txToIdRWLock;

                    /** TxId to transaction map. */
                    static std::map<int32_t, SP_TransactionImpl> txToId;

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
                    ~TransactionsImpl() {}

                    /**
                     * Start new transaction.
                     *
                     * @param concurrency Concurrency.
                     * @param isolation Isolation.
                     * @param timeout Timeout in milliseconds. Zero if for infinite timeout.
                     * @param txSize Number of entries participating in transaction (may be approximate).
                     * @param err Error.
                     * @return Transaction ID on success.
                     */
                    SharedPointer<TransactionImpl> TxStart(
                            TransactionConcurrency::Type concurrency,
                            TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t txSize);

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
                    void SyncMessage(const ReqT& req, RspT& rsp);
                private:
                    /** Data router. */
                    SP_DataRouter router;

                    IGNITE_NO_COPY_ASSIGNMENT(TransactionsImpl)
                };

                void txThreadCheck();
            }
        }
    }
}

#endif // TRANSACTIONS_IMPL_H
