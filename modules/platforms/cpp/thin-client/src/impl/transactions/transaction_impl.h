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

#include "impl/data_router.h"
#include <ignite/common/fixed_size_array.h>
#include "ignite/thin/transactions/transaction_consts.h"
#include "impl/transactions/transactions_impl.h"

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
                     * @param txid Transaction Id.
                     * @param concurrency Transaction concurrency.
                     * @param isolation Transaction isolation.
                     * @param timeout Transaction timeout.
                     * @param size Number of entries participating in transaction (may be approximate).
                     */
                    TransactionImpl(
                            TransactionsImpl& txImpl,
                            int32_t txid,
                            ignite::thin::transactions::TransactionConcurrency::Type concurrency,
                            ignite::thin::transactions::TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t size) :
                        txs(txImpl),
                        txId(txid),
                        concurrency(concurrency),
                        isolation(isolation),
                        timeout(timeout),
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
                     * @param label Transaction specific label.
                     */
                    static SP_TransactionImpl Create(
                            TransactionsImpl& txs,
                            ignite::thin::transactions::TransactionConcurrency::Type concurrency,
                            ignite::thin::transactions::TransactionIsolation::Type isolation,
                            int64_t timeout,
                            int32_t txSize,
                            ignite::common::concurrent::SharedPointer<common::FixedSizeArray<char> > label);
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
                    static ignite::common::concurrent::ThreadLocalInstance<SP_TransactionImpl> threadTx;

                    /** Concurrency. */
                    int concurrency;

                    /** Isolation. */
                    int isolation;

                    /** Timeout in milliseconds. */
                    int64_t timeout;

                    /** Transaction size. */
                    int32_t txSize;

                    /** Closed flag. */
                    bool closed;

                    IGNITE_NO_COPY_ASSIGNMENT(TransactionImpl)
                };
            }
        }
    }
}

#endif // _IGNITE_IMPL_THIN_TRANSACTION_IMPL
