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

#ifndef _IGNITE_IMPL_TRANSACTIONS_TRANSACTION_IMPL
#define _IGNITE_IMPL_TRANSACTIONS_TRANSACTION_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>
#include <ignite/transactions/transaction_consts.h>

#include "ignite/impl/transactions/transactions_impl.h"
#include "ignite/impl/ignite_environment.h"

namespace ignite 
{
    namespace impl
    {
        namespace transactions
        {
            /**
             * Transaction implementation.
             */
            class IGNITE_FRIEND_EXPORT TransactionImpl
            {
                typedef ignite::common::concurrent::SharedPointer<TransactionImpl> SP_TransactionImpl;
                typedef ignite::common::concurrent::SharedPointer<TransactionsImpl> SP_TransactionsImpl;
                typedef ignite::common::concurrent::ThreadLocalInstance<SP_TransactionImpl> TL_SP_TransactionsImpl;
                typedef ignite::common::concurrent::CriticalSection CriticalSection;
                typedef ignite::transactions::TransactionState TransactionState;
            public:
                /**
                 * Destructor.
                 */
                ~TransactionImpl();

                /**
                 * Factory method. Create new instance of the class.
                 *
                 * @param txs Transactions implimentation.
                 * @param id Transaction id.
                 * @param concurrency Concurrency.
                 * @param isolation Isolation.
                 * @param timeout Timeout in milliseconds.
                 * @param txSize Transaction size.
                 *
                 * @return Shared pointer to new instance.
                 */
                static SP_TransactionImpl Create(SP_TransactionsImpl txs, int concurrency,
                    int isolation, int64_t timeout, int32_t txSize, IgniteError& err);

                /**
                 * Get active transaction for the current thread.
                 *
                 * @return Active transaction implementation for current thread
                 * or null pointer if there is no active transaction for
                 * the thread.
                 */
                static SP_TransactionImpl GetCurrent();

                /**
                 * Check if the transaction has been closed.
                 *
                 * @return True if the transaction has been closed.
                 */
                bool IsClosed() const;

                /**
                 * Commit the transaction.
                 *
                 * @param err Error.
                 */
                void Commit(IgniteError& err);

                /**
                 * Rollback the transaction.
                 *
                 * @param err Error.
                 */
                void Rollback(IgniteError& err);

                /**
                 * Close the transaction.
                 *
                 * @param err Error.
                 */
                void Close(IgniteError& err);

                /**
                 * Make transaction into rollback-only.
                 *
                 * After transaction have been marked as rollback-only it may
                 * only be rolled back. Error occurs if such transaction is
                 * being commited.
                 *
                 * @param err Error.
                 */
                void SetRollbackOnly(IgniteError& err);

                /**
                 * Check if the transaction is rollback-only.
                 *
                 * After transaction have been marked as rollback-only it may
                 * only be rolled back. Error occurs if such transaction is
                 * being commited.
                 *
                 * @param err Error.
                 * @return True if the transaction is rollback-only.
                 */
                bool IsRollbackOnly(IgniteError& err);

                /**
                 * Get state.
                 *
                 * @param err Error.
                 * @return Current state.
                 */
                TransactionState::Type GetState(IgniteError& err);

                /**
                 * Get concurrency.
                 *
                 * @return Concurrency.
                 */
                int GetConcurrency() const
                {
                    return concurrency;
                }

                /**
                 * Get isolation.
                 *
                 * @return Isolation.
                 */
                int GetIsolation() const
                {
                    return isolation;
                }

                /**
                 * Get timeout.
                 *
                 * @return Timeout in milliseconds.
                 */
                int64_t GetTimeout() const
                {
                    return timeout;
                }

            private:
                /**
                 * Constructor.
                 *
                 * @param txs Transactions implimentation.
                 * @param id Transaction id.
                 * @param concurrency Concurrency.
                 * @param isolation Isolation.
                 * @param timeout Timeout in milliseconds.
                 * @param txSize Transaction size.
                 */
                TransactionImpl(SP_TransactionsImpl txs, int64_t id, int concurrency,
                    int isolation, int64_t timeout, int32_t txSize);

                /**
                 * Get error for closed transaction.
                 *
                 * @return Error instance.
                 */
                IgniteError GetClosedError() const;

                /** Thread local instance of the transaction. */
                static TL_SP_TransactionsImpl threadTx;

                /** Access lock. */
                CriticalSection accessLock;

                /** Transactions. */
                SP_TransactionsImpl txs;

                /** Transaction ID. */
                int64_t id;

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

                IGNITE_NO_COPY_ASSIGNMENT(TransactionImpl);
            };
        }
    }
}

#endif //_IGNITE_IMPL_TRANSACTIONS_TRANSACTION_IMPL
