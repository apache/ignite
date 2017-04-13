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

#include "ignite/impl/transactions/transactions_impl.h"
#include "ignite/impl/transactions/transaction_impl.h"

using namespace ignite::jni::java;
using namespace ignite::transactions;

namespace ignite 
{
    namespace impl
    {
        namespace transactions
        {
            TransactionImpl::TL_SP_TransactionsImpl TransactionImpl::threadTx;

            TransactionImpl::TransactionImpl(SP_TransactionsImpl txs, int64_t id,
                int concurrency, int isolation, int64_t timeout, int32_t txSize) :
                txs(txs),
                id(id),
                concurrency(concurrency),
                isolation(isolation),
                timeout(timeout),
                txSize(txSize),
                state(TransactionState::UNKNOWN),
                closed(false)
            {
                // No-op.
            }

            TransactionImpl::SP_TransactionImpl TransactionImpl::Create(SP_TransactionsImpl txs,
                int concurrency, int isolation, int64_t timeout, int32_t txSize, IgniteError& err)
            {
                int64_t id = txs.Get()->TxStart(concurrency, isolation, timeout, txSize, err);

                SP_TransactionImpl tx;

                if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                {
                    tx = SP_TransactionImpl(new TransactionImpl(txs, id, concurrency,
                        isolation, timeout, txSize));

                    threadTx.Set(tx);
                }

                return tx;
            }

            TransactionImpl::~TransactionImpl()
            {
                // No-op.
            }

            TransactionImpl::SP_TransactionImpl TransactionImpl::GetCurrent()
            {
                SP_TransactionImpl tx = threadTx.Get();
                TransactionImpl* ptr = tx.Get();

                if (ptr && ptr->IsClosed())
                {
                    tx = SP_TransactionImpl();

                    threadTx.Remove();
                }

                return tx;
            }

            bool TransactionImpl::IsClosed() const
            {
                return closed;
            }

            void TransactionImpl::Commit(IgniteError& err)
            {
                common::concurrent::CsLockGuard guard(accessLock);

                if (IsClosed())
                {
                    err = GetClosedError();

                    return;
                }

                TransactionState::Type newState = txs.Get()->TxCommit(id, err);

                if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                {
                    state = newState;

                    closed = true;
                }
            }

            void TransactionImpl::Rollback(IgniteError & err)
            {
                common::concurrent::CsLockGuard guard(accessLock);

                if (IsClosed())
                {
                    err = GetClosedError();

                    return;
                }

                TransactionState::Type newState = txs.Get()->TxRollback(id, err);

                if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                {
                    state = newState;

                    closed = true;
                }
            }

            void TransactionImpl::Close(IgniteError & err)
            {
                common::concurrent::CsLockGuard guard(accessLock);

                if (IsClosed())
                {
                    err = GetClosedError();

                    return;
                }

                TransactionState::Type newState = txs.Get()->TxClose(id, err);

                if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                {
                    state = newState;

                    closed = true;
                }
            }

            void TransactionImpl::SetRollbackOnly(IgniteError & err)
            {
                common::concurrent::CsLockGuard guard(accessLock);

                if (IsClosed())
                {
                    err = GetClosedError();

                    return;
                }

                txs.Get()->TxSetRollbackOnly(id, err);
            }

            bool TransactionImpl::IsRollbackOnly(IgniteError& err)
            {
                TransactionState::Type state0 = GetState(err);

                return state0 == TransactionState::MARKED_ROLLBACK ||
                       state0 == TransactionState::ROLLING_BACK ||
                       state0 == TransactionState::ROLLED_BACK;
            }

            TransactionState::Type TransactionImpl::GetState(IgniteError& err)
            {
                common::concurrent::CsLockGuard guard(accessLock);

                if (closed)
                    return state;

                return txs.Get()->TxState(id, err);
            }

            IgniteError TransactionImpl::GetClosedError() const
            {
                std::stringstream buf;

                buf << "Transaction " << id << " is closed. State: " << state;

                return IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_STATE, buf.str().c_str());
            }
        }
    }
}

