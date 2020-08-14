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

#include "impl/message.h"
#include "impl/transactions/transactions_impl.h"
#include "impl/response_status.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl::thin;

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                TransactionImpl::TL_TXID TransactionImpl::threadTx;

                TransactionsImpl::TransactionsImpl(const SP_DataRouter& router) :
                    router(router)
                {
                }

                std::map<int32_t, SP_TransactionImpl> TransactionImpl::txToId;

                ReadWriteLock TransactionImpl::txToIdRWLock;

                template<typename ReqT, typename RspT>
                void TransactionsImpl::SyncMessage(const ReqT& req, RspT& rsp)
                {
                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_TX, rsp.GetError().c_str());
                }

                SharedPointer<TransactionImpl> TransactionsImpl::TxStart(
                        TransactionConcurrency::Type concurrency,
                        TransactionIsolation::Type isolation,
                        int64_t timeout,
                        int32_t txSize,
                        const char* label)
                {
                    SP_TransactionImpl tx = TransactionImpl::Create(*this, concurrency, isolation, timeout, txSize, label);

                    return tx;
                }

                SP_TransactionImpl TransactionImpl::Create(
                    TransactionsImpl& txs,
                    TransactionConcurrency::Type concurrency,
                    TransactionIsolation::Type isolation,
                    int64_t timeout,
                    int32_t txSize,
                    const char* label)
                {
                    int32_t txId = threadTx.Get();

                    SP_TransactionImpl tx;

                    if (txId != 0)
                    {
                        std::map<int32_t, SP_TransactionImpl>::iterator it;

                        {
                            RwSharedLockGuard lock(txToIdRWLock);

                            it = txToId.find(txId);

                            if (it != txToId.end())
                                tx = it->second;
                            else
                                throw IgniteError(IgniteError::IGNITE_ERR_TX_THIS_THREAD, TX_ALREADY_CLOSED);
                        }

                        TransactionImpl* ptr = tx.Get();

                        if (ptr && !ptr->IsClosed())
                        {
                            throw IgniteError(IgniteError::IGNITE_ERR_TX_THIS_THREAD, TX_ALREADY_STARTED);
                        }
                    }

                    TxStartRequest<RequestType::OP_TX_START> req(concurrency, isolation, timeout, txSize, label);

                    Int32Response rsp;

                    txs.SyncMessage(req, rsp);

                    int32_t curTxId = rsp.GetValue();

                    tx = SP_TransactionImpl(new TransactionImpl(txs, curTxId, concurrency, isolation, timeout, txSize));

                    threadTx.Set(curTxId);

                    RwExclusiveLockGuard lock(txToIdRWLock);

                    txToId[tx.Get()->TxId()] = tx;

                    return tx;
                }

                SP_TransactionImpl TransactionImpl::GetCurrent()
                {
                    int32_t txId = threadTx.Get();

                    SP_TransactionImpl tx;

                    std::map<int32_t, SP_TransactionImpl>::iterator it;

                    {
                        RwSharedLockGuard lock(txToIdRWLock);

                        it = txToId.find(txId);

                        if (it != txToId.end())
                        {
                            tx = it->second;
                        }
                    }

                    if (tx.IsValid())
                    {
                        TransactionImpl* ptr = tx.Get();

                        if (ptr && ptr->IsClosed())
                        {
                            tx = SP_TransactionImpl();

                            threadTx.Remove();
                        }
                    }
                    else
                    {
                        tx = SP_TransactionImpl();
                    }

                    return tx;
                }

                bool TransactionImpl::IsClosed() const
                {
                    return closed;
                }

                SP_TransactionImpl TransactionsImpl::GetCurrent()
                {
                    return TransactionImpl::GetCurrent();
                }

                int32_t TransactionsImpl::TxCommit(int32_t txId)
                {
                    TxEndRequest<RequestType::OP_TX_END> req(txId, true);

                    Response rsp;

                    SyncMessage(req, rsp);

                    return rsp.GetStatus();
                }

                int32_t TransactionsImpl::TxRollback(int32_t txId)
                {
                    TxEndRequest<RequestType::OP_TX_END> req(txId, false);

                    Response rsp;

                    SyncMessage(req, rsp);

                    return rsp.GetStatus();
                }

                int32_t TransactionsImpl::TxClose(int32_t txId)
                {
                    return TxRollback(txId);
                }

                void TransactionImpl::Commit()
                {
                    txThreadCheck(*this);

                    txs.TxCommit(txId);

                    txThreadEnd(*this);
                }

                void TransactionImpl::Rollback()
                {
                    txThreadCheck(*this);

                    txs.TxRollback(txId);

                    txThreadEnd(*this);
                }

                void TransactionImpl::Close()
                {
                    txThreadCheck(*this);

                    if (IsClosed())
                    {
                        return;
                    }

                    txs.TxClose(txId);

                    txThreadEnd(*this);
                }

                void TransactionImpl::Closed()
                {
                    closed = true;
                }

                void TransactionImpl::txThreadEnd(TransactionImpl& tx)
                {
                    tx.Closed();

                    RwExclusiveLockGuard lock(txToIdRWLock);

                    txToId.erase(tx.TxId());

                    threadTx.Set(0);
                }

                void TransactionImpl::txThreadCheck(const TransactionImpl& tx)
                {
                    int32_t currentTxId = threadTx.Get();

                    if (currentTxId == 0)
                        throw IgniteError(IgniteError::IGNITE_ERR_TX_THIS_THREAD, TX_ALREADY_CLOSED);

                    if (currentTxId != tx.TxId())
                        throw IgniteError(IgniteError::IGNITE_ERR_TX_THIS_THREAD, TX_DIFFERENT_THREAD);
                }
            }
        }
    }
}
