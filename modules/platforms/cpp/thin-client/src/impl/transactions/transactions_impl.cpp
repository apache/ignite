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
#include "impl/transactions/transaction_impl.h"
#include "impl/response_status.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl::thin;
using namespace ignite::thin::transactions;

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                TransactionsImpl::TransactionsImpl(const SP_DataRouter& router) :
                    router(router)
                {
                    // No-op.
                }

                template<typename ReqT, typename RspT>
                void TransactionsImpl::SendTxMessage(const ReqT& req, RspT& rsp)
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
                        SharedPointer<common::FixedSizeArray<char> > label)
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
                    SharedPointer<common::FixedSizeArray<char> > label)
                {
                    SP_TransactionImpl tx = txs.GetCurrent();

                    TransactionImpl* ptr = tx.Get();

                    if (ptr && !ptr->IsClosed())
                    {
                        throw IgniteError(IgniteError::IGNITE_ERR_TX_THIS_THREAD, TX_ALREADY_STARTED);
                    }

                    TxStartRequest req(concurrency, isolation, timeout, label);

                    Int32Response rsp;

                    txs.SendTxMessage(req, rsp);

                    int32_t curTxId = rsp.GetValue();

                    tx = SP_TransactionImpl(new TransactionImpl(txs, curTxId, concurrency, isolation, timeout, txSize));

                    txs.SetCurrent(tx);

                    return tx;
                }

                bool TransactionImpl::IsClosed() const
                {
                    return closed;
                }

                SP_TransactionImpl TransactionsImpl::GetCurrent()
                {
                    SP_TransactionImpl tx = threadTx.Get();

                    TransactionImpl* ptr = tx.Get();

                    if (ptr && ptr->IsClosed())
                    {
                        threadTx.Remove();

                        tx = SP_TransactionImpl();
                    }

                    return tx;
                }

                void TransactionsImpl::SetCurrent(const SP_TransactionImpl& impl)
                {
                    threadTx.Set(impl);
                }

                void TransactionsImpl::ResetCurrent()
                {
                    threadTx.Remove();
                }

                int32_t TransactionsImpl::TxCommit(int32_t txId)
                {
                    TxEndRequest req(txId, true);

                    Response rsp;

                    SendTxMessage(req, rsp);

                    return rsp.GetStatus();
                }

                int32_t TransactionsImpl::TxRollback(int32_t txId)
                {
                    TxEndRequest req(txId, false);

                    Response rsp;

                    SendTxMessage(req, rsp);

                    return rsp.GetStatus();
                }

                int32_t TransactionsImpl::TxClose(int32_t txId)
                {
                    return TxRollback(txId);
                }

                void TransactionImpl::Commit()
                {
                    ThreadCheck();

                    txs.TxCommit(txId);

                    ThreadEnd();
                }

                void TransactionImpl::Rollback()
                {
                    ThreadCheck();

                    txs.TxRollback(txId);

                    ThreadEnd();
                }

                void TransactionImpl::Close()
                {
                    ThreadCheck();

                    if (IsClosed())
                    {
                        return;
                    }

                    txs.TxClose(txId);

                    ThreadEnd();
                }

                void TransactionImpl::SetClosed()
                {
                    closed = true;
                }

                void TransactionImpl::ThreadEnd()
                {
                    this->SetClosed();

                    txs.ResetCurrent();
                }

                void TransactionImpl::ThreadCheck()
                {
                    SP_TransactionImpl tx = txs.GetCurrent();

                    TransactionImpl* ptr = tx.Get();

                    if (!ptr)
                        throw IgniteError(IgniteError::IGNITE_ERR_TX_THIS_THREAD, TX_ALREADY_CLOSED);

                    if (ptr->TxId() != this->TxId())
                        throw IgniteError(IgniteError::IGNITE_ERR_TX_THIS_THREAD, TX_DIFFERENT_THREAD);
                }
            }
        }
    }
}
