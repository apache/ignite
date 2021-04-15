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
#include "impl/transactions/transaction_impl.h"
#include "impl/transactions/transactions_impl.h"
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
                template<typename ReqT, typename RspT>
                void TransactionImpl::SendTxMessage(const ReqT& req, RspT& rsp)
                {
                    channel.Get()->SyncMessage(req, rsp, static_cast<int32_t>(timeout / 1000) + ioTimeout);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_TX, rsp.GetError().c_str());
                }

                SP_TransactionImpl TransactionImpl::Create(
                    TransactionsImpl& txs,
                    SP_DataRouter& router,
                    TransactionConcurrency::Type concurrency,
                    TransactionIsolation::Type isolation,
                    int64_t timeout,
                    int32_t txSize,
                    SharedPointer<common::FixedSizeArray<char> > label)
                {
                    SP_TransactionImpl tx = txs.GetCurrent();

                    TransactionImpl* ptr = tx.Get();

                    if (ptr && !ptr->IsClosed())
                        throw IgniteError(IgniteError::IGNITE_ERR_TX_THIS_THREAD, TX_ALREADY_STARTED);

                    TxStartRequest req(concurrency, isolation, timeout, label);

                    Int32Response rsp;

                    SP_DataChannel channel = router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_TX, rsp.GetError().c_str());

                    int32_t curTxId = rsp.GetValue();

                    tx = SP_TransactionImpl(new TransactionImpl(txs, channel, curTxId, concurrency,
                        isolation, timeout, router.Get()->GetIoTimeout(), txSize));

                    txs.SetCurrent(tx);

                    return tx;
                }

                bool TransactionImpl::IsClosed() const
                {
                    return closed;
                }

                void TransactionImpl::Commit()
                {
                    ThreadCheck();

                    TxEndRequest req(txId, true);

                    Response rsp;

                    SendTxMessage(req, rsp);

                    ThreadEnd();
                }

                void TransactionImpl::Rollback()
                {
                    ThreadCheck();

                    TxEndRequest req(txId, false);

                    Response rsp;

                    SendTxMessage(req, rsp);

                    ThreadEnd();
                }

                void TransactionImpl::Close()
                {
                    ThreadCheck();

                    if (IsClosed())
                    {
                        return;
                    }

                    Rollback();

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
