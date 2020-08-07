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

#include <iostream>

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
                TransactionImpl::TL_SP_TransactionsImpl TransactionImpl::threadTx;

                TransactionsImpl::TransactionsImpl(const SP_DataRouter& router) :
                    router(router)
                {
                }

                template<typename ReqT, typename RspT>
                void TransactionsImpl::SyncMessage(const ReqT& req, RspT& rsp)
                {

                    std::cout << "router SyncMessage: " << router.Get() << std::endl;
                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                SharedPointer<TransactionImpl> TransactionsImpl::TxStart(
                        TransactionConcurrency::Type concurrency,
                        TransactionIsolation::Type isolation,
                        int64_t timeout,
                        int32_t txSize)
                {
                    //IgniteError err = IgniteError(); !!! ???

                    SharedPointer<TransactionImpl> tx = TransactionImpl::Create(this,
                        concurrency, isolation, timeout, txSize);

                    std::cout << "router TxStart " << router.Get() << std::endl;

                    std::cout << "router TxStart " << tx.Get()->txs.Get()->router.Get() << std::endl;

                    return tx;
                }

                SP_TransactionImpl TransactionImpl::Create(
                    SP_TransactionsImpl txs,
                    TransactionConcurrency::Type concurrency,
                    TransactionIsolation::Type isolation,
                    int64_t timeout,
                    int32_t txSize)
                {
                    TxStartRequest<RequestType::OP_TX_START> req(concurrency, isolation, timeout, txSize);

                    Int32Response rsp;

                    txs.Get()->SyncMessage(req, rsp);

                    int32_t txId = rsp.GetValue();

                    SP_TransactionImpl tx = SP_TransactionImpl(new TransactionImpl(txs, txId, concurrency, isolation, timeout, txSize));

                    std::cout << "router Create " << txs.Get()->router.Get() << std::endl;

                    threadTx.Set(tx);

                    return tx;
                }

                SP_TransactionImpl TransactionImpl::GetCurrent()
                {
                    SP_TransactionImpl tx = threadTx.Get();

                    TransactionImpl* ptr = tx.Get();

                    if (ptr && ptr->IsClosed())
                    {
                        tx = SP_TransactionImpl();

                        threadTx.Remove();
                    }
                    if (ptr)
                    std::cout << "router GetCurrent " << ptr->txs.Get()->router.Get() << std::endl;

                    return tx;
                }

                bool TransactionImpl::IsClosed() const
                {
                    return closed;
                }

                SP_TransactionImpl TransactionsImpl::GetCurrent()
                {
                    std::cout << "GetCurrent " << router.Get() <<  std::endl;

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
                    std::cout << "12" <<  std::endl;
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
                    common::concurrent::CsLockGuard guard(accessLock);

                    int32_t rsp = txs.Get()->TxCommit(txId);

                    if (rsp == ResponseStatus::SUCCESS)
                    {
                        closed = true;
                    }
                }

                void TransactionImpl::Rollback()
                {
                    common::concurrent::CsLockGuard guard(accessLock);

                    int32_t rsp = txs.Get()->TxRollback(txId);

                    if (rsp == ResponseStatus::SUCCESS)
                    {
                        closed = true;
                    }
                }

                void TransactionImpl::Close()
                {
                    common::concurrent::CsLockGuard guard(accessLock);

                    if (IsClosed())
                    {
                        return;
                    }

                    int32_t rsp = txs.Get()->TxClose(txId);

                    if (rsp == ResponseStatus::SUCCESS)
                    {
                        closed = true;
                    }
                }
            }
        }
    }
}
