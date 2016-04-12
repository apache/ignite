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

using namespace ignite::common::java;
using namespace ignite::transactions;

namespace ignite 
{
    namespace impl
    {
        namespace transactions
        {
            TransactionImpl::TxImplSharedPtrTli TransactionImpl::threadTx;

            TransactionImpl::TransactionImpl(int64_t id, int concurrency,
                int isolation, int64_t timeout, int32_t txSize) :
                id(id),
                concurrency(concurrency),
                isolation(isolation),
                timeout(timeout),
                txSize(txSize),
                state(IGNITE_TX_STATE_UNKNOWN),
                closed(false)
            {
                // No-op.
            }

            TransactionImpl::TxImplSharedPtr TransactionImpl::Create(int64_t id,
                int concurrency, int isolation, int64_t timeout, int32_t txSize)
            {
                TxImplSharedPtr tx = TxImplSharedPtr(new TransactionImpl(id,
                    concurrency, isolation, timeout, txSize));

                threadTx.Set(tx);

                return tx;
            }

            TransactionImpl::~TransactionImpl()
            {
                // No-op.
            }

            TransactionImpl::TxImplSharedPtr TransactionImpl::GetCurrent()
            {
                TxImplSharedPtr tx = threadTx.Get();

                if (tx.Get()->IsClosed())
                {
                    tx = TxImplSharedPtr();

                    threadTx.Set(tx);
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

                //state;

                closed = true;
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

