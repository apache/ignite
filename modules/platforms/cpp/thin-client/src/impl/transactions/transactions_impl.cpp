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

#include "impl/transactions/transactions_impl.h"

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

                SharedPointer<TransactionImpl> TransactionsImpl::TxStart(
                        TransactionConcurrency::Type concurrency,
                        TransactionIsolation::Type isolation,
                        int64_t timeout,
                        int32_t txSize,
                        SharedPointer<common::FixedSizeArray<char> > label)
                {
                    SP_TransactionImpl tx = TransactionImpl::Create(*this, router, concurrency, isolation, timeout, txSize, label);

                    return tx;
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
            }
        }
    }
}
