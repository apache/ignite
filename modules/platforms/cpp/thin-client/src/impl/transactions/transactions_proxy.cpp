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

#include "ignite/impl/thin/transactions/transactions_proxy.h"
#include "impl/transactions/transactions_impl.h"

using namespace ignite::impl::thin;
using namespace transactions;

namespace
{
    using namespace ignite::common::concurrent;

    TransactionsImpl& GetTxsImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<TransactionsImpl*>(ptr.Get());
    }

    TransactionImpl& GetTxImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<TransactionImpl*>(ptr.Get());
    }
}

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace transactions
            {
                #define DEFAULT_CONCURRENCY TransactionConcurrency::PESSIMISTIC
                #define DEFAULT_ISOLATION TransactionIsolation::READ_COMMITTED
                #define DEFAULT_TIMEOUT 0
                #define DEFAULT_TX_SIZE 0

                TransactionProxy TransactionsProxy::txStart()
                {
                    return TransactionProxy(GetTxsImpl(impl).TxStart(DEFAULT_CONCURRENCY, DEFAULT_ISOLATION, DEFAULT_TIMEOUT, DEFAULT_TX_SIZE));
                }

                TransactionProxy TransactionsProxy::txStart(TransactionConcurrency::Type concurrency, TransactionIsolation::Type isolation)
                {
                    return TransactionProxy(GetTxsImpl(impl).TxStart(concurrency, isolation, DEFAULT_TIMEOUT, DEFAULT_TX_SIZE));
                }

                TransactionProxy TransactionsProxy::txStart(TransactionConcurrency::Type concurrency, TransactionIsolation::Type isolation, int64_t timeout, int32_t txSize)
                {
                    return TransactionProxy(GetTxsImpl(impl).TxStart(concurrency, isolation, timeout, txSize));
                }

                void TransactionProxy::commit()
                {
                    GetTxImpl(impl).Commit();
                }

                void TransactionProxy::rollback()
                {
                    GetTxImpl(impl).Rollback();
                }

                void TransactionProxy::close()
                {
                    try
                    {
                        GetTxImpl(impl).Close();
                    }
                    catch (...)
                    {
                    }
                }
            }
        }
    }
}
