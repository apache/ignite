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

using namespace ignite::common::java;

namespace ignite 
{
    namespace impl
    {
        namespace transactions
        {
            TransactionsImpl::TransactionsImpl(IgniteEnvSharedPtr env, jobject javaRef) :
                env(env),
                javaRef(javaRef)
            {
                // No-op.
            }

            TransactionsImpl::~TransactionsImpl()
            {
                JniContext::Release(javaRef);
            }

            int64_t TransactionsImpl::TxStart(int concurrency, int isolation,
                int64_t timeout, int32_t txSize, IgniteError& err)
            {
                using impl::transactions::TransactionsImpl;

                JniErrorInfo jniErr;

                int64_t id = env.Get()->Context()->TransactionsStart(javaRef,
                    concurrency, isolation, timeout, txSize, &jniErr);

                if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                return id;
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxCommit(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int32_t state = env.Get()->Context()->TransactionsCommit(javaRef, id, &jniErr);

                if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                return ToTransactionState(state);
            }

            TransactionsImpl::TransactionState TransactionsImpl::ToTransactionState(int state)
            {
                using namespace ignite::transactions;
                switch (state)
                {
                    case IGNITE_TX_STATE_ACTIVE:
                    case IGNITE_TX_STATE_PREPARING:
                    case IGNITE_TX_STATE_PREPARED:
                    case IGNITE_TX_STATE_MARKED_ROLLBACK:
                    case IGNITE_TX_STATE_COMMITTING:
                    case IGNITE_TX_STATE_COMMITTED:
                    case IGNITE_TX_STATE_ROLLING_BACK:
                    case IGNITE_TX_STATE_ROLLED_BACK:
                    case IGNITE_TX_STATE_UNKNOWN:
                        return static_cast<TransactionState>(state);

                    default:
                        return IGNITE_TX_STATE_UNKNOWN;
                }
            }
        }
    }
}

