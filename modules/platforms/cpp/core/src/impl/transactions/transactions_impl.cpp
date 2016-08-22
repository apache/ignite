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

using namespace ignite::jni::java;
using namespace ignite::java;
using namespace ignite::transactions;

namespace ignite 
{
    namespace impl
    {
        namespace transactions
        {
            /**
             * Transaction opertion.
             */
            enum Operation
            {
                /** Get metrics operation. */
                OP_METRICS = 2
            };

            TransactionsImpl::TransactionsImpl(SP_IgniteEnvironment env, jobject javaRef) :
                InteropTarget(env, javaRef)
            {
                // No-op.
            }

            TransactionsImpl::~TransactionsImpl()
            {
                // No-op.
            }

            int64_t TransactionsImpl::TxStart(int concurrency, int isolation,
                int64_t timeout, int32_t txSize, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int64_t id = GetEnvironment().Context()->TransactionsStart(GetTarget(),
                    concurrency, isolation, timeout, txSize, &jniErr);

                if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                return id;
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxCommit(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int state = GetEnvironment().Context()->TransactionsCommit(GetTarget(), id, &jniErr);

                if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                return ToTransactionState(state);
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxRollback(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int state = GetEnvironment().Context()->TransactionsRollback(GetTarget(), id, &jniErr);

                if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                return ToTransactionState(state);
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxClose(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int state = GetEnvironment().Context()->TransactionsClose(GetTarget(), id, &jniErr);

                if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                return ToTransactionState(state);
            }

            bool TransactionsImpl::TxSetRollbackOnly(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                bool rollbackOnly = GetEnvironment().Context()->TransactionsSetRollbackOnly(GetTarget(), id, &jniErr);

                if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                return rollbackOnly;
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxState(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int state = GetEnvironment().Context()->TransactionsState(GetTarget(), id, &jniErr);

                if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                return ToTransactionState(state);
            }

            /**
             * Output operation for Metrics.
             */
            class OutTransactionMetricsOperation : public OutputOperation
            {
            public:
                /**
                 * Constructor.
                 */
                OutTransactionMetricsOperation()
                {
                    // No-op.
                }

                virtual void ProcessOutput(ignite::impl::binary::BinaryReaderImpl& reader)
                {
                    Timestamp commitTime = reader.ReadTopObject<Timestamp>();
                    Timestamp rollbackTime = reader.ReadTopObject<Timestamp>();
                    int32_t commits = reader.ReadInt32();
                    int32_t rollbacks = reader.ReadInt32();

                    val = TransactionMetrics(commitTime, rollbackTime, commits, rollbacks);
                }

                virtual void SetNull()
                {
                    // No-op.
                }

                /**
                 * Get value.
                 *
                 * @return Value.
                 */
                TransactionMetrics& Get()
                {
                    return val;
                }

            private:
                /** Value */
                TransactionMetrics val;

                IGNITE_NO_COPY_ASSIGNMENT(OutTransactionMetricsOperation)
            };

            TransactionMetrics TransactionsImpl::GetMetrics(IgniteError& err)
            {
                OutTransactionMetricsOperation op;

                InOp(OP_METRICS, op, &err);

                if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                    return op.Get();

                return ignite::transactions::TransactionMetrics();
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

