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
                /** Get metrics. */
                OP_METRICS = 2,
                /** Start tx. */
                OP_START = 3,
                /** Commit. */
                OP_COMMIT = 4,
                /** Rollback. */
                OP_ROLLBACK = 5,
                /** Close tx. */
                OP_CLOSE = 6,
                /** Get tx state. */
                OP_STATE = 7,
                /** Set rollback-only mode. */
                OP_SET_ROLLBACK_ONLY = 8,
                /** Reset metrics. */
                OP_RESET_METRICS = 11,
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

            /*
             * Input operation for starting a transaction.
             */
            class InTransactionStartOperation : public InputOperation
            {
            public:
                /**
                * Constructor.
                *
                * @param concurrency Concurrency.
                * @param isolation Isolation.
                * @param timeout Timeout in milliseconds. Zero if for infinite timeout.
                * @param txSize Number of entries participating in transaction (may be approximate).
                */
                InTransactionStartOperation(int concurrency, int isolation, int64_t timeout, int32_t txSize) :
                    concurrency(concurrency), isolation(isolation), timeout(timeout), txSize(txSize)
                {
                    // No-op.
                }

                virtual void ProcessInput(binary::BinaryWriterImpl& writer)
                {                        
                    writer.WriteInt32(concurrency);
                    writer.WriteInt32(isolation);
                    writer.WriteInt64(timeout);
                    writer.WriteInt32(txSize);
                }
            private:
                int concurrency; 
                
                int isolation;
                    
                int64_t timeout;
                
                int32_t txSize;

                IGNITE_NO_COPY_ASSIGNMENT(InTransactionStartOperation)
            };

            /**
            * Output operation for starting a transaction.
            */
            class OutTransactionStartOperation : public OutputOperation
            {
            public:
                /**
                * Constructor.
                */
                OutTransactionStartOperation(): val(0)
                {
                    // No-op.
                }

                virtual void ProcessOutput(binary::BinaryReaderImpl& reader) 
                {
                    val = reader.ReadInt64();
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
                int64_t Get()
                {
                    return val;
                }

            private:
                /** Value */
                int64_t val;

                IGNITE_NO_COPY_ASSIGNMENT(OutTransactionStartOperation)
            };


            int64_t TransactionsImpl::TxStart(int concurrency, int isolation,
                int64_t timeout, int32_t txSize, IgniteError& err)
            {
                InTransactionStartOperation inOp(concurrency, isolation, timeout, txSize);
                OutTransactionStartOperation outOp;

                OutInOp(OP_START, inOp, outOp, &err);

                return outOp.Get();
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxCommit(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int state = static_cast<int>(OutInOpLong(OP_COMMIT, id, &err));

                return ToTransactionState(state);
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxRollback(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int state = static_cast<int>(OutInOpLong(OP_ROLLBACK, id, &err));

                return ToTransactionState(state);
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxClose(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int state = static_cast<int>(OutInOpLong(OP_CLOSE, id, &err));

                return ToTransactionState(state);
            }

            bool TransactionsImpl::TxSetRollbackOnly(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                bool rollbackOnly = OutInOpLong(OP_SET_ROLLBACK_ONLY, id, &err) == 1;

                return rollbackOnly;
            }

            TransactionsImpl::TransactionState TransactionsImpl::TxState(int64_t id, IgniteError& err)
            {
                JniErrorInfo jniErr;

                int state = static_cast<int>(OutInOpLong(OP_STATE, id, &err));

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

