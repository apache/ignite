/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
            struct Operation
            {
                enum Type
                {
                    /** Get metrics. */
                    METRICS = 2,

                    /** Start tx. */
                    START = 3,

                    /** Commit. */
                    COMMIT = 4,

                    /** Rollback. */
                    ROLLBACK = 5,

                    /** Close tx. */
                    CLOSE = 6,

                    /** Get tx state. */
                    STATE = 7,

                    /** Set rollback-only mode. */
                    SET_ROLLBACK_ONLY = 8,

                    /** Reset metrics. */
                    RESET_METRICS = 11,
                };
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

                OutInOp(Operation::START, inOp, outOp, err);

                return outOp.Get();
            }

            TransactionState::Type TransactionsImpl::TxCommit(int64_t id, IgniteError& err)
            {
                int state = static_cast<int>(OutInOpLong(Operation::COMMIT, id, err));

                return ToTransactionState(state);
            }

            TransactionState::Type TransactionsImpl::TxRollback(int64_t id, IgniteError& err)
            {
                int state = static_cast<int>(OutInOpLong(Operation::ROLLBACK, id, err));

                return ToTransactionState(state);
            }

            TransactionState::Type TransactionsImpl::TxClose(int64_t id, IgniteError& err)
            {
                int state = static_cast<int>(OutInOpLong(Operation::CLOSE, id, err));

                return ToTransactionState(state);
            }

            bool TransactionsImpl::TxSetRollbackOnly(int64_t id, IgniteError& err)
            {
                bool rollbackOnly = OutInOpLong(Operation::SET_ROLLBACK_ONLY, id, err) == 1;

                return rollbackOnly;
            }

            TransactionState::Type TransactionsImpl::TxState(int64_t id, IgniteError& err)
            {
                int state = static_cast<int>(OutInOpLong(Operation::STATE, id, err));

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

                InOp(Operation::METRICS, op, err);

                if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                    return op.Get();

                return TransactionMetrics();
            }

            TransactionState::Type TransactionsImpl::ToTransactionState(int state)
            {
                using namespace ignite::transactions;
                switch (state)
                {
                    case TransactionState::ACTIVE:
                    case TransactionState::PREPARING:
                    case TransactionState::PREPARED:
                    case TransactionState::MARKED_ROLLBACK:
                    case TransactionState::COMMITTING:
                    case TransactionState::COMMITTED:
                    case TransactionState::ROLLING_BACK:
                    case TransactionState::ROLLED_BACK:
                    case TransactionState::UNKNOWN:
                        return static_cast<TransactionState::Type>(state);

                    default:
                        return TransactionState::UNKNOWN;
                }
            }
        }
    }
}

