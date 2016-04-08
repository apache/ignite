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

#ifndef _IGNITE_TRANSACTION_IMPL
#define _IGNITE_TRANSACTION_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/common/java.h>

#include "ignite/impl/cache/cache_impl.h"
#include "ignite/impl/ignite_environment.h"
#include "ignite/impl/utils.h"

namespace ignite 
{
    namespace impl
    {
        namespace transactions
        {
            /**
             * Transaction implementation.
             */
            class IGNITE_FRIEND_EXPORT TransactionImpl
            {
            public:
                typedef ignite::common::concurrent::SharedPointer<TransactionImpl> TxImplSharedPtr;

                /**
                 * Destructor.
                 */
                ~TransactionImpl();

                /**
                 * Factory method. Create new instance of the class.
                 *
                 * @param id Transaction id.
                 * @param concurrency Concurrency.
                 * @param isolation Isolation.
                 * @param timeout Timeout in milliseconds.
                 * @param txSize Transaction size.
                 *
                 * @return Shared pointer to new instance.
                 */
                static TxImplSharedPtr Create(int64_t id, int concurrency,
                    int isolation, int64_t timeout, int32_t txSize);

                /**
                 * Get active transaction for the current thread.
                 *
                 * @return Active transaction implementation for current thread
                 * or null pointer if there is no active transaction for
                 * the thread.
                 */
                static TxImplSharedPtr GetCurrent();

            private:
                /**
                 * Constructor.
                 *
                 * @param id Transaction id.
                 * @param concurrency Concurrency.
                 * @param isolation Isolation.
                 * @param timeout Timeout in milliseconds.
                 * @param txSize Transaction size.
                 */
                TransactionImpl(int64_t id, int concurrency, int isolation,
                    int64_t timeout, int32_t txSize);

                typedef ignite::common::concurrent::ThreadLocalInstance<TxImplSharedPtr> TxImplSharedPtrTli;

                /** Thread local instance of the transaction. */
                static TxImplSharedPtrTli threadTx;

                /** Transaction ID. */
                int64_t id;

                /** Concurrency. */
                int concurrency;

                /** Isolation. */
                int isolation;

                /** Timeout in milliseconds. */
                int64_t timeout;

                /** Transaction size. */
                int32_t txSize;

                IGNITE_NO_COPY_ASSIGNMENT(TransactionImpl)
            };
        }
    }
}

#endif