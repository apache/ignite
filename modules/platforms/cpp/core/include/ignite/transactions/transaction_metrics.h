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

/**
 * @file
 * Declares ignite::transactions::TransactionMetrics class.
 */

#ifndef _IGNITE_TRANSACTIONS_TRANSACTION_METRICS
#define _IGNITE_TRANSACTIONS_TRANSACTION_METRICS

#include <ignite/common/concurrent.h>
#include <ignite/common/java.h>

#include "ignite/impl/transactions/transaction_metrics_impl.h"

namespace ignite
{
    namespace transactions
    {
        /**
         * Transaction metrics.
         */
        class IGNITE_IMPORT_EXPORT TransactionMetrics
        {
        public:
            /**
             * Constructor.
             */
            TransactionMetrics(impl::transactions::TransactionMetricsImpl* impl) :
                impl(impl)
            {
                //No-op.
            }

        private:
            /** Implementation delegate. */
            common::concurrent::SharedPointer<impl::transactions::TransactionMetricsImpl> impl;
        };
    }
}

#endif //_IGNITE_TRANSACTIONS_TRANSACTION_METRICS