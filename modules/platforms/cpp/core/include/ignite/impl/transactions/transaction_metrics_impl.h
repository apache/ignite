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

#ifndef _IGNITE_IMPL_TRANSACTIONS_TRANSACTION_METRICS_IMPL
#define _IGNITE_IMPL_TRANSACTIONS_TRANSACTION_METRICS_IMPL

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>
#include <ignite/common/java.h>

namespace ignite
{
    namespace impl
    {
        namespace transactions
        {
            /**
             * Transaction metrics.
             */
            class IGNITE_IMPORT_EXPORT TransactionMetricsImpl
            {
            public:
                /**
                 * Constructor.
                 */
                TransactionMetricsImpl();

            private:
            };
        }
    }
}

#endif //_IGNITE_IMPL_TRANSACTIONS_TRANSACTION_METRICS_IMPL