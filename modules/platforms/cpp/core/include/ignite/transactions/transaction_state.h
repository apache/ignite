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
 * Declares ignite::transactions::TransactionState enumeration.
 */

#ifndef _IGNITE_TRANSACTIONS_TRANSACTION_STATE
#define _IGNITE_TRANSACTIONS_TRANSACTION_STATE

namespace ignite 
{
    namespace transactions
    {
        /**
         * Cache transaction state.
         */
        enum TransactionState
        {
            /** Transaction started. */
            IGNITE_TX_STATE_ACTIVE,

            /** Transaction validating. */
            IGNITE_TX_STATE_PREPARING,

            /** Transaction validation succeeded. */
            IGNITE_TX_STATE_PREPARED,

            /** Transaction is marked for rollback. */
            IGNITE_TX_STATE_MARKED_ROLLBACK,

            /** Transaction commit started (validating finished). */
            IGNITE_TX_STATE_COMMITTING,

            /** Transaction commit succeeded. */
            IGNITE_TX_STATE_COMMITTED,

            /** Transaction rollback started (validation failed). */
            IGNITE_TX_STATE_ROLLING_BACK,

            /** Transaction rollback succeeded. */
            IGNITE_TX_STATE_ROLLED_BACK,

            /** Transaction rollback failed or is otherwise unknown state. */
            IGNITE_TX_STATE_UNKNOWN
        };
    }
}

#endif //_IGNITE_TRANSACTIONS_TRANSACTION_STATE