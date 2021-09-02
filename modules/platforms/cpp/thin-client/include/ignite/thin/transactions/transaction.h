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

#ifndef _IGNITE_THIN_TRANSACTIONS_CLIENT_TRANSACTION
#define _IGNITE_THIN_TRANSACTIONS_CLIENT_TRANSACTION

#include <ignite/impl/thin/transactions/transactions_proxy.h>

namespace ignite
{
    namespace thin
    {
        namespace transactions
        {
            /**
             * Transaction client.
             *
             * Implements main transactionsl API.
             *
             * This class is implemented as a reference to an implementation so copying of this class instance will only
             * create another reference to the same underlying object. Underlying object will be released automatically
             * once all the instances are destructed.
             */
            class ClientTransaction {

            public:
                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                ClientTransaction(ignite::impl::thin::transactions::TransactionProxy impl) :
                    proxy(impl)
                {
                    // No-op.
                }

                /**
                 * Default constructor.
                 */
                ClientTransaction()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~ClientTransaction()
                {
                    // No-op.
                }

                /**
                 * Commits this transaction.
                 */
                void Commit()
                {
                    proxy.commit();
                }

                /**
                 * Rolls back this transaction.
                 */
                void Rollback()
                {
                    proxy.rollback();
                }

                /**
                 * Ends the transaction. Transaction will be rolled back if it has not been committed.
                 */
                void Close()
                {
                    proxy.close();
                }

            private:
                /** Implementation. */
                ignite::impl::thin::transactions::TransactionProxy proxy;
            };
        }
    }
}

#endif // _IGNITE_THIN_TRANSACTIONS_CLIENT_TRANSACTION
