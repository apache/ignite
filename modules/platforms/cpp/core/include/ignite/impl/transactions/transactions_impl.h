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

#ifndef _IGNITE_TRANSACTIONS_IMPL
#define _IGNITE_TRANSACTIONS_IMPL

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
             * Transactions implementation.
             */
            class IGNITE_FRIEND_EXPORT TransactionsImpl
            {
                friend class Ignite;

                /** Synonym for Ignite Environment Shared Pointer. */
                typedef ignite::common::concurrent::SharedPointer<ignite::impl::IgniteEnvironment> IgniteEnvSharedPtr;
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                TransactionsImpl(IgniteEnvSharedPtr env, jobject javaRef);

                /**
                 * Destructor.
                 */
                ~TransactionsImpl();

            private:
                /** Environment. */
                IgniteEnvSharedPtr env;

                /** Native Java counterpart. */
                jobject javaRef;

                IGNITE_NO_COPY_ASSIGNMENT(TransactionsImpl)
            };
        }
    }
}

#endif