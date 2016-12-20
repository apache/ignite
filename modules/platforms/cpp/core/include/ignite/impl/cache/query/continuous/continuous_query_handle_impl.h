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
 * Declares ignite::impl::cache::query::continuous::ContinuousQueryHandleImpl class.
 */

#ifndef _IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_IMPL
#define _IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_IMPL

#include "ignite/cache/query/query_cursor.h"
#include "ignite/impl/cache/query/continuous/continuous_query_impl.h"

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                namespace continuous
                {
                    /**
                     * Continuous query handle implementation.
                     */
                    class IGNITE_IMPORT_EXPORT ContinuousQueryHandleImpl
                    {
                        typedef common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;
                        typedef common::concurrent::SharedPointer<ContinuousQueryImplBase> SP_ContinuousQueryImplBase;
                    public:
                        /**
                         * Default constructor.
                         * 
                         * @param env Environment.
                         * @param javaRef Java reference.
                         */
                        ContinuousQueryHandleImpl(SP_IgniteEnvironment env, int64_t handle, jobject javaRef);

                        /**
                         * Destructor.
                         */
                        ~ContinuousQueryHandleImpl();

                        /**
                         * Gets the cursor for initial query.
                         * Can be called only once, throws exception on consequent calls.
                         *
                         * @param err Error.
                         * @return Initial query cursor.
                         */
                        QueryCursorImpl* GetInitialQueryCursor(IgniteError& err);

                        /**
                         * Set query to keep pointer to.
                         *
                         * @param query Query.
                         */
                        void SetQuery(SP_ContinuousQueryImplBase query);

                    private:
                        /** Environment. */
                        SP_IgniteEnvironment env;

                        /** Local handle for handle registry. */
                        int64_t handle;

                        /** Handle to Java object. */
                        jobject javaRef;

                        /** Shared pointer to query. Kept for query to live long enough. */
                        SP_ContinuousQueryImplBase qry;

                        /** Mutex. */
                        common::concurrent::CriticalSection mutex;

                        /** Cursor extracted. */
                        bool extracted;
                    };
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_IMPL