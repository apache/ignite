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
                    public:
                        /**
                         * Default constructor.
                         * 
                         * @param env Environment.
                         * @param javaRef Java reference.
                         */
                        ContinuousQueryHandleImpl(ignite::common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef);

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

                    private:
                        /** Environment. */
                        ignite::common::concurrent::SharedPointer<impl::IgniteEnvironment> env;

                        /** Handle to Java object. */
                        jobject javaRef;

                        /** Cursor extracted. */
                        bool extracted;
                    };
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_IMPL