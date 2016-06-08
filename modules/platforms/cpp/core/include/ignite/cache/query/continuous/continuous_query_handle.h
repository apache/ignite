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
 * Declares ignite::cache::query::continuous::ContinuousQueryHandle class.
 */

#ifndef _IGNITE_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE
#define _IGNITE_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE

#include <stdint.h>
#include <string>

#include <ignite/impl/cache/query/continuous/continuous_query_handle_impl.h>

namespace ignite
{
    namespace cache
    {
        namespace query
        {
            namespace continuous
            {
                /**
                 * Continuous query handle.
                 */
                template<typename K, typename V>
                class ContinuousQueryHandle
                {
                public:
                    /**
                     * Default constructor.
                     */
                    ContinuousQueryHandle() :
                        impl()
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     *
                     * Internal method. Should not be used by user.
                     *
                     * @param impl Implementation.
                     */
                    ContinuousQueryHandle(impl::cache::query::continuous::ContinuousQueryHandleImpl* impl) :
                        impl(impl)
                    {
                        // No-op.
                    }

                    /**
                     * Gets the cursor for initial query.
                     * Can be called only once, throws exception on consequent calls.
                     *
                     * @return Initial query cursor.
                     */
                    QueryCursor<K, V> GetInitialQueryCursor()
                    {
                        return QueryCursor<K, V>();
                    }

                private:
                    /** Implementation delegate. */
                    ignite::common::concurrent::SharedPointer<impl::cache::query::continuous::ContinuousQueryHandleImpl> impl;
                };
            }
        }
    }
}

#endif //_IGNITE_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE