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
                     * Can be called only once, throws IgniteError on consequent
                     * calls.
                     *
                     * @return Initial query cursor.
                     */
                    QueryCursor<K, V> GetInitialQueryCursor()
                    {
                        IgniteError err;

                        QueryCursor<K, V> res = GetInitialQueryCursor(err);

                        IgniteError::ThrowIfNeeded(err);

                        return res;
                    }

                    /**
                     * Gets the cursor for initial query.
                     * Can be called only once, results in error on consequent
                     * calls.
                     *
                     * @param err Error.
                     * @return Initial query cursor.
                     */
                    QueryCursor<K, V> GetInitialQueryCursor(IgniteError& err)
                    {
                        impl::cache::query::continuous::ContinuousQueryHandleImpl* impl0 = impl.Get();

                        if (impl0)
                            return QueryCursor<K, V>(impl0->GetInitialQueryCursor(err));
                        else
                        {
                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                                "Instance is not usable (did you check for error?).");

                            return QueryCursor<K, V>();
                        }
                    }

                    /**
                     * Check if the instance is valid.
                     *
                     * Invalid instance can be returned if some of the previous
                     * operations have resulted in a failure. For example invalid
                     * instance can be returned by not-throwing version of method
                     * in case of error. Invalid instances also often can be
                     * created using default constructor.
                     *
                     * @return True if the instance is valid and can be used.
                     */
                    bool IsValid() const
                    {
                        return impl.IsValid();
                    }

                private:
                    typedef impl::cache::query::continuous::ContinuousQueryHandleImpl ContinuousQueryHandleImpl;

                    /** Implementation delegate. */
                    common::concurrent::SharedPointer<ContinuousQueryHandleImpl> impl;
                };
            }
        }
    }
}

#endif //_IGNITE_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE