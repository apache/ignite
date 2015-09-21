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

#ifndef _IGNITE_CACHE_QUERY_CURSOR
#define _IGNITE_CACHE_QUERY_CURSOR

#include <vector>

#include <ignite/common/concurrent.h>

#include "ignite/cache/cache_entry.h"
#include "ignite/ignite_error.h"
#include "ignite/impl/cache/query/query_impl.h"
#include "ignite/impl/operations.h"

namespace ignite
{    
    namespace cache
    {
        namespace query
        {            
            /**
             * Query cursor.
             */
            template<typename K, typename V>
            class QueryCursor
            {
            public:
                /**
                 * Default constructor.
                 */
                QueryCursor() : impl(NULL)
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                QueryCursor(impl::cache::query::QueryCursorImpl* impl) : 
                    impl(ignite::common::concurrent::SharedPointer<impl::cache::query::QueryCursorImpl>(impl))
                {
                    // No-op.
                }
                
                /**
                 * Check whether next entry exists.
                 *
                 * @return True if next entry exists.
                 */
                bool HasNext()
                {
                    IgniteError err;

                    bool res = HasNext(err);

                    IgniteError::ThrowIfNeeded(err);

                    return res;
                }

                /**
                 * Check whether next entry exists.
                 *
                 * @param err Error.
                 * @return True if next entry exists.
                 */
                bool HasNext(IgniteError& err)
                {
                    impl::cache::query::QueryCursorImpl* impl0 = impl.Get();

                    if (impl0)
                        return impl0->HasNext(&err);
                    else
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, 
                            "Instance is not usable (did you check for error?).");

                        return false;
                    }
                }

                /**
                 * Get next entry.
                 *
                 * @return Next entry.
                 */
                CacheEntry<K, V> GetNext()
                {
                    IgniteError err;

                    CacheEntry<K, V> res = GetNext(err);

                    IgniteError::ThrowIfNeeded(err);

                    return res;                        
                }

                /**
                 * Get next entry.
                 *
                 * @param err Error.
                 * @return Next entry.
                 */
                CacheEntry<K, V> GetNext(IgniteError& err)
                {
                    impl::cache::query::QueryCursorImpl* impl0 = impl.Get();

                    if (impl0) {
                        impl::Out2Operation<K, V> outOp;

                        impl0->GetNext(outOp, &err);

                        if (err.GetCode() == IgniteError::IGNITE_SUCCESS) 
                        {
                            K& key = outOp.Get1();
                            V& val = outOp.Get2();

                            return CacheEntry<K, V>(key, val);
                        }
                        else 
                            return CacheEntry<K, V>();
                    }
                    else
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Instance is not usable (did you check for error?).");

                        return CacheEntry<K, V>();
                    }
                }

                /**
                 * Get all entries.
                 * 
                 * @param Vector where query entries will be stored.
                 */
                void GetAll(std::vector<CacheEntry<K, V>>& res)
                {
                    IgniteError err;

                    GetAll(res, err);

                    IgniteError::ThrowIfNeeded(err);
                }

                /**
                 * Get all entries.
                 * 
                 * @param Vector where query entries will be stored.
                 * @param err Error.                 
                 */
                void GetAll(std::vector<CacheEntry<K, V>>& res, IgniteError& err)
                {
                    impl::cache::query::QueryCursorImpl* impl0 = impl.Get();

                    if (impl0) {
                        impl::OutQueryGetAllOperation<K, V> outOp(&res);

                        impl0->GetAll(outOp, &err);
                    }
                    else
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Instance is not usable (did you check for error?).");
                }

            private:
                /** Implementation delegate. */
                ignite::common::concurrent::SharedPointer<impl::cache::query::QueryCursorImpl> impl;
            };
        }
    }    
}

#endif