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
 * Declares ignite::thin::cache::query::QueryCursor class template.
 */

#ifndef _IGNITE_THIN_CACHE_QUERY_QUERY_CURSOR
#define _IGNITE_THIN_CACHE_QUERY_QUERY_CURSOR

#include <vector>

#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

#include <ignite/thin/cache/cache_entry.h>

#include <ignite/impl/thin/readable.h>
#include <ignite/impl/thin/cache/query/query_cursor_proxy.h>

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            namespace query
            {
                /**
                 * Query cursor class template.
                 *
                 * Both key and value types should be default-constructable, copy-constructable
                 * and assignable. Also BinaryType class template should be specialized for both
                 * types.
                 *
                 * This class is implemented as a reference to an implementation so copying
                 * of this class instance will only create another reference to the same
                 * underlying object. Underlying object will be released automatically once all
                 * the instances are destructed.
                 */
                template<typename K, typename V>
                class QueryCursor
                {
                public:
                    /**
                     * Default constructor.
                     */
                    QueryCursor()
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     *
                     * @param impl Implementation.
                     */
                    explicit QueryCursor(const impl::thin::cache::query::QueryCursorProxy& impl) :
                        impl(impl)
                    {
                        // No-op.
                    }

                    /**
                     * Check whether next entry exists.
                     *
                     * @return True if next entry exists.
                     *
                     * @throw IgniteError class instance in case of failure.
                     */
                    bool HasNext() const
                    {
                        return impl.HasNext();
                    }

                    /**
                     * Get next entry.
                     *
                     * @return Next entry.
                     *
                     * @throw IgniteError class instance in case of failure.
                     */
                    CacheEntry<K, V> GetNext()
                    {
                        CacheEntry<K, V> entry;
                        impl::thin::ReadableImpl< CacheEntry<K, V> > readable(entry);

                        impl.GetNext(readable);

                        return entry;
                    }

                    /**
                     * Get all entries.
                     *
                     * @param res Vector where query entries will be stored.
                     *
                     * @throw IgniteError class instance in case of failure.
                     */
                    void GetAll(std::vector<CacheEntry<K, V> >& res)
                    {
                        res.clear();
                        GetAll(std::inserter(res, res.end()));
                    }

                    /**
                     * Get all entries.
                     *
                     * @param iter Output iterator.
                     *
                     * @throw IgniteError class instance in case of failure.
                     */
                    template<typename OutIter>
                    void GetAll(OutIter iter)
                    {
                        impl::thin::ReadableContainerImpl< CacheEntry<K, V>, OutIter > collection(iter);

                        while (HasNext())
                        {
                            *iter = GetNext();
                            ++iter;
                        }
                    }

                private:
                    /** Implementation delegate. */
                    impl::thin::cache::query::QueryCursorProxy impl;
                };
            }
        }


    }
}

#endif //_IGNITE_THIN_CACHE_QUERY_QUERY_CURSOR
