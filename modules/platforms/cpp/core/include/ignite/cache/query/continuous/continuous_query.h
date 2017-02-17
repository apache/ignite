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
 * Declares ignite::cache::query::continuous::ContinuousQuery class.
 */

#ifndef _IGNITE_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY
#define _IGNITE_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY

#include <ignite/impl/cache/query/continuous/continuous_query_impl.h>
#include <ignite/cache/event/cache_entry_event_listener.h>

namespace ignite
{
    namespace cache
    {
        // Forward-declaration.
        template<typename K, typename V>
        class IGNITE_IMPORT_EXPORT Cache;

        namespace query
        {
            namespace continuous
            {
                /**
                 * Continuous query.
                 *
                 * Continuous queries allow to register a remote and a listener
                 * for cache update events. On any update to the related cache
                 * an event is sent to the node that has executed the query and
                 * listener is notified on that node.
                 *
                 * Continuous query can either be executed on the whole topology
                 * or only on local node.
                 *
                 * To execute the query over the cache use method
                 * ignite::cache::Cache::QueryContinuous().
                 */
                template<typename K, typename V>
                class ContinuousQuery
                {
                    friend class Cache<K, V>;
                public:

                    /**
                     * Default value for the buffer size.
                     */
                    enum { DEFAULT_BUFFER_SIZE = 1 };

                    /**
                     * Default value for the time interval.
                     */
                    enum { DEFAULT_TIME_INTERVAL = 0 };

                    /**
                     * Destructor.
                     */
                    ~ContinuousQuery()
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     *
                     * @param lsnr Event listener. Invoked on the node where
                     *     continuous query execution has been started.
                     */
                    ContinuousQuery(Reference<event::CacheEntryEventListener<K, V> > lsnr) :
                        impl(new impl::cache::query::continuous::ContinuousQueryImpl<K, V>(lsnr))
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     *
                     * @param lsnr Event listener Invoked on the node where
                     *     continuous query execution has been started.
                     * @param loc Whether query should be executed locally.
                     */
                    ContinuousQuery(Reference<event::CacheEntryEventListener<K, V> > lsnr, bool loc) :
                        impl(new impl::cache::query::continuous::ContinuousQueryImpl<K, V>(lsnr, loc))
                    {
                        // No-op.
                    }

                    /**
                     * Set local flag.
                     *
                     * @param val Value of the flag. If true, query will be
                     *     executed only on local node, so only local entries
                     *     will be returned as query result.
                     */
                    void SetLocal(bool val)
                    {
                        impl.Get()->SetLocal(val);
                    }

                    /**
                     * Get local flag.
                     *
                     * @return Value of the flag. If true, query will be
                     *     executed only on local node, so only local entries
                     *     will be returned as query result.
                     */
                    bool GetLocal() const
                    {
                        return impl.Get()->GetLocal();
                    }

                    /**
                     * Set buffer size.
                     *
                     * When a cache update happens, entry is first
                     * put into a buffer. Entries from buffer will be sent to
                     * the master node only if the buffer is full or time
                     * provided via timeInterval is exceeded.
                     *
                     * @param val Buffer size.
                     */
                    void SetBufferSize(int32_t val)
                    {
                        impl.Get()->SetBufferSize(val);
                    }

                    /**
                     * Get buffer size.
                     *
                     * When a cache update happens, entry is first
                     * put into a buffer. Entries from buffer will be sent to
                     * the master node only if the buffer is full or time
                     * provided via timeInterval is exceeded.
                     *
                     * @return Buffer size.
                     */
                    int32_t GetBufferSize() const
                    {
                        return impl.Get()->GetBufferSize();
                    }

                    /**
                     * Set time interval.
                     *
                     * When a cache update happens, entry is first put into
                     * a buffer. Entries from buffer are sent to the master node
                     * only if the buffer is full (its size can be changed via
                     * SetBufferSize) or time provided via this method is
                     * exceeded.
                     *
                     * Default value is DEFAULT_TIME_INTERVAL, i.e. 0, which
                     * means that time check is disabled and entries will be
                     * sent only when buffer is full.
                     *
                     * @param val Time interval in miliseconds.
                     */
                    void SetTimeInterval(int64_t val)
                    {
                        impl.Get()->SetTimeInterval(val);
                    }

                    /**
                     * Get time interval.
                     *
                     * When a cache update happens, entry is first put into
                     * a buffer. Entries from buffer are sent to the master node
                     * only if the buffer is full (its size can be changed via
                     * SetBufferSize) or time provided via SetTimeInterval
                     * method is exceeded.
                     *
                     * Default value is DEFAULT_TIME_INTERVAL, i.e. 0, which
                     * means that time check is disabled and entries will be
                     * sent only when buffer is full.
                     *
                     * @return Time interval.
                     */
                    int64_t GetTimeInterval() const
                    {
                        return impl.Get()->GetTimeInterval();
                    }

                    /**
                     * Set cache entry event listener.
                     *
                     * @param val Cache entry event listener. Invoked on the
                     *     node where continuous query execution has been
                     *     started.
                     */
                    void SetListener(Reference<event::CacheEntryEventListener<K, V> > lsnr)
                    {
                        impl.Get()->SetListener(lsnr);
                    }

                    /**
                     * Get cache entry event listener.
                     *
                     * @return Cache entry event listener.
                     */
                    const event::CacheEntryEventListener<K, V>& GetListener() const
                    {
                        return impl.Get()->GetListener();
                    }

                    /**
                     * Get cache entry event listener.
                     *
                     * @return Cache entry event listener.
                     */
                    event::CacheEntryEventListener<K, V>& GetListener()
                    {
                        return impl.Get()->GetListener();
                    }

                private:
                    /** Implementation. */
                    common::concurrent::SharedPointer<impl::cache::query::continuous::ContinuousQueryImpl<K, V> > impl;
                };
            }
        }
    }
}

#endif //_IGNITE_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY