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
 * Declares ignite::thin::cache::query::continuous::ContinuousQueryClient class.
 */

#ifndef _IGNITE_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_CLIENT
#define _IGNITE_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_CLIENT

#include <ignite/reference.h>

#include <ignite/thin/cache/event/cache_entry_event_listener.h>
#include <ignite/thin/cache/event/java_cache_entry_event_filter.h>

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            namespace query
            {
                namespace continuous
                {
                    /**
                     * Continuous query client.
                     *
                     * Continuous query client allow to register a listener for cache update events. On any update to
                     * the related cache an event is sent to the client that has executed the query and listener is
                     * notified on that client.
                     *
                     * Continuous query can either be executed on the whole topology or only on local node.
                     *
                     * To execute the query over the cache use method
                     * ignite::thin::cache::CacheClient::QueryContinuous().
                     */
                    template<typename K, typename V>
                    class ContinuousQueryClient
                    {
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
                        ~ContinuousQueryClient()
                        {
                            // No-op.
                        }

                        /**
                         * Constructor.
                         *
                         * @param lsnr Event listener. Invoked on the node where continuous query execution has been
                         * started.
                         */
                        explicit ContinuousQueryClient(Reference<event::CacheEntryEventListener<K, V> > lsnr) :
                            bufferSize(DEFAULT_BUFFER_SIZE),
                            timeInterval(DEFAULT_TIME_INTERVAL),
                            includeExpired(false),
                            listener(lsnr),
                            filter()
                        {
                            // No-op.
                        }

                        /**
                         * Set buffer size.
                         *
                         * When a cache update happens, entry is first put into a buffer. Entries from buffer will be
                         * sent to the master node only if the buffer is full or time provided via SetTimeInterval is
                         * exceeded.
                         *
                         * @param val Buffer size.
                         */
                        void SetBufferSize(int32_t val)
                        {
                            bufferSize = val;
                        }

                        /**
                         * Get buffer size.
                         *
                         * When a cache update happens, entry is first put into a buffer. Entries from buffer will be
                         * sent to the master node only if the buffer is full or time provided via SetTimeInterval is
                         * exceeded.
                         *
                         * @return Buffer size.
                         */
                        int32_t GetBufferSize() const
                        {
                            return bufferSize;
                        }

                        /**
                         * Set time interval.
                         *
                         * When a cache update happens, entry is first put into a buffer. Entries from buffer are sent
                         * to the master node only if the buffer is full (its size can be changed via SetBufferSize) or
                         * time provided via this method is exceeded.
                         *
                         * Default value is DEFAULT_TIME_INTERVAL, i.e. 0, which means that time check is disabled and
                         * entries will be sent only when buffer is full.
                         *
                         * @param val Time interval in miliseconds.
                         */
                        void SetTimeInterval(int64_t val)
                        {
                            timeInterval = val;
                        }

                        /**
                         * Get time interval.
                         *
                         * When a cache update happens, entry is first put into a buffer. Entries from buffer are sent
                         * to the master node only if the buffer is full (its size can be changed via SetBufferSize) or
                         * time provided via this method is exceeded.
                         *
                         * Default value is DEFAULT_TIME_INTERVAL, i.e. 0, which means that time check is disabled and
                         * entries will be sent only when buffer is full.
                         *
                         * @return Time interval.
                         */
                        int64_t GetTimeInterval() const
                        {
                            return timeInterval;
                        }

                        /**
                         * Sets a value indicating whether to notify about Expired events.
                         *
                         * If @c true, then the listener will get notifications about expired cache entries. Otherwise,
                         * only Created, Updated, and Removed events will be passed to the listener.
                         *
                         * Defaults to @c false.
                         *
                         * @param val Flag value.
                         */
                        void SetIncludeExpired(bool val)
                        {
                            includeExpired = val;
                        }

                        /**
                         * Gets a value indicating whether to notify about Expired events.
                         *
                         * If @c true, then the listener will get notifications about expired cache entries. Otherwise,
                         * only Created, Updated, and Removed events will be passed to the listener.
                         *
                         * Defaults to @c false.
                         *
                         * @return Flag value.
                         */
                        bool GetIncludeExpired() const
                        {
                            return includeExpired;
                        }

                        /**
                         * Set cache entry event listener.
                         *
                         * @param lsnr Cache entry event listener. Invoked on the
                         *     node where continuous query execution has been
                         *     started.
                         */
                        void SetListener(Reference<event::CacheEntryEventListener<K, V> > lsnr)
                        {
                            listener = lsnr;
                        }

                        /**
                         * Get cache entry event listener.
                         *
                         * @return Cache entry event listener.
                         */
                        const event::CacheEntryEventListener<K, V>& GetListener() const
                        {
                            return *listener.Get();
                        }

                        /**
                         * Get cache entry event listener.
                         *
                         * @return Cache entry event listener.
                         */
                        event::CacheEntryEventListener<K, V>& GetListener()
                        {
                            return *listener.Get();
                        }

                        /**
                         * Set Java event filter to be used on server to determine what events should be transferred
                         * to local event listener.
                         *
                         * @param fltr Java remote filter.
                         */
                        void SetJavaFilter(const event::JavaCacheEntryEventFilter& fltr)
                        {
                            filter = fltr;
                        }

                        /**
                         * Get remote Java filter reference.
                         *
                         * @return Remote Java filter.
                         */
                        event::JavaCacheEntryEventFilter& GetJavaFilter()
                        {
                            return filter;
                        }

                        /**
                         * Get remote Java filter reference.
                         *
                         * @return Remote Java filter.
                         */
                        const event::JavaCacheEntryEventFilter& GetJavaFilter() const
                        {
                            return filter;
                        }

                    private:
                        /** Buffer size. */
                        int32_t bufferSize;

                        /** Time interval. */
                        int64_t timeInterval;

                        /** Include expired. */
                        bool includeExpired;

                        /** Listener. */
                        Reference<event::CacheEntryEventListener<K, V> > listener;

                        /** Remote filter. */
                        event::JavaCacheEntryEventFilter filter;
                    };
                }
            }
        }
    }
}

#endif //_IGNITE_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_CLIENT