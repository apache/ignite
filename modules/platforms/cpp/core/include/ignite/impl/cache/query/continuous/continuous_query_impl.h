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
 * Declares ignite::impl::cache::query::continuous::ContinuousQueryImpl class.
 */

#ifndef _IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_IMPL
#define _IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_IMPL

#include <stdint.h>

#include <ignite/reference.h>

#include <ignite/cache/event/cache_entry_event_listener.h>
#include <ignite/binary/binary_raw_reader.h>

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
                     * Continuous query base implementation class.
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
                    class ContinuousQueryImplBase
                    {
                    public:
                        /**
                         * Destructor.
                         */
                        virtual ~ContinuousQueryImplBase()
                        {
                            // No-op.
                        }

                        /**
                         * Default value for the buffer size.
                         */
                        enum { DEFAULT_BUFFER_SIZE = 1 };

                        /**
                         * Default value for the time interval.
                         */
                        enum { DEFAULT_TIME_INTERVAL = 0 };

                        /**
                         * Constructor.
                         *
                         * @param loc Whether query should be executed locally.
                         */
                        explicit ContinuousQueryImplBase(bool loc) :
                            local(loc),
                            bufferSize(DEFAULT_BUFFER_SIZE),
                            timeInterval(DEFAULT_TIME_INTERVAL)
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
                            local = val;
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
                            return local;
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
                            bufferSize = val;
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
                            return bufferSize;
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
                            timeInterval = val;
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
                            return timeInterval;
                        }

                        /**
                         * Callback that reads and processes cache events.
                         *
                         * @param reader Reader to use.
                         */
                        virtual void ReadAndProcessEvents(ignite::binary::BinaryRawReader& reader) = 0;

                    private:
                        /**
                         * Local flag. When set query will be executed only on local
                         * node, so only local entries will be returned as query
                         * result.
                         *
                         * Default value is false.
                         */
                        bool local;

                        /**
                         * Buffer size. When a cache update happens, entry is first
                         * put into a buffer. Entries from buffer will be sent to
                         * the master node only if the buffer is full or time
                         * provided via timeInterval is exceeded.
                         *
                         * Default value is DEFAULT_BUFFER_SIZE.
                         */
                        int32_t bufferSize;

                        /**
                         * Time interval in miliseconds. When a cache update
                         * happens, entry is first put into a buffer. Entries from
                         * buffer will be sent to the master node only if the buffer
                         * is full (its size can be changed via SetBufferSize) or
                         * time provided via SetTimeInterval method is exceeded.
                         *
                         * Default value is DEFAULT_TIME_INTERVAL, i.e. 0, which
                         * means that time check is disabled and entries will be
                         * sent only when buffer is full.
                         */
                        int64_t timeInterval;
                    };

                    /**
                     * Continuous query implementation.
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
                    class ContinuousQueryImpl : public ContinuousQueryImplBase
                    {
                    public:
                        /**
                         * Destructor.
                         */
                        virtual ~ContinuousQueryImpl()
                        {
                            // No-op.
                        }

                        /**
                         * Constructor.
                         *
                         * @param lsnr Event listener. Invoked on the node where
                         *     continuous query execution has been started.
                         */
                        ContinuousQueryImpl(Reference<ignite::cache::event::CacheEntryEventListener<K, V> >& lsnr) :
                            ContinuousQueryImplBase(false),
                            lsnr(lsnr)
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
                        ContinuousQueryImpl(Reference<ignite::cache::event::CacheEntryEventListener<K, V> >& lsnr, bool loc) :
                            ContinuousQueryImplBase(loc),
                            lsnr(lsnr)
                        {
                            // No-op.
                        }

                        /**
                         * Set cache entry event listener.
                         *
                         * @param val Cache entry event listener. Invoked on the
                         *     node where continuous query execution has been
                         *     started.
                         */
                        void SetListener(Reference<ignite::cache::event::CacheEntryEventListener<K, V> >& val)
                        {
                            lsnr = val;
                        }

                        /**
                         * Check if the query has listener.
                         *
                         * @return True if the query has listener.
                         */
                        bool HasListener() const
                        {
                            return !lsnr.IsNull();
                        }

                        /**
                         * Get cache entry event listener.
                         *
                         * @return Cache entry event listener.
                         */
                        const ignite::cache::event::CacheEntryEventListener<K, V>& GetListener() const
                        {
                            return lsnr.Get();
                        }

                        /**
                         * Get cache entry event listener.
                         *
                         * @return Cache entry event listener.
                         */
                        ignite::cache::event::CacheEntryEventListener<K, V>& GetListener()
                        {
                            return lsnr.Get();
                        }

                        /**
                         * Callback that reads and processes cache events.
                         *
                         * @param reader Reader to use.
                         */
                        virtual void ReadAndProcessEvents(ignite::binary::BinaryRawReader& reader)
                        {
                            // Number of events.
                            int32_t cnt = reader.ReadInt32();

                            // Storing events here.
                            std::vector< ignite::cache::CacheEntryEvent<K, V> > events;
                            events.resize(cnt);

                            for (int32_t i = 0; i < cnt; ++i)
                                events[i].Read(reader);

                            lsnr.Get().OnEvent(events.data(), cnt);
                        }

                    private:
                        /** Cache entry event listener. */
                        Reference<ignite::cache::event::CacheEntryEventListener<K, V> > lsnr;
                    };
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_IMPL