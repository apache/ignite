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

#include <stdint.h>

#include <ignite/cache/event/cache_entry_event_listener.h>
#include <ignite/binary/binary_raw_reader.h>

namespace ignite
{
    namespace cache
    {
        namespace query
        {
            namespace continuous
            {
                /**
                 * Continuous query base.
                 */
                class ContinuousQueryBase
                {
                public:
                    enum { DEFAULT_BUFFER_SIZE = 1 };
                    enum { DEFAULT_TIME_INTERVAL = 0 };

                    /**
                     * Constructor.
                     *
                     * @param lsnr Event listener.
                     */
                    ContinuousQueryBase(bool loc) :
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
                     * @param val Buffer size.
                     */
                    void SetBufferSize(int32_t val)
                    {
                        bufferSize = val;
                    }

                    /**
                     * Get buffer size.
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
                     * @param val Time interval.
                     */
                    void SetTimeInterval(int64_t val)
                    {
                        timeInterval = val;
                    }

                    /**
                     * Get time interval.
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
                    virtual void ReadAndProcessEvents(binary::BinaryRawReader& reader) = 0;

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
                     * Default value is 0, which means that time check is disabled and entries will be 
                     * sent only when buffer is full.
                     */
                    int64_t timeInterval;
                };

                /**
                 * Continuous query.
                 */
                template<typename K, typename V>
                class ContinuousQuery : public ContinuousQueryBase
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param lsnr Event listener.
                     */
                    ContinuousQuery(event::CacheEntryEventListener<K, V>& lsnr) :
                        ContinuousQueryBase(false),
                        lsnr(&lsnr)
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     *
                     * @param lsnr Event listener.
                     * @param loc Whether query should be executed locally.
                     */
                    ContinuousQuery(event::CacheEntryEventListener<K, V>& lsnr, bool loc) :
                        ContinuousQueryBase(loc),
                        lsnr(&lsnr)
                    {
                        // No-op.
                    }

                    /**
                     * Set cache entry event listener.
                     *
                     * @param val Cache entry event listener.
                     */
                    void SetListener(event::CacheEntryEventListener<K, V>& val)
                    {
                        lsnr = &lsnr;
                    }

                    /**
                     * Get cache entry event listener.
                     *
                     * @return Cache entry event listener.
                     */
                    const event::CacheEntryEventListener<K, V>& GetListener() const
                    {
                        return *lsnr;
                    }

                    /**
                     * Get cache entry event listener.
                     *
                     * @return Cache entry event listener.
                     */
                    event::CacheEntryEventListener<K, V>& GetListener()
                    {
                        return *lsnr;
                    }

                    /**
                     * Callback that reads and processes cache events.
                     *
                     * @param reader Reader to use.
                     */
                    virtual void ReadAndProcessEvents(binary::BinaryRawReader& reader)
                    {
                        // Number of events.
                        int32_t cnt = reader.ReadInt32();

                        // Storing events here.
                        std::vector< CacheEntryEvent<K, V> > events;
                        events.resize(cnt);

                        for (int32_t i = 0; i < cnt; ++i)
                            events[i].Read(reader);

                        lsnr->OnEvent(events.data(), cnt);
                    }

                private:
                    /**
                     * Cache entry event listener. Invoked on the node where
                     * continuous query execution has been started.
                     */
                    event::CacheEntryEventListener<K, V>* lsnr;
                };
            }
        }
    }
}

#endif //_IGNITE_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY