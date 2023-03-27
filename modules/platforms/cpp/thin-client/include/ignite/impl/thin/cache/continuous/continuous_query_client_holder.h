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

#ifndef _IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_CLIENT
#define _IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_CLIENT

#include <ignite/common/concurrent.h>

#include <ignite/thin/cache/query/continuous/continuous_query_client.h>

namespace ignite
{
    namespace impl
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
                         * Continuous query client holder.
                         */
                        class ContinuousQueryClientHolderBase
                        {
                        public:
                            /**
                             * Destructor.
                             */
                            virtual ~ContinuousQueryClientHolderBase()
                            {
                                // No-op.
                            }

                            /**
                             * Read and process cache events.
                             *
                             * @param reader Reader to use.
                             */
                            virtual void ReadAndProcessEvents(ignite::binary::BinaryRawReader& reader) = 0;

                            /**
                             * Get buffer size.
                             *
                             * @return Buffer size.
                             */
                            virtual int32_t GetBufferSize() const = 0;

                            /**
                             * Get time interval.
                             *
                             * @return Time interval.
                             */
                            virtual int64_t GetTimeInterval() const = 0;

                            /**
                             * Gets a value indicating whether to notify about Expired events.
                             *
                             * @return Flag value.
                             */
                            virtual bool GetIncludeExpired() const = 0;

                            /**
                             * Disconnected callback.
                             *
                             * Called if channel was disconnected.
                             */
                            virtual void OnDisconnected() = 0;
                        };

                        /** Shared pointer to ContinuousQueryClientHolderBase. */
                        typedef common::concurrent::SharedPointer<ContinuousQueryClientHolderBase> SP_ContinuousQueryClientHolderBase;

                        /**
                         * Continuous query client holder.
                         */
                        template<typename K, typename V>
                        class ContinuousQueryClientHolder : public ContinuousQueryClientHolderBase
                        {
                        public:
                            /** Wrapped continuous query type. */
                            typedef ignite::thin::cache::query::continuous::ContinuousQueryClient<K, V> ContinuousQuery;

                            /**
                             * Constructor.
                             *
                             * @param continuousQuery
                             */
                            ContinuousQueryClientHolder(const ContinuousQuery& continuousQuery) :
                                continuousQuery(continuousQuery)
                            {
                                // No-op.
                            }

                            /**
                             * Read and process cache events.
                             *
                             * @param reader Reader to use.
                             */
                            virtual void ReadAndProcessEvents(ignite::binary::BinaryRawReader& reader)
                            {
                                // Number of events.
                                int32_t cnt = reader.ReadInt32();

                                // Storing events here.
                                std::vector< ignite::thin::cache::CacheEntryEvent<K, V> > events;
                                events.resize(cnt);

                                for (int32_t i = 0; i < cnt; ++i)
                                    events[i].Read(reader);

                                continuousQuery.GetListener().OnEvent(events.data(), cnt);
                            }

                            /**
                             * Get buffer size.
                             *
                             * @return Buffer size.
                             */
                            virtual int32_t GetBufferSize() const
                            {
                                return continuousQuery.GetBufferSize();
                            }

                            /**
                             * Get time interval.
                             *
                             * @return Time interval.
                             */
                            virtual int64_t GetTimeInterval() const
                            {
                                return continuousQuery.GetTimeInterval();
                            }

                            /**
                             * Gets a value indicating whether to notify about Expired events.
                             *
                             * @return Flag value.
                             */
                            virtual bool GetIncludeExpired() const
                            {
                                return continuousQuery.GetIncludeExpired();
                            }

                            /**
                             * Disconnected callback.
                             *
                             * Called if channel was disconnected.
                             */
                            virtual void OnDisconnected()
                            {
                                continuousQuery.GetListener().OnDisconnected();
                            }

                        private:
                            /** Continuous query. */
                            ContinuousQuery continuousQuery;
                        };
                    }
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_CLIENT