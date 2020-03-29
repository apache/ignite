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
 * Declares ignite::cache::event::CacheEntryEventFilter class.
 */

#ifndef _IGNITE_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER
#define _IGNITE_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER

#include <ignite/cache/event/cache_entry_event.h>
#include <ignite/impl/cache/event/cache_entry_event_filter_base.h>

namespace ignite
{
    class IgniteBinding;

    namespace impl
    {
        namespace cache
        {
            namespace event
            {
                template<typename T>
                class CacheEntryEventFilterHolder;
            }
        }
    }

    namespace cache
    {
        namespace event
        {
            /**
             * Cache entry event filter.
             *
             * All templated types should be default-constructable,
             * copy-constructable and assignable.
             *
             * @tparam K Key type.
             * @tparam V Value type.
             */
            template<typename K, typename V>
            class CacheEntryEventFilter : private impl::cache::event::CacheEntryEventFilterBase
            {
                template<typename T>
                friend class impl::cache::event::CacheEntryEventFilterHolder;

            public:
                /**
                 * Default constructor.
                 */
                CacheEntryEventFilter()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CacheEntryEventFilter()
                {
                    // No-op.
                }

                /**
                 * Event callback.
                 *
                 * @param event Event.
                 * @return True if the event passes filter.
                 */
                virtual bool Process(const CacheEntryEvent<K, V>& event) = 0;

            private:
                /**
                 * Process serialized events.
                 *
                 * @param reader Reader for a serialized event.
                 * @return Filter evaluation result.
                 */
                virtual bool ReadAndProcessEvent(binary::BinaryRawReader& reader)
                {
                    CacheEntryEvent<K, V> event;

                    event.Read(reader);

                    return Process(event);
                }
            };
        }
    }
}

#endif //_IGNITE_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER