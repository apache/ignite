/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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