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

#ifndef _IGNITE_IMPL_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER_BASE
#define _IGNITE_IMPL_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER_BASE

#include <ignite/binary/binary_raw_reader.h>

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace event
            {
                /**
                 * Base for the Cache Entry Event Filter.
                 */
                class CacheEntryEventFilterBase
                {
                public:
                    /**
                     * Default constructor.
                     */
                    CacheEntryEventFilterBase()
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    virtual ~CacheEntryEventFilterBase()
                    {
                        // No-op.
                    }

                    /**
                     * Process serialized events.
                     *
                     * @param reader Reader for a serialized event.
                     * @return Filter evaluation result.
                     */
                    virtual bool ReadAndProcessEvent(ignite::binary::BinaryRawReader& reader) = 0;
                };
            }
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER_BASE
