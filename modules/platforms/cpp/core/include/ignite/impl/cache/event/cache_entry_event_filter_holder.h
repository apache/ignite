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

#ifndef _IGNITE_IMPL_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER_HOLDER
#define _IGNITE_IMPL_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER_HOLDER

#include <ignite/reference.h>

#include <ignite/impl/operations.h>

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            /**
             * Holder for the Cache Entry Event Filter.
             */
            template<typename F>
            class CacheEntryEventFilterHolder : public InputOperation
            {
            public:
                typedef F FilterType;

                /**
                 * Default constructor.
                 */
                CacheEntryEventFilterHolder() :
                    filter()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param filter Filter.
                 */
                CacheEntryEventFilterHolder(const Reference<FilterType>& filter) :
                    filter(filter)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CacheEntryEventFilterHolder()
                {
                    // No-op.
                }

                /**
                 * Process input.
                 *
                 * @param writer Writer.
                 */
                virtual void ProcessInput(binary::BinaryWriterImpl& writer)
                {
                    if (!filter.IsNull())
                    {
                        writer.WriteBool(true);
                        writer.WriteObject<FilterType>(*filter.Get());
                    }
                    else
                    {
                        writer.WriteBool(false);
                        writer.WriteNull();
                    }
                }

            private:
                /** Stored filter. */
                Reference<FilterType> filter;
            };

            template<>
            class CacheEntryEventFilterHolder<void> : public InputOperation
            {
            public:
                /**
                 * Default constructor.
                 */
                CacheEntryEventFilterHolder()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 */
                CacheEntryEventFilterHolder(const Reference<void>&)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CacheEntryEventFilterHolder()
                {
                    // No-op.
                }

                /**
                 * Process input.
                 *
                 * @param writer Writer.
                 */
                virtual void ProcessInput(binary::BinaryWriterImpl& writer)
                {
                    writer.WriteBool(false);
                    writer.WriteNull();
                }
            };
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_EVENT_CACHE_ENTRY_EVENT_FILTER_HOLDER
