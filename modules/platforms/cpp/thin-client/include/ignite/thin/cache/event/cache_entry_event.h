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
 * Declares ignite::thin::cache::event::CacheEntryEvent class.
 */

#ifndef _IGNITE_THIN_CACHE_EVENT_CACHE_ENTRY_EVENT
#define _IGNITE_THIN_CACHE_EVENT_CACHE_ENTRY_EVENT

#include <ignite/binary/binary_raw_reader.h>
#include <ignite/thin/cache/cache_entry.h>

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
                        template<typename K, typename V>
                        class ContinuousQueryClientHolder;
                    }
                }
            }
        }
    }
    namespace thin
    {
        namespace cache
        {
            /**
             * Cache event type.
             */
            struct CacheEntryEventType
            {
                enum Type
                {
                    /** An event type indicating that the cache entry was created. */
                    CREATED = 0,

                    /** An event type indicating that the cache entry was updated. i.e. a previous */
                    UPDATED = 1,

                    /** An event type indicating that the cache entry was removed. */
                    REMOVED = 2,

                    /** An event type indicating that the cache entry was removed by expiration policy. */
                    EXPIRED = 3
                };
            };

            /**
             * Cache entry event class template.
             *
             * Both key and value types should be default-constructable,
             * copy-constructable and assignable.
             */
            template<typename K, typename V>
            class CacheEntryEvent : public CacheEntry<K, V>
            {
                friend class ignite::impl::thin::cache::query::continuous::ContinuousQueryClientHolder<K, V>;

            public:
                /**
                 * Default constructor.
                 *
                 * Creates instance with all fields default-constructed.
                 */
                CacheEntryEvent() :
                    CacheEntry<K, V>(),
                    oldVal(),
                    hasOldValue(false),
                    eventType(CacheEntryEventType::CREATED)
                {
                    // No-op.
                }
    
                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                CacheEntryEvent(const CacheEntryEvent<K, V>& other) :
                    CacheEntry<K, V>(other),
                    oldVal(other.oldVal),
                    hasOldValue(other.hasOldValue),
                    eventType(other.eventType)
                {
                    // No-op.
                }
    
                /**
                 * Destructor.
                 */
                virtual ~CacheEntryEvent()
                {
                    // No-op.
                }
    
                /**
                 * Assignment operator.
                 *
                 * @param other Other instance.
                 * @return *this.
                 */
                CacheEntryEvent& operator=(const CacheEntryEvent<K, V>& other)
                {
                    if (this != &other)
                    {
                        CacheEntry<K, V>::operator=(other);
    
                        oldVal = other.oldVal;
                        hasOldValue = other.hasOldValue;
                        eventType = other.eventType;
                    }
    
                    return *this;
                }

                /**
                 * Get event type.
                 *
                 * @see CacheEntryEventType for details on events you can receive.
                 *
                 * @return Event type.
                 */
                CacheEntryEventType::Type GetEventType() const
                {
                    return eventType;
                };
    
                /**
                 * Get old value.
                 *
                 * @return Old value.
                 */
                const V& GetOldValue() const
                {
                    return oldVal;
                }
    
                /**
                 * Check if the old value exists.
                 *
                 * @return True, if the old value exists.
                 */
                bool HasOldValue() const
                {
                    return hasOldValue;
                }
    
            private:
                /**
                 * Reads cache event using provided raw reader.
                 *
                 * @param reader Reader to use.
                 */
                void Read(binary::BinaryRawReader& reader)
                {
                    this->key = reader.ReadObject<K>();

                    this->hasOldValue = reader.TryReadObject(this->oldVal);
                    this->hasValue = reader.TryReadObject(this->val);

                    int8_t intType = reader.ReadInt8();
                    if (intType < 0 || intType > 3)
                    {
                        std::string errMsg = "Event type is not supported: " + common::LexicalCast<std::string>(intType);
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, errMsg.c_str());
                    }

                    eventType = static_cast<CacheEntryEventType::Type>(intType);
                }

                /** Old value. */
                V oldVal;
    
                /** Indicates whether old value exists */
                bool hasOldValue;

                /** Event type. */
                CacheEntryEventType::Type eventType;
            };
        }
    }
}

#endif //_IGNITE_THIN_CACHE_EVENT_CACHE_ENTRY_EVENT
