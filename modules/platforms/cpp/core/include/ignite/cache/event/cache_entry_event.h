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
 * Declares ignite::cache::event::CacheEntryEvent class.
 */

#ifndef _IGNITE_CACHE_EVENT_CACHE_ENTRY_EVENT
#define _IGNITE_CACHE_EVENT_CACHE_ENTRY_EVENT

#include <ignite/binary/binary_raw_reader.h>
#include <ignite/cache/cache_entry.h>

namespace ignite
{
    namespace cache
    {
        /**
         * Cache entry event class template.
         *
         * Both key and value types should be default-constructable,
         * copy-constructable and assignable.
         */
        template<typename K, typename V>
        class CacheEntryEvent : public CacheEntry<K, V>
        {
        public:
            /**
             * Default constructor.
             *
             * Creates instance with all fields default-constructed.
             */
            CacheEntryEvent() :
                CacheEntry<K, V>(),
                oldVal(),
                hasOldValue(false)
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
                hasOldValue(other.hasOldValue)
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
                }

                return *this;
            }

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
            }

        private:
            /** Old value. */
            V oldVal;

            /** Indicates whether old value exists */
            bool hasOldValue;
        };
    }
}

#endif //_IGNITE_CACHE_EVENT_CACHE_ENTRY_EVENT
