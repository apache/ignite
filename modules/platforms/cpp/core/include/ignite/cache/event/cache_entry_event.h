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
