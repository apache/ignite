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
 * Declares ignite::cache::CacheEntryProcessor class.
 */

#ifndef _IGNITE_CACHE_CACHE_ENTRY_PROCESSOR
#define _IGNITE_CACHE_CACHE_ENTRY_PROCESSOR

#include <ignite/common/common.h>
#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/cache/cache_entry_processor_holder.h>

namespace ignite
{
    class IgniteBinding;

    namespace cache
    {
        /**
         * %Cache entry processor class template.
         *
         * Any cache processor should inherit from this class.
         *
         * All templated types should be default-constructable,
         * copy-constructable and assignable.
         *
         * @tparam K Key type.
         * @tparam V Value type.
         * @tparam R Process method return type.
         * @tparam A Process method argument type.
         */
        template<typename K, typename V, typename R, typename A>
        class CacheEntryProcessor
        {
            friend class ignite::IgniteBinding;

            typedef A ArgumentType;
            typedef K KeyType;
            typedef V ValueType;
            typedef R ReturnType;

        public:
            /**
             * Destructor.
             */
            virtual ~CacheEntryProcessor()
            {
                // No-op.
            }

            /**
             * Process entry, using input argument and return result.
             *
             * @param entry Entry to process.
             * @param arg Argument.
             * @return Processing result.
             */
            virtual R Process(MutableCacheEntry<K, V>& entry, const A& arg) = 0;
        };
    }
}

#endif //_IGNITE_CACHE_CACHE_ENTRY_PROCESSOR
