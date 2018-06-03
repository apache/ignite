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
