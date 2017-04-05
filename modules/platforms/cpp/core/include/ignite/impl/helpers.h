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

#ifndef _IGNITE_IMPL_HELPERS
#define _IGNITE_IMPL_HELPERS

#include <map>

#include <ignite/cache/cache_entry.h>
#include <ignite/impl/binary/binary_writer_impl.h>

namespace ignite
{
    namespace impl
    {
        /**
         * Class-helper to properly write values of different type.
         */
        template<typename K, typename V>
        struct ContainerEntryWriteHelper
        {
            template<typename E>
            static void Write(binary::BinaryWriterImpl& writer, const E& val)
            {
                writer.WriteTopObject(val);
            }

            static void Write(binary::BinaryWriterImpl& writer, const typename std::map<K, V>::value_type& val)
            {
                writer.WriteTopObject(val.first);
                writer.WriteTopObject(val.second);
            }

            static void Write(binary::BinaryWriterImpl& writer, const ignite::cache::CacheEntry<K, V>& val)
            {
                writer.WriteTopObject(val.GetKey());
                writer.WriteTopObject(val.GetValue());
            }
        };
    }
}

#endif //_IGNITE_IMPL_HELPERS
