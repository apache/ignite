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

#ifndef _IGNITE_CACHE_CACHE_ENTRY_PROCESSOR_HOLDER
#define _IGNITE_CACHE_CACHE_ENTRY_PROCESSOR_HOLDER

#include <ignite/common/common.h>
#include <ignite/cache/mutable_cache_entry.h>
#include <ignite/portable/portable.h>

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            /**
            * Mutable Cache entry state.
            */
            enum MutableCacheEntryState
            {
                /** No changes have been committed to entry. */
                ENTRY_STATE_INTACT = 0,

                /** Value of the entry has been changed. */
                ENTRY_STATE_VALUE_SET = 1,

                /** Entry has been removed from cache. */
                ENTRY_STATE_VALUE_REMOVED = 2,

                /** Error occured. Represented in portable form. */
                ENTRY_STATE_ERR_PORTABLE = 3,

                /** Error occured. Represented in string form. */
                ENTRY_STATE_ERR_STRING = 4
            };

            template<typename V>
            MutableCacheEntryState GetMutableCacheEntryState(const V& value_before, bool exists_before, 
                                                             const V& value_after, bool exists_after)
            {
                if ((!exists_before && exists_after) ||
                    (exists_before && exists_after && value_before != value_after))
                    return ENTRY_STATE_VALUE_SET;
                
                if (exists_before && !exists_after)
                    return ENTRY_STATE_VALUE_REMOVED;

                return ENTRY_STATE_INTACT;
            }

            /**
             * Holder for the Cache Entry Processor and its argument. Used as a convenient way to
             * transmit Cache Entry Processor processor between nodes.
             */
            template<typename P, typename A>
            class CacheEntryProcessorHolder
            {
            public:

                typedef P ProcessorType;
                typedef A ArgumentType;

                /**
                 * Default constructor.
                 */
                CacheEntryProcessorHolder() : proc(), arg()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 */
                CacheEntryProcessorHolder(const P& proc, const A& arg) : proc(proc), arg(arg)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~CacheEntryProcessorHolder()
                {
                    // No-op.
                }

                /**
                 * Get processor.
                 *
                 * @return Processor.
                 */
                const ProcessorType& getProcessor() const
                {
                    return proc;
                }

                /**
                 * Get argument.
                 *
                 * @return Argument.
                 */
                const ArgumentType& getArgument() const
                {
                    return arg;
                }

                /**
                 * Process key-value pair by the underlying Cache Entry Processor
                 * using binded argument.
                 *
                 * @param key Cache entry key.
                 * @param value Cache entry value. New value is stored here upon completion.
                 * @param exists Entry existance indicator.
                 * @param state State of the entry after the processing.
                 * @return Result of the processing.
                 */
                template<typename R, typename K, typename V>
                R Process(const K& key, V& value, bool exists, MutableCacheEntryState &state)
                {
                    ignite::cache::MutableCacheEntry<K, V> entry(key, value);

                    R res = proc.Process(entry, arg);

                    state = GetMutableCacheEntryState(value, exists, entry.GetValue(), entry.Exists());

                    value = entry.GetValue();

                    return res;
                }

            private:
                /** Stored processor. */
                ProcessorType proc;

                /** Stored argument. */
                ArgumentType  arg;
            };
        }
    }

    namespace portable
    {
        /**
         * Portable type specialisation for CacheEntryProcessorHolder.
         */
        template<typename P, typename A>
        struct PortableType<impl::cache::CacheEntryProcessorHolder<P, A>>
        {
            typedef impl::cache::CacheEntryProcessorHolder<P, A> UnderlyingType;

            IGNITE_PORTABLE_GET_FIELD_ID_AS_HASH
            IGNITE_PORTABLE_GET_HASH_CODE_ZERO(UnderlyingType)
            IGNITE_PORTABLE_IS_NULL_FALSE(UnderlyingType)
            IGNITE_PORTABLE_GET_NULL_DEFAULT_CTOR(UnderlyingType)

            int32_t GetTypeId()
            {
                return GetPortableStringHashCode(GetTypeName().c_str());
            }
            
            std::string GetTypeName()
            {
                PortableType<P> p;
                // TODO: implement GetTypeName for basic types?
                //PortableType<A> a;

                std::string name = "CacheEntryProcessorHolder<" + p.GetTypeName() + ",int>";
                return name;
            }

            void Write(PortableWriter& writer, UnderlyingType obj)
            {
                writer.WriteObject("proc", obj.getProcessor());
                writer.WriteObject("arg", obj.getArgument());
            }

            UnderlyingType Read(PortableReader& reader)
            {
                const P& proc = reader.ReadObject<P>("proc");
                const A& arg = reader.ReadObject<A>("arg");

                return UnderlyingType(proc, arg);
            }
        };
    }
}

#endif
