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

#ifndef _IGNITE_IMPL_CACHE_CACHE_ENTRY_PROCESSOR_HOLDER
#define _IGNITE_IMPL_CACHE_CACHE_ENTRY_PROCESSOR_HOLDER

#include <ignite/common/common.h>
#include <ignite/cache/mutable_cache_entry.h>
#include <ignite/binary/binary.h>

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            /**
             * Mutable Cache entry state.
             */
            struct MutableCacheEntryState
            {
                enum Type
                {
                    /** No changes have been committed to entry. */
                    INTACT = 0,

                    /** Value of the entry has been changed. */
                    VALUE_SET = 1,

                    /** Entry has been removed from cache. */
                    VALUE_REMOVED = 2,

                    /** Error occured. Represented in binary form. */
                    ERR_BINARY = 3,

                    /** Error occured. Represented in string form. */
                    ERR_STRING = 4
                };
            };

            /**
             * Get state of the mutable cache entry.
             *
             * @param valueBefore Cache entry value before mutation.
             * @param existsBefore Flag for entry existence before mutation.
             * @param valueBefore Cache entry value after mutation.
             * @param existsBefore Flag for entry existence after mutation.
             * @return Cache entry state.
             */
            template<typename V>
            MutableCacheEntryState::Type GetMutableCacheEntryState(const V& valueBefore, bool existsBefore,
                                                                   const V& valueAfter, bool existsAfter)
            {
                if ((!existsBefore && existsAfter) ||
                    (existsBefore && existsAfter && !(valueBefore == valueAfter)))
                    return MutableCacheEntryState::VALUE_SET;

                if (existsBefore && !existsAfter)
                    return MutableCacheEntryState::VALUE_REMOVED;

                return MutableCacheEntryState::INTACT;
            }

            /**
             * Holder for the Cache Entry Processor and its argument. Used as a convenient way to
             * transmit Cache Entry Processor between nodes.
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
                CacheEntryProcessorHolder() :
                    proc(),
                    arg()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param proc Processor.
                 * @param arg Argument.
                 */
                CacheEntryProcessorHolder(const P& proc, const A& arg) :
                    proc(proc),
                    arg(arg)
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
                 * Equality operator should be defined for the value type.
                 *
                 * @param key Cache entry key.
                 * @param value Cache entry value. New value is stored here upon completion.
                 * @param exists Entry existance indicator.
                 * @param state State of the entry after the processing.
                 * @return Result of the processing.
                 */
                template<typename R, typename K, typename V>
                R Process(const K& key, V& value, bool exists, MutableCacheEntryState::Type &state)
                {
                    typedef ignite::cache::MutableCacheEntry<K, V> Entry;

                    Entry entry;

                    if (exists)
                        entry = Entry(key, value);
                    else
                        entry = Entry(key);

                    R res = proc.Process(entry, arg);

                    state = GetMutableCacheEntryState(value, exists, entry.GetValue(), entry.IsExists());

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

    namespace binary
    {
        /**
         * Binary type specialization for CacheEntryProcessorHolder.
         */
        template<typename P, typename A>
        struct BinaryType<impl::cache::CacheEntryProcessorHolder<P, A> > :
            BinaryTypeNonNullableType< impl::cache::CacheEntryProcessorHolder<P, A> >
        {
            typedef impl::cache::CacheEntryProcessorHolder<P, A> UnderlyingType;

            IGNITE_BINARY_GET_FIELD_ID_AS_HASH

            static int32_t GetTypeId()
            {
                static bool typeIdInited = false;
                static int32_t typeId;
                static common::concurrent::CriticalSection initLock;

                if (typeIdInited)
                    return typeId;

                common::concurrent::CsLockGuard guard(initLock);

                if (typeIdInited)
                    return typeId;

                std::string typeName;
                GetTypeName(typeName);

                typeId = GetBinaryStringHashCode(typeName.c_str());
                typeIdInited = true;

                return typeId;
            }

            static void GetTypeName(std::string& dst)
            {
                // Using static variable and only initialize it once for better
                // performance. Type name can't change in the course of the
                // program flow.
                static std::string name;
                static common::concurrent::CriticalSection initLock;

                // Name has been constructed already. Return it.
                if (!name.empty())
                {
                    dst = name;

                    return;
                }

                common::concurrent::CsLockGuard guard(initLock);

                if (!name.empty())
                {
                    dst = name;

                    return;
                }

                // Constructing name here.
                std::string procName;

                BinaryType<P>::GetTypeName(procName);

                // -1 is for unnessecary null byte at the end of the C-string.
                name.reserve(sizeof("CacheEntryProcessorHolder<>") - 1 + procName.size());

                // Processor name is enough for identification as it is
                // forbidden to register the same processor type several times.
                name.append("CacheEntryProcessorHolder<").append(procName).push_back('>');

                dst = name;
            }

            static void Write(BinaryWriter& writer, const UnderlyingType& obj)
            {
                BinaryRawWriter raw = writer.RawWriter();

                raw.WriteObject(obj.getProcessor());
                raw.WriteObject(obj.getArgument());
            }

            static void Read(BinaryReader& reader, UnderlyingType& dst)
            {
                BinaryRawReader raw = reader.RawReader();

                const P& proc = raw.ReadObject<P>();
                const A& arg = raw.ReadObject<A>();

                dst = UnderlyingType(proc, arg);
            }
        };
    }
}

#endif //_IGNITE_IMPL_CACHE_CACHE_ENTRY_PROCESSOR_HOLDER
