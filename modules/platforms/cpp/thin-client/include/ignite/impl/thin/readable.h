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

#ifndef _IGNITE_IMPL_THIN_READABLE
#define _IGNITE_IMPL_THIN_READABLE

#include <utility>

#include <ignite/binary/binary_raw_reader.h>
#include <ignite/thin/cache/cache_entry.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Abstraction to any type that can be read from a binary stream.
             */
            class Readable
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~Readable()
                {
                    // No-op.
                }

                /**
                 * Read value using reader.
                 *
                 * @param reader Reader to use.
                 */
                virtual void Read(binary::BinaryReaderImpl& reader) = 0;
            };

            /**
             * Implementation of the Readable class for a concrete type.
             */
            template<typename T>
            class ReadableImpl : public Readable
            {
            public:
                /** Value type. */
                typedef T ValueType;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                ReadableImpl(ValueType& value) :
                    value(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ReadableImpl()
                {
                    // No-op.
                }

                /**
                 * Read value using reader.
                 *
                 * @param reader Reader to use.
                 */
                virtual void Read(binary::BinaryReaderImpl& reader)
                {
                    reader.ReadTopObject<ValueType>(value);
                }

            private:
                /** Value reference. */
                ValueType& value;
            };

            /**
             * Implementation of the Readable class for the std::pair type.
             */
            template<typename T1, typename T2>
            class ReadableImpl< std::pair<T1, T2> > : public Readable
            {
            public:
                /** First value type. */
                typedef T1 ValueType1;

                /** Second value type. */
                typedef T2 ValueType2;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                ReadableImpl(std::pair<ValueType1, ValueType2>& pair) :
                    pair(pair)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ReadableImpl()
                {
                    // No-op.
                }

                /**
                 * Read value using reader.
                 *
                 * @param reader Reader to use.
                 */
                virtual void Read(binary::BinaryReaderImpl& reader)
                {
                    reader.ReadTopObject<ValueType1>(pair.first);
                    reader.ReadTopObject<ValueType2>(pair.second);
                }

            private:
                /** Pair reference. */
                std::pair<ValueType1, ValueType2>& pair;
            };

            /**
             * Implementation of the Readable class for the CacheEntry type.
             */
            template<typename K, typename V>
            class ReadableImpl< ignite::thin::cache::CacheEntry<K, V> > : public Readable
            {
            public:
                /** Key type. */
                typedef K KeyType;

                /** Value type. */
                typedef V ValueType;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                ReadableImpl(ignite::thin::cache::CacheEntry<KeyType, ValueType>& entry) :
                    entry(entry)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ReadableImpl()
                {
                    // No-op.
                }

                /**
                 * Read value using reader.
                 *
                 * @param reader Reader to use.
                 */
                virtual void Read(binary::BinaryReaderImpl& reader)
                {
                    reader.ReadTopObject<KeyType>(entry.key);
                    reader.ReadTopObject<ValueType>(entry.val);
                }

            private:
                /** Entry reference. */
                ignite::thin::cache::CacheEntry<KeyType, ValueType>& entry;
            };

            /**
             * Implementation of Readable interface for map.
             *
             * @tparam T Type for the element in the container.
             * @tparam I Out iterator.
             */
            template<typename T, typename I>
            class ReadableContainerImpl : public Readable
            {
            public:
                /** Type of the element in the containers. */
                typedef T ValueType;

                /** Type of the iterator. */
                typedef I IteratorType;

                /** Readable type for the element in the containers. */
                typedef ReadableImpl<ValueType> ReadableType;

                /**
                 * Constructor.
                 *
                 * @param iter Iterator.
                 */
                ReadableContainerImpl(IteratorType iter) :
                    iter(iter)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ReadableContainerImpl()
                {
                    // No-op.
                }

                /**
                 * Read value using reader.
                 *
                 * @param reader Reader to use.
                 */
                virtual void Read(binary::BinaryReaderImpl& reader)
                {
                    using namespace ignite::binary;

                    int32_t cnt = reader.ReadInt32();

                    for (int32_t i = 0; i < cnt; ++i)
                    {
                        ValueType value;

                        ReadableType readable(value);

                        readable.Read(reader);

                        *iter = value;
                        ++iter;
                    }
                }

            private:
                /** Iterator type. */
                IteratorType iter;
            };
        }
    }
}

#endif // _IGNITE_IMPL_THIN_READABLE
