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

#include <ignite/binary/binary_raw_reader.h>

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
                /** Data router. */
                ValueType& value;
            };

            /**
             * Implementation of Readable interface for map.
             *
             * @tparam T1 Type of the first element in the pair.
             * @tparam T2 Type of the second element in the pair.
             * @tparam I Out iterator.
             */
            template<typename T1, typename T2, typename I>
            class ReadableMapImpl : public Readable
            {
            public:
                /** Type of the first element in the pair. */
                typedef T1 ElementType1;

                /** Type of the second element in the pair. */
                typedef T2 ElementType2;
                
                /** Type of the iterator. */
                typedef I IteratorType;

                /**
                 * Constructor.
                 *
                 * @param iter Iterator.
                 */
                ReadableMapImpl(IteratorType iter) :
                    iter(iter)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ReadableMapImpl()
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
                        std::pair<ElementType1, ElementType2> pair;

                        reader.ReadTopObject<ElementType1>(pair.first);
                        reader.ReadTopObject<ElementType2>(pair.second);

                        iter = pair;

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
