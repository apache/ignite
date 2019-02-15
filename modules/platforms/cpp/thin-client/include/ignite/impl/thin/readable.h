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
                    reader.ReadTopObject0<ignite::binary::BinaryReader, ValueType>(value);
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
