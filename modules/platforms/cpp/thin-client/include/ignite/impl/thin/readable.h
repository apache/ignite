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

#include <ignite/impl/binary/binary_reader_impl.h>

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
        }
    }
}

#endif // _IGNITE_IMPL_THIN_READABLE
