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
 * Declares ignite::binary::BinaryObject class.
 */

#ifndef _IGNITE_IMPL_BINARY_BINARY_OBJECT_IMPL
#define _IGNITE_IMPL_BINARY_BINARY_OBJECT_IMPL

#include <stdint.h>

#include <ignite/impl/interop/interop.h>
#include <ignite/impl/binary/binary_reader_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Binary object implementation.
             *
             * This is a thin wrapper over the memory area that contains serialized
             * binary object. Provides some methods that allow to access object's
             * data without deserialization. Also provides method that allows
             * deserialize object.
             */
            class IGNITE_IMPORT_EXPORT BinaryObjectImpl
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param mem Binary object memory.
                 * @param start Object starting position in memory.
                 */
                BinaryObjectImpl(interop::InteropMemory& mem, int32_t start);

                /**
                 * Copy constructor.
                 *
                 * @param other Another instance.
                 */
                BinaryObjectImpl(const BinaryObjectImpl& other);

                /**
                 * Assignment operator.
                 *
                 * @param other Another instance.
                 * @return *this.
                 */
                BinaryObjectImpl& operator=(const BinaryObjectImpl& other);

                /**
                 * Create from InteropMemory instance.
                 * @throw IgniteError if the memory at the specified offset
                 *    is not a valid BinaryObject.
                 *
                 * @param mem Memory.
                 * @param offset Offset in memory.
                 * @return BinaryObjectImpl instance.
                 */
                static BinaryObjectImpl FromMemory(interop::InteropMemory& mem, int32_t offset);

                /**
                 * Create from InteropMemory instance.
                 * @warning Does not check memory for validity.
                 *
                 * @param mem Memory.
                 * @param offset Offset in memory.
                 * @return BinaryObjectImpl instance.
                 */
                static BinaryObjectImpl FromMemoryUnsafe(interop::InteropMemory& mem, int32_t offset);

                /**
                 * Deserialize object.
                 * @throw IgniteError if the object can not be deserialized to specified type.
                 *
                 * @return Deserialized value.
                 */
                template<typename T>
                T Deserialize() const
                {
                    interop::InteropInputStream stream(mem);

                    stream.Position(start);
                    BinaryReaderImpl reader(&stream);

                    return reader.ReadObject<T>();
                }

                /**
                 * Get binary object field.
                 *
                 * @warning Works only if all object fields are objects.
                 *     Otherwise behavior is undefined.
                 *
                 * @param idx Field index. Starts from 0.
                 * @return Binary object field.
                 */
                BinaryObjectImpl GetField(int32_t idx);

                /**
                 * Get object data.
                 *
                 * @return Pointer to object data.
                 */
                const int8_t* GetData() const;

                /**
                 * Get object length.
                 * @throw IgniteError if the object is not in a valid state.
                 *
                 * @return Object length.
                 */
                int32_t GetLength() const;

                /**
                 * Get type ID.
                 * @throw IgniteError if the object is not in a valid state.
                 *
                 * @return Type ID.
                 */
                int32_t GetTypeId() const;

                /**
                 * Get object hash code.
                 * @throw IgniteError if the object is not in a valid state.
                 *
                 * @return Object hash code.
                 */
                int32_t GetHashCode() const;

            private:
                /** Underlying object memory. */
                interop::InteropMemory* mem;

                /** Object starting position in memory. */
                int32_t start;
            };
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_OBJECT_IMPL