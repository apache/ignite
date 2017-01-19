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

#ifndef _IGNITE_BINARY_BINARY_OBJECT
#define _IGNITE_BINARY_BINARY_OBJECT

#include <stdint.h>

#include <ignite/impl/interop/interop.h>
#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/binary/binary_common.h>
#include <ignite/impl/binary/binary_utils.h>

namespace ignite
{
    namespace binary
    {
        class BinaryObject
        {
        public:
            /**
             * Constructor.
             *
             * @param mem Binary object memory.
             * @param start Object starting position in memory.
             */
            BinaryObject(impl::interop::InteropMemory& mem, int32_t start) :
                mem(mem),
                start(start)
            {
                // No-op.
            }

            /**
             * Deserialize object.
             * @throw IgniteError if the object can not be deserialized to specified type.
             *
             * @return Deserialized value.
             */
            template<typename T>
            T Deserialize() const
            {
                impl::interop::InteropInputStream stream(&mem);

                stream.Position(start);
                impl::binary::BinaryReaderImpl reader(&stream);

                return reader.ReadObject<T>();
            }

            /**
             * Get object data.
             * @throw IgniteError if the object is not in a valid state.
             *
             * @return Pointer to object data.
             */
            const int8_t* GetData() const
            {
                impl::binary::BinaryUtils::CheckEnoughData(mem, start + impl::binary::IGNITE_OFFSET_DATA, GetLength());

                return mem.Data() + start + impl::binary::IGNITE_OFFSET_DATA;
            }

            /**
             * Get object length.
             * @throw IgniteError if the object is not in a valid state.
             *
             * @return Object length.
             */
            int32_t GetLength() const
            {
                int16_t flags = GetFlags();

                int32_t end = 0;

                if (flags & impl::binary::IGNITE_BINARY_FLAG_HAS_SCHEMA)
                    end = impl::binary::BinaryUtils::ReadInt32(mem, start + impl::binary::IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
                else
                    end = impl::binary::BinaryUtils::ReadInt32(mem, start + impl::binary::IGNITE_OFFSET_LEN);

                return end - impl::binary::IGNITE_OFFSET_DATA;
            }

        private:
            /**
             * Get object type.
             * @throw IgniteError if the object is not in a valid state.
             *
             * @return Object type.
             */
            int8_t GetType() const
            {
                return impl::binary::BinaryUtils::ReadInt8(mem, start);
            }

            /**
             * Get object flags.
             * @throw IgniteError if the object is not in a valid state.
             *
             * @return Object flags.
             */
            int16_t GetFlags() const
            {
                return impl::binary::BinaryUtils::ReadInt16(mem, start);
            }

            /** Underlying object memory. */
            impl::interop::InteropMemory& mem;

            /** Object starting position in memory. */
            int32_t start;
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_OBJECT