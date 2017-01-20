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

#include "ignite/binary/binary_object.h"

namespace ignite
{
    namespace binary
    {
        BinaryObject::BinaryObject(impl::interop::InteropMemory& mem, int32_t start) :
            mem(mem),
            start(start)
        {
            // No-op.
        }

        const int8_t* BinaryObject::GetData() const
        {
            Validate();

            return mem.Data() + start + impl::binary::IGNITE_OFFSET_DATA;
        }

        int32_t BinaryObject::GetLength() const
        {
            Validate();

            int16_t flags = GetFlags();

            int32_t end = 0;

            if (flags & impl::binary::IGNITE_BINARY_FLAG_HAS_SCHEMA)
                end = impl::binary::BinaryUtils::UnsafeReadInt32(mem, start + impl::binary::IGNITE_OFFSET_SCHEMA_OR_RAW_OFF);
            else
                end = impl::binary::BinaryUtils::UnsafeReadInt32(mem, start + impl::binary::IGNITE_OFFSET_LEN);

            return end - impl::binary::IGNITE_OFFSET_DATA;
        }

        int8_t BinaryObject::GetType() const
        {
            return impl::binary::BinaryUtils::UnsafeReadInt8(mem, start);
        }

        int16_t BinaryObject::GetFlags() const
        {
            return impl::binary::BinaryUtils::UnsafeReadInt16(mem, start);
        }

        void BinaryObject::Validate() const
        {
            if (mem.Length() < (impl::binary::IGNITE_OFFSET_DATA))
            {
                IGNITE_ERROR_FORMATTED_3(ignite::IgniteError::IGNITE_ERR_MEMORY,
                    "Not enough data in the binary object", "memPtr", mem.PointerLong(),
                    "len", mem.Length(), "headerLen", impl::binary::IGNITE_OFFSET_DATA);
            }

            int8_t type = GetType();
            if (type != impl::binary::IGNITE_TYPE_OBJECT)
            {
                IGNITE_ERROR_FORMATTED_3(ignite::IgniteError::IGNITE_ERR_MEMORY,
                    "Not enough data in the binary object", "memPtr", mem.PointerLong(),
                    "type", type, "expected", impl::binary::IGNITE_TYPE_OBJECT);
            }
        }
    }
}