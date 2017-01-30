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

#include <ignite/ignite_error.h>

#include <ignite/impl/binary/binary_object_header.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            BinaryObjectHeader BinaryObjectHeader::FromMemory(interop::InteropMemory& mem, int32_t offset)
            {
                if ((mem.Length() - offset) < SIZE)
                {
                    IGNITE_ERROR_FORMATTED_3(ignite::IgniteError::IGNITE_ERR_MEMORY,
                        "Not enough data in the binary object", "memPtr", mem.PointerLong(),
                        "len", mem.Length(), "headerLen", SIZE);
                }

                BinaryObjectHeader hdr(mem.Data() + offset);

                int8_t type = hdr.GetType();
                if (type != impl::binary::IGNITE_TYPE_OBJECT)
                {
                    IGNITE_ERROR_FORMATTED_3(ignite::IgniteError::IGNITE_ERR_MEMORY,
                        "Not enough data in the binary object", "memPtr", mem.PointerLong(),
                        "type", type, "expected", impl::binary::IGNITE_TYPE_OBJECT);
                }

                return hdr;
            }
        }
    }
}
