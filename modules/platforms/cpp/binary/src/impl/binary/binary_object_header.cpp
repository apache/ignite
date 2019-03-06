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

#include <ignite/ignite_error.h>

#include <ignite/impl/binary/binary_object_header.h>
#include <ignite/impl/binary/binary_utils.h>

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
                        "len", (mem.Length() - offset), "headerLen", static_cast<int>(SIZE));
                }

                int8_t type = BinaryUtils::UnsafeReadInt8(mem, offset);
                if (type == impl::binary::IGNITE_TYPE_BINARY)
                {
                    int32_t binLen = BinaryUtils::UnsafeReadInt32(mem, offset + IGNITE_COMMON_HDR_LEN);
                    int32_t binOff = BinaryUtils::ReadInt32(mem, offset + IGNITE_BINARY_HDR_LEN + binLen);

                    return BinaryObjectHeader::FromMemory(mem, offset + IGNITE_BINARY_HDR_LEN + binOff);
                }
                else if (type != impl::binary::IGNITE_TYPE_OBJECT)
                {
                    IGNITE_ERROR_FORMATTED_3(ignite::IgniteError::IGNITE_ERR_MEMORY,
                        "Not expected type header of the binary object", "memPtr", mem.PointerLong(),
                        "type", (type & 0xFF),
                        "expected", (impl::binary::IGNITE_TYPE_OBJECT & 0xFF));
                }

                return BinaryObjectHeader(mem.Data() + offset);
            }

            int8_t* BinaryObjectHeader::GetMem()
            {
                return reinterpret_cast<int8_t*>(header);
            }
        }
    }
}
