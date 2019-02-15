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

#ifndef _IGNITE_EXAMPLES_ADDRESS
#define _IGNITE_EXAMPLES_ADDRESS

#include "ignite/binary/binary.h"

namespace ignite
{
    namespace examples
    {
        struct Address
        {
            Address() : street(), zip(0)
            {
                // No-op.
            }

            Address(const std::string& street, int zip) :
                street(street), zip(zip)
            {
                // No-op.
            }

            std::string ToString() 
            {
                std::ostringstream oss;

                oss << "Address [street=" << street << ", zip=" << zip << "]";

                return oss.str();
            }

            std::string street;
            int zip;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<ignite::examples::Address>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("Address");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "Address";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(ignite::examples::Address& obj)
            {
                return 0;
            }

            static bool IsNull(ignite::examples::Address obj)
            {
                return false;
            }

            static void GetNull(ignite::examples::Address& dst)
            {
                dst = ignite::examples::Address("", 0);
            }

            static void Write(BinaryWriter& writer, const ignite::examples::Address& obj)
            {
                writer.WriteString("street", obj.street);
                writer.WriteInt32("zip", obj.zip);
            }

            static void Read(BinaryReader& reader, ignite::examples::Address& dst)
            {
                dst.street = reader.ReadString("street");
                dst.zip = reader.ReadInt32("zip");
            }
        };
    }
}

#endif