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

#ifndef _IGNITE_EXAMPLES_ORGANIZATION
#define _IGNITE_EXAMPLES_ORGANIZATION

#include "ignite/binary/binary.h"

#include "ignite/examples/address.h"

namespace ignite
{
    namespace examples
    {
        struct Organization
        {
            Organization() :
                name(), addr()
            {
                // No-op.
            }

            Organization(const std::string& name) :
                name(name), addr()
            {
                // No-op.
            }

            Organization(const std::string& name, Address addr) :
                name(name), addr(addr)
            {
                // No-op.
            }

            std::string ToString() 
            {
                std::ostringstream oss;

                oss << "Organization [name=" << name << ", Address=" << addr.ToString() << "]";

                return oss.str();
            }

            std::string name;
            Address addr;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<ignite::examples::Organization>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("Organization");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "Organization";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const ignite::examples::Organization& obj)
            {
                return 0;
            }

            static bool IsNull(const ignite::examples::Organization& obj)
            {
                return false;
            }

            static void GetNull(ignite::examples::Organization& dst)
            {
                dst = ignite::examples::Organization("", ignite::examples::Address());
            }

            static void Write(BinaryWriter& writer, const ignite::examples::Organization& obj)
            {
                writer.WriteString("name", obj.name);
                writer.WriteObject<ignite::examples::Address>("addr", obj.addr);
            }

            static void Read(BinaryReader& reader, ignite::examples::Organization& dst)
            {
                dst.name = reader.ReadString("name");
                dst.addr = reader.ReadObject<ignite::examples::Address>("addr");
            }
        };
    }
}

#endif