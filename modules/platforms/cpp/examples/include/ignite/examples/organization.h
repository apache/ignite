/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        struct BinaryType<examples::Organization> : BinaryTypeDefaultAll<examples::Organization>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "Organization";
            }

            static void Write(BinaryWriter& writer, const examples::Organization& obj)
            {
                writer.WriteString("name", obj.name);
                writer.WriteObject<examples::Address>("addr", obj.addr);
            }

            static void Read(BinaryReader& reader, examples::Organization& dst)
            {
                dst.name = reader.ReadString("name");
                dst.addr = reader.ReadObject<examples::Address>("addr");
            }
        };
    }
}

#endif