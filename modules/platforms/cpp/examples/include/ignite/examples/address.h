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

            static int32_t GetHashCode(ignite::examples::Address obj)
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