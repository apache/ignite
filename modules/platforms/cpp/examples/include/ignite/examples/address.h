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

#include "ignite/portable/portable.h"

namespace ignite
{
    namespace examples 
    {
        struct Address 
        {
            Address()
            {
                street = "";
                zip = 0;
            }
            
            Address(std::string street, int zip) : street(street), zip(zip) 
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
    namespace portable 
    {
        template<>
        struct PortableType<ignite::examples::Address>
        {
            int32_t GetTypeId()
            {
                return GetPortableStringHashCode("Address");
            }

            std::string GetTypeName()
            {
                return "Address";
            }

            int32_t GetFieldId(const char* name)
            {
                return GetPortableStringHashCode(name);
            }

            int32_t GetHashCode(ignite::examples::Address obj)
            {
                return 0;
            }

            bool IsNull(ignite::examples::Address obj)
            {
                return false;
            }

            ignite::examples::Address GetNull()
            {
                return ignite::examples::Address("", 0);
            }

            void Write(PortableWriter& writer, ignite::examples::Address obj)
            {
                writer.WriteString("street", obj.street);
                writer.WriteInt32("zip", obj.zip);
            }

            ignite::examples::Address Read(PortableReader& reader)
            {
                std::string street = reader.ReadString("street");
                int zip = reader.ReadInt32("zip");
                
                return ignite::examples::Address(street, zip);
            }
        };    
    }    
}

#endif