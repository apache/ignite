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

#ifndef _IGNITE_EXAMPLES_ORGANIZATION
#define _IGNITE_EXAMPLES_ORGANIZATION

#include "ignite/portable/portable.h"

#include "ignite/examples/address.h"

namespace ignite
{
    namespace examples 
    {
        struct Organization 
        {
            Organization()
            {
                name = "";
                addr = Address();
            }
            
            Organization(std::string name, Address addr) : name(name), addr(addr) 
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
    namespace portable 
    {
        template<>
        struct PortableType<ignite::examples::Organization>
        {
            int32_t GetTypeId()
            {
                return GetPortableStringHashCode("Organization");
            }

            std::string GetTypeName()
            {
                return "Organization";
            }

            int32_t GetFieldId(const char* name)
            {
                return GetPortableStringHashCode(name);
            }

            int32_t GetHashCode(ignite::examples::Organization obj)
            {
                return 0;
            }

            bool IsNull(ignite::examples::Organization obj)
            {
                return false;
            }

            ignite::examples::Organization GetNull()
            {
                return ignite::examples::Organization("", ignite::examples::Address());
            }

            void Write(PortableWriter& writer, ignite::examples::Organization obj)
            {
                writer.WriteString("name", obj.name);
                writer.WriteObject<ignite::examples::Address>("addr", obj.addr);
            }

            ignite::examples::Organization Read(PortableReader& reader)
            {
                std::string name = reader.ReadString("name");
                ignite::examples::Address addr = reader.ReadObject<ignite::examples::Address>("addr");
                                
                return ignite::examples::Organization(name, addr);
            }
        };    
    }    
}

#endif