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

#ifndef _IGNITE_EXAMPLES_PERSON
#define _IGNITE_EXAMPLES_PERSON

#include <string>
#include <sstream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

namespace ignite
{
    namespace examples
    {
        struct Person
        {
            Person() : orgId(0), salary(.0)
            {
                // No-op.
            }

            Person(int64_t orgId, const std::string& firstName,
                const std::string& lastName, const std::string& resume, double salary) :
                orgId(orgId),
                firstName(firstName),
                lastName(lastName),
                resume(resume),
                salary(salary)
            {
                // No-op.
            }

            std::string ToString() const
            {
                std::ostringstream oss;

                oss << "Person [orgId=" << orgId
                    << ", lastName=" << lastName
                    << ", firstName=" << firstName
                    << ", salary=" << salary
                    << ", resume=" << resume << ']';

                return oss.str();
            }

            int64_t orgId;
            std::string firstName;
            std::string lastName;
            std::string resume;
            double salary;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::examples::Person)

            typedef ignite::examples::Person Person;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(Person)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(Person)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(Person)
            IGNITE_BINARY_IS_NULL_FALSE(Person)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Person)

            void Write(BinaryWriter& writer, ignite::examples::Person obj)
            {
                writer.WriteInt64("orgId", obj.orgId);
                writer.WriteString("firstName", obj.firstName);
                writer.WriteString("lastName", obj.lastName);
                writer.WriteString("resume", obj.resume);
                writer.WriteDouble("salary", obj.salary);
            }

            ignite::examples::Person Read(BinaryReader& reader)
            {
                int64_t orgId = reader.ReadInt64("orgId");
                std::string firstName = reader.ReadString("firstName");
                std::string lastName = reader.ReadString("lastName");
                std::string resume = reader.ReadString("resume");
                double salary = reader.ReadDouble("salary");

                return ignite::examples::Person(orgId, firstName, lastName, resume, salary);
            }

        IGNITE_BINARY_TYPE_END
    }
};

#endif // _IGNITE_EXAMPLES_PERSON