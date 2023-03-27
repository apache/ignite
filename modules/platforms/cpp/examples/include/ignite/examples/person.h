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

        // Person key with affinity Organization info. If we want to to collocate Persons with the same value of orgId
        // we need to put struct as a cache key that contains fields: 1) id of the record (person id or serial id number)
        // 2) collocation column info (orgId). This is required because of constraint : affinity key must be part of the
        // key.
        struct PersonKey {
            PersonKey(int64_t id, int64_t orgId) : id(id), orgIdAff(orgId)
            {
                // No-op.
            }

            PersonKey() : id(0), orgIdAff(0)
            {
                // No-op.
            }

            std::string ToString() const
            {
                std::ostringstream oss;

                oss << "PersonKey [id=" << id
                    << ", orgIdAff=" << orgIdAff
                    << "]";
                return oss.str();
            }

            int64_t  id;
            int64_t  orgIdAff;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<examples::Person> : BinaryTypeDefaultAll<examples::Person>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "Person";
            }

            static void Write(BinaryWriter& writer, const examples::Person& obj)
            {
                writer.WriteInt64("orgId", obj.orgId);
                writer.WriteString("firstName", obj.firstName);
                writer.WriteString("lastName", obj.lastName);
                writer.WriteString("resume", obj.resume);
                writer.WriteDouble("salary", obj.salary);
            }

            static void Read(BinaryReader& reader, examples::Person& dst)
            {
                dst.orgId = reader.ReadInt64("orgId");
                dst.firstName = reader.ReadString("firstName");
                dst.lastName = reader.ReadString("lastName");
                dst.resume = reader.ReadString("resume");
                dst.salary = reader.ReadDouble("salary");
            }
        };

        template<>
        struct BinaryType<examples::PersonKey> : BinaryTypeDefaultAll<examples::PersonKey>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "PersonKey";
            }

            static void GetAffinityFieldName(std::string& dst)
            {
                dst = "orgIdAff";
            }

            static void Write(BinaryWriter& writer, const examples::PersonKey& obj)
            {
                writer.WriteInt64("id", obj.id);
                writer.WriteInt64("orgIdAff", obj.orgIdAff);
            }

            static void Read(BinaryReader& reader, examples::PersonKey& dst)
            {
                dst.id = reader.ReadInt64("id");
                dst.orgIdAff = reader.ReadInt64("orgIdAff");
            }
        };
    }
};

#endif // _IGNITE_EXAMPLES_PERSON