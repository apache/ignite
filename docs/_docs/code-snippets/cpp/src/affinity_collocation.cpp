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

#include <stdint.h>

#include "ignite/ignite.h"
#include "ignite/ignition.h"


//tag::affinity-collocation[]
struct Person
{
    int32_t id;
    std::string name;
    int32_t cityId;
    std::string companyId;
};

struct PersonKey
{
    int32_t id;
    std::string companyId;
};

struct Company
{
    std::string name;
};

namespace ignite { namespace binary {
template<> struct BinaryType<Person> : BinaryTypeDefaultAll<Person>
{
    static void GetTypeName(std::string& dst)
    {
        dst = "Person";
    }

    static void Write(BinaryWriter& writer, const Person& obj)
    {
        writer.WriteInt32("id", obj.id);
        writer.WriteString("name", obj.name);
        writer.WriteInt32("cityId", obj.cityId);
        writer.WriteString("companyId", obj.companyId);
    }

    static void Read(BinaryReader& reader, Person& dst)
    {
        dst.id = reader.ReadInt32("id");
        dst.name = reader.ReadString("name");
        dst.cityId = reader.ReadInt32("cityId");
        dst.companyId = reader.ReadString("companyId");
    }
};

template<> struct BinaryType<PersonKey> : BinaryTypeDefaultAll<PersonKey>
{
    static void GetTypeName(std::string& dst)
    {
        dst = "PersonKey";
    }

    static void GetAffinityFieldName(std::string& dst)
    {
        dst = "companyId";
    }

    static void Write(BinaryWriter& writer, const PersonKey& obj)
    {
        writer.WriteInt32("id", obj.id);
        writer.WriteString("companyId", obj.companyId);
    }

    static void Read(BinaryReader& reader, PersonKey& dst)
    {
        dst.id = reader.ReadInt32("id");
        dst.companyId = reader.ReadString("companyId");
    }
};

template<> struct BinaryType<Company> : BinaryTypeDefaultAll<Company>
{
    static void GetTypeName(std::string& dst)
    {
        dst = "Company";
    }

    static void Write(BinaryWriter& writer, const Company& obj)
    {
        writer.WriteString("name", obj.name);
    }

    static void Read(BinaryReader& reader, Company& dst)
    {
        dst.name = reader.ReadString("name");
    }
};
}};  // namespace ignite::binary

int main()
{
    using namespace ignite;
    using namespace cache;

    IgniteConfiguration cfg;
    Ignite ignite = Ignition::Start(cfg);

    Cache<PersonKey, Person> personCache = ignite.GetOrCreateCache<PersonKey, Person>("person");
    Cache<std::string, Company> companyCache = ignite.GetOrCreateCache<std::string, Company>("company");

    Person person{};
    person.name = "Vasya";

    Company company{};
    company.name = "Company1";

    personCache.Put(PersonKey{1, "company1_key"}, person);
    companyCache.Put("company1_key", company);

    return 0;
}
//end::affinity-collocation[]
