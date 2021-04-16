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
#include <iostream>
#include <sstream>

#include <ignite/ignition.h>
#include <ignite/compute/compute.h>
#include "person.h"

using namespace ignite;
using namespace cache;

//tag::compute-acessing-data[]
/*
 * Function class.
 */
class GetValue : public compute::ComputeFunc<void>
{
    friend struct ignite::binary::BinaryType<GetValue>;
public:
    /*
     * Default constructor.
     */
    GetValue()
    {
        // No-op.
    }

    /**
     * Callback.
     */
    virtual void Call()
    {
        Ignite& node = GetIgnite();

        // Get the data you need
        Cache<int64_t, Person> cache = node.GetCache<int64_t, Person>("person");

        // do with the data what you need to do
        Person person = cache.Get(1);
    }
};
//end::compute-acessing-data[]

/**
 * Binary type structure. Defines a set of functions required for type to be serialized and deserialized.
 */
namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<GetValue>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("GetValue");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "GetValue";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const GetValue& obj)
            {
                return 0;
            }

            static bool IsNull(const GetValue& obj)
            {
                return false;
            }

            static void GetNull(GetValue& dst)
            {
                dst = GetValue();
            }

            static void Write(BinaryWriter& writer, const GetValue& obj)
            {
                // No-op.
            }

            static void Read(BinaryReader& reader, GetValue& dst)
            {
                // No-op.
            }
        };
    }
}

int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    Cache<int64_t, Person> cache = ignite.GetOrCreateCache<int64_t, Person>("person");
    cache.Put(1, Person(1, "first", "last", "resume", 100.00));

    // Get binding instance.
    IgniteBinding binding = ignite.GetBinding();

    // Registering our class as a compute function.
    binding.RegisterComputeFunc<GetValue>();

    // Get compute instance.
    compute::Compute compute = ignite.GetCompute();

    // Run compute task.
    compute.Run(GetValue());
}
