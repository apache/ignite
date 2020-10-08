#include <stdint.h>
#include <iostream>
#include <sstream>

#include <ignite/ignition.h>
#include <ignite/compute/compute.h>

using namespace ignite;

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

using namespace ignite;

//tag::compute-broadcast[]
/*
 * Function class.
 */
class Hello : public compute::ComputeFunc<void>
{
    friend struct ignite::binary::BinaryType<Hello>;
public:
    /*
     * Default constructor.
     */
    Hello()
    {
        // No-op.
    }

    /**
     * Callback.
     */
    virtual void Call()
    {
        std::cout << "Hello" << std::endl;
    }

};

/**
 * Binary type structure. Defines a set of functions required for type to be serialized and deserialized.
 */
namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<Hello>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("Hello");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "Hello";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const Hello& obj)
            {
                return 0;
            }

            static bool IsNull(const Hello& obj)
            {
                return false;
            }

            static void GetNull(Hello& dst)
            {
                dst = Hello();
            }

            static void Write(BinaryWriter& writer, const Hello& obj)
            {
                // No-op.
            }

            static void Read(BinaryReader& reader, Hello& dst)
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

    // Get binding instance.
    IgniteBinding binding = ignite.GetBinding();

    // Registering our class as a compute function.
    binding.RegisterComputeFunc<Hello>();

    // Broadcast to all nodes.
    compute::Compute compute = ignite.GetCompute();

    // Declaring function instance.
    Hello hello;

    // Print out hello message on nodes in the cluster group.
    compute.Broadcast(hello);
}
//end::compute-broadcast[]