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
using namespace cache;

//tag::cache-asynchronous-execution[]
/*
 * Function class.
 */
class HelloWorld : public compute::ComputeFunc<void>
{
    friend struct ignite::binary::BinaryType<HelloWorld>;
public:
    /*
     * Default constructor.
     */
    HelloWorld()
    {
        // No-op.
    }

    /**
     * Callback.
     */
    virtual void Call()
    {
        std::cout << "Job Result: Hello World" << std::endl;
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
        struct BinaryType<HelloWorld>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("HelloWorld");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "HelloWorld";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const HelloWorld& obj)
            {
                return 0;
            }

            static bool IsNull(const HelloWorld& obj)
            {
                return false;
            }

            static void GetNull(HelloWorld& dst)
            {
                dst = HelloWorld();
            }

            static void Write(BinaryWriter& writer, const HelloWorld& obj)
            {
                // No-op.
            }

            static void Read(BinaryReader& reader, HelloWorld& dst)
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
    binding.RegisterComputeFunc<HelloWorld>();

    // Get compute instance.
    compute::Compute compute = ignite.GetCompute();

    // Declaring function instance.
    HelloWorld helloWorld;

    // Making asynchronous call.
    compute.RunAsync(helloWorld);
}
//end::cache-asynchronous-execution[]
