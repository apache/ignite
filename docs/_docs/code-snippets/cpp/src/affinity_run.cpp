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

//tag::affinity-run[]
/*
 * Function class.
 */
struct FuncAffinityRun : compute::ComputeFunc<void>
{
    /*
    * Default constructor.
    */
    FuncAffinityRun()
    {
        // No-op.
    }

    /*
    * Parameterized constructor.
    */
    FuncAffinityRun(std::string cacheName, int32_t key) :
        cacheName(cacheName), key(key)
    {
        // No-op.
    }

    /**
     * Callback.
     */
    virtual void Call()
    {
        Ignite& node = GetIgnite();

        Cache<int32_t, std::string> cache = node.GetCache<int32_t, std::string>(cacheName.c_str());

        // Peek is a local memory lookup.
        std::cout << "Co-located [key= " << key << ", value= " << cache.LocalPeek(key, CachePeekMode::ALL) << "]" << std::endl;
    }

    std::string cacheName;
    int32_t key;
};

/**
 * Binary type structure. Defines a set of functions required for type to be serialized and deserialized.
 */
namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<FuncAffinityRun>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("FuncAffinityRun");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "FuncAffinityRun";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const FuncAffinityRun& obj)
            {
                return 0;
            }

            static bool IsNull(const FuncAffinityRun& obj)
            {
                return false;
            }

            static void GetNull(FuncAffinityRun& dst)
            {
                dst = FuncAffinityRun();
            }

            static void Write(BinaryWriter& writer, const FuncAffinityRun& obj)
            {
                writer.WriteString("cacheName", obj.cacheName);
                writer.WriteInt32("key", obj.key);
            }

            static void Read(BinaryReader& reader, FuncAffinityRun& dst)
            {
                dst.cacheName = reader.ReadString("cacheName");
                dst.key = reader.ReadInt32("key");
            }
        };
    }
}


int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    // Get cache instance.
    Cache<int32_t, std::string> cache = ignite.GetOrCreateCache<int32_t, std::string>("myCache");

    // Get binding instance.
    IgniteBinding binding = ignite.GetBinding();

    // Registering our class as a compute function.
    binding.RegisterComputeFunc<FuncAffinityRun>();

    // Get compute instance.
    compute::Compute compute = ignite.GetCompute();

    int key = 1;

    // This closure will execute on the remote node where
    // data for the given 'key' is located.
    compute.AffinityRun(cache.GetName(), key, FuncAffinityRun(cache.GetName(), key));
}
//end::affinity-run[]
