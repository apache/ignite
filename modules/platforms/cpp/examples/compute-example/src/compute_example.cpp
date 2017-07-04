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

/*
 * Function class.
 */
class CountWords : public compute::ComputeFunc<int32_t>
{
    friend struct ignite::binary::BinaryType<CountWords>;
public:
    /*
     * Default constructor.
     */
    CountWords() :
        text()
    {
        // No-op.
    }

    /*
     * Constructor.
     *
     * @param text Text.
     */
    CountWords(const std::string& text) :
        text(text)
    {
        // No-op.
    }

    /**
     * Callback.
     * Counts number of words in provided text.
     *
     * @return Number of words in provided text.
     */
    virtual int32_t Call()
    {
        std::stringstream buf(text);
        std::string word;

        int32_t wordsCount = 0;
        while (buf >> word)
            ++wordsCount;

        return wordsCount;
    }

private:
    /** Text in which to count words. */
    std::string text;
};

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<CountWords>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("CountWords");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "CountWords";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const CountWords& obj)
            {
                return 0;
            }

            static bool IsNull(const CountWords& obj)
            {
                return false;
            }

            static void GetNull(CountWords& dst)
            {
                dst = CountWords("");
            }

            static void Write(BinaryWriter& writer, const CountWords& obj)
            {
                writer.RawWriter().WriteString(obj.text);
            }

            static void Read(BinaryReader& reader, CountWords& dst)
            {
                dst.text = reader.RawReader().ReadString();
            }
        };
    }
}

int main()
{
    IgniteConfiguration cfg;

    cfg.springCfgPath = "platforms/cpp/examples/compute-example/config/compute-example.xml";

    try
    {
        // Start a node.
        Ignite ignite = Ignition::Start(cfg);

        std::cout << std::endl;
        std::cout << ">>> Compute example started." << std::endl;
        std::cout << std::endl;

        // Get binding instance.
        IgniteBinding binding = ignite.GetBinding();

        // Registering our class as a compute function.
        binding.RegisterComputeFunc<CountWords>();

        // Get compute instance.
        compute::Compute compute = ignite.GetCompute();

        std::string testText = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.";

        // Declaring counter.
        CountWords counter(testText);

        // Making call.
        int32_t wordsCount = compute.Call<int32_t>(counter);

        // Printing result.
        std::cout << ">>> Text contains " << wordsCount << " words" << std::endl;

        // Stop node.
        Ignition::StopAll(false);
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;

        return err.GetCode();
    }

    std::cout << std::endl;
    std::cout << ">>> Example finished, press 'Enter' to exit ..." << std::endl;
    std::cout << std::endl;

    std::cin.get();

    return 0;
}
