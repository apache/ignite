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

#ifndef _MSC_VER
    #define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include <ignite/ignition.h>
#include <ignite/test_utils.h>

using namespace ignite;
using namespace ignite::compute;
using namespace ignite::common::concurrent;

using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct ComputeTestSuiteFixture
{
    Ignite node;

    /*
     * Constructor.
     */
    ComputeTestSuiteFixture() :
#ifdef IGNITE_TESTS_32
        node(ignite_test::StartNode("cache-test-32.xml", "ClusterTest"))
#else
        node(ignite_test::StartNode("cache-test.xml", "ClusterTest"))
#endif
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ComputeTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }
};

struct Func1 : ComputeFunc<std::string>
{
    Func1(int32_t a, int32_t b) :
        a(a), b(b)
    {
        // No-op.
    }

    virtual std::string Call()
    {
        std::stringstream tmp;

        tmp << a << '.' << b;

        return tmp.str();
    }

    int32_t a;
    int32_t b;
};

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<Func1>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("Func1");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "Func1";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static bool IsNull(const Func1& obj)
            {
                return obj.a == 0 && obj.b == 0;
            }

            static void GetNull(Func1& dst)
            {
                dst = Func1(0, 0);
            }

            static void Write(BinaryWriter& writer, const Func1& obj)
            {
                writer.WriteInt32("a", obj.a);
                writer.WriteInt32("b", obj.b);
            }

            static void Read(BinaryReader& reader, Func1& dst)
            {
                dst.a = reader.ReadInt32("a");
                dst.b = reader.ReadInt32("b");
            }
        };
    }
}

BOOST_FIXTURE_TEST_SUITE(ComputeTestSuite, ComputeTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteCallTest)
{
    Compute compute = node.GetCompute();

    BOOST_CHECKPOINT("Making Call");
    std::string res = compute.Call<std::string>(Func1(8, 5));

    BOOST_CHECK_EQUAL(res, "8.5");
}

BOOST_AUTO_TEST_SUITE_END()