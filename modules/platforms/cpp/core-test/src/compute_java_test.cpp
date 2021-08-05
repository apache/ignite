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

#include <boost/test/unit_test.hpp>
#include <boost/chrono.hpp>
#include <boost/thread.hpp>

#include <ignite/ignition.h>
#include <ignite/test_utils.h>

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cluster;
using namespace ignite::compute;
using namespace ignite::common::concurrent;
using namespace ignite::impl;
using namespace ignite_test;

using namespace boost::unit_test;

namespace
{
    /** Echo task name. */
    const std::string ECHO_TASK("org.apache.ignite.platform.PlatformComputeEchoTask");

    /** Echo type: null. */
    const int32_t ECHO_TYPE_NULL = 0;
}

/*
 * Test setup fixture.
 */
struct ComputeJavaTestSuiteFixture
{
    Ignite node;

    Ignite MakeNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        const char* config = "compute-server0-32.xml";
#else
        const char* config = "compute-server0.xml";
#endif
        return StartNode(config, name);
    }

    /*
     * Constructor.
     */
    ComputeJavaTestSuiteFixture() :
        node(MakeNode("ComputeNode1"))
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ComputeJavaTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }
};

//namespace ignite
//{
//    namespace binary
//    {
//        template<>
//        struct BinaryType<Func1> : BinaryTypeDefaultAll<Func1>
//        {
//            static void GetTypeName(std::string& dst)
//            {
//                dst = "Func1";
//            }
//
//            static void Write(BinaryWriter& writer, const Func1& obj)
//            {
//                writer.WriteInt32("a", obj.a);
//                writer.WriteInt32("b", obj.b);
//                writer.WriteObject<IgniteError>("err", obj.err);
//            }
//
//            static void Read(BinaryReader& reader, Func1& dst)
//            {
//                dst.a = reader.ReadInt32("a");
//                dst.b = reader.ReadInt32("b");
//                dst.err = reader.ReadObject<IgniteError>("err");
//            }
//        };
//    }
//}

BOOST_FIXTURE_TEST_SUITE(ComputeJavaTestSuite, ComputeJavaTestSuiteFixture)

BOOST_AUTO_TEST_CASE(EchoTaskNull)
{
    Compute compute = node.GetCompute();

    int* res = compute.ExecuteJavaTask<int*>(ECHO_TASK, ECHO_TYPE_NULL);

    BOOST_CHECK(res == 0);
}

BOOST_AUTO_TEST_SUITE_END()
