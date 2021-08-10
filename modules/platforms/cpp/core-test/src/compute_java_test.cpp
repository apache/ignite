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

    /** Node name task name. */
    const std::string NODE_NAME_TASK("org.apache.ignite.platform.PlatformComputeNodeNameTask");

    /** Echo type: null. */
    const int32_t ECHO_TYPE_NULL = 0;

    /** Echo type: byte. */
    const int32_t ECHO_TYPE_BYTE = 1;

    /** Echo type: bool. */
    const int32_t ECHO_TYPE_BOOL = 2;

    /** Echo type: short. */
    const int32_t ECHO_TYPE_SHORT = 3;

    /** Echo type: char. */
    const int32_t ECHO_TYPE_CHAR = 4;

    /** Echo type: int. */
    const int32_t ECHO_TYPE_INT = 5;

    /** Echo type: long. */
    const int32_t ECHO_TYPE_LONG = 6;

    /** Echo type: float. */
    const int32_t ECHO_TYPE_FLOAT = 7;

    /** Echo type: double. */
    const int32_t ECHO_TYPE_DOUBLE = 8;

    /** Echo type: object. */
    const int32_t ECHO_TYPE_OBJECT = 12;

    /** Echo type: uuid. */
    const int32_t ECHO_TYPE_UUID = 22;
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

    /**
     * Get default cache.
     *
     * @return Default cache.
     */
    template<typename T>
    Cache<int32_t, T> GetDefaultCache()
    {
        return node.GetOrCreateCache<int32_t, T>("default");
    }
};

/**
 * Binarizable object for task tests.
 */
class PlatformComputeBinarizable
{
public:
    /**
     * Constructor.
     */
    PlatformComputeBinarizable()
    {
        // No-op.
    }

    /**
     * Constructor,
     *
     * @param field Field.
     */
    PlatformComputeBinarizable(int32_t field) :
        field(field)
    {
        // No-op.
    }

    /** Field. */
    int32_t field;
};


namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<PlatformComputeBinarizable> : BinaryTypeDefaultAll<PlatformComputeBinarizable>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "PlatformComputeBinarizable";
            }

            static void Write(BinaryWriter& writer, const PlatformComputeBinarizable& obj)
            {
                writer.WriteInt32("field", obj.field);
            }

            static void Read(BinaryReader& reader, PlatformComputeBinarizable& dst)
            {
                dst.field = reader.ReadInt32("field");
            }
        };
    }
}

BOOST_FIXTURE_TEST_SUITE(ComputeJavaTestSuite, ComputeJavaTestSuiteFixture)

BOOST_AUTO_TEST_CASE(EchoTaskNull)
{
    Compute compute = node.GetCompute();

    int* res = compute.ExecuteJavaTask<int*>(ECHO_TASK, ECHO_TYPE_NULL);

    BOOST_CHECK(res == 0);
}

BOOST_AUTO_TEST_CASE(EchoTaskPrimitives)
{
    Compute compute = node.GetCompute();

    BOOST_CHECK_EQUAL(1, compute.ExecuteJavaTask<int8_t>(ECHO_TASK, ECHO_TYPE_BYTE));
    BOOST_CHECK_EQUAL(true, compute.ExecuteJavaTask<bool>(ECHO_TASK, ECHO_TYPE_BOOL));
    BOOST_CHECK_EQUAL(1, compute.ExecuteJavaTask<int16_t>(ECHO_TASK, ECHO_TYPE_SHORT));
    BOOST_CHECK_EQUAL(1, compute.ExecuteJavaTask<uint16_t>(ECHO_TASK, ECHO_TYPE_CHAR));
    BOOST_CHECK_EQUAL(1, compute.ExecuteJavaTask<int32_t>(ECHO_TASK, ECHO_TYPE_INT));
    BOOST_CHECK_EQUAL(1LL, compute.ExecuteJavaTask<int64_t>(ECHO_TASK, ECHO_TYPE_LONG));
    BOOST_CHECK_EQUAL(1.0f, compute.ExecuteJavaTask<float>(ECHO_TASK, ECHO_TYPE_FLOAT));
    BOOST_CHECK_EQUAL(1.0, compute.ExecuteJavaTask<double>(ECHO_TASK, ECHO_TYPE_DOUBLE));
}

BOOST_AUTO_TEST_CASE(EchoTaskObject)
{
    Compute compute = node.GetCompute();
    Cache<int32_t, int32_t> cache = GetDefaultCache<int32_t>();

    for (int32_t i = 0; i < 100; ++i)
    {
        int32_t value = i * 42;
        cache.Put(ECHO_TYPE_OBJECT, value);

        PlatformComputeBinarizable res =
            compute.ExecuteJavaTask<PlatformComputeBinarizable>(ECHO_TASK, ECHO_TYPE_OBJECT);

        BOOST_CHECK_EQUAL(value, res.field);
    }
}

BOOST_AUTO_TEST_CASE(EchoTaskGuid)
{
    Compute compute = node.GetCompute();
    Cache<int32_t, ignite::Guid> cache = GetDefaultCache<ignite::Guid>();

    for (int32_t i = 0; i < 100; ++i)
    {
        Guid value(i * 406586897, i * 87178291199);

        cache.Put(ECHO_TYPE_UUID, value);

        ignite::Guid res = compute.ExecuteJavaTask<ignite::Guid>(ECHO_TASK, ECHO_TYPE_UUID);

        BOOST_CHECK_EQUAL(value, res);
    }
}

BOOST_AUTO_TEST_CASE(Cluster)
{
    Ignite node2 = MakeNode("ComputeNode2");

    Compute compute = node.GetCompute(node.GetCluster().ForLocal());

    for (int32_t i = 0; i < 100; ++i)
    {
        std::string res = compute.ExecuteJavaTask<std::string>(NODE_NAME_TASK);

        BOOST_CHECK_EQUAL(std::string(node.GetName()), res);
    }
}

BOOST_AUTO_TEST_SUITE_END()
