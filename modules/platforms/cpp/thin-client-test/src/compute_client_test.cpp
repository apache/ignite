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

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <ignite/complex_type.h>
#include <test_utils.h>

using namespace ignite::thin;
using namespace boost::unit_test;

namespace
{
    /** Echo task name. */
    const std::string ECHO_TASK("org.apache.ignite.platform.PlatformComputeEchoTask");

    /** Test task. */
    const std::string TEST_TASK("org.apache.ignite.internal.client.thin.TestTask");

    /** Test failover task. */
    const std::string TEST_FAILOVER_TASK("org.apache.ignite.internal.client.thin.TestFailoverTask");

    /** Test result cache task. */
    const std::string TEST_RESULT_CACHE_TASK("org.apache.ignite.internal.client.thin.TestResultCacheTask");

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

class ComputeClientTestSuiteFixture
{
public:
    static ignite::Ignite StartNode(const char* name)
    {
        return ignite_test::StartCrossPlatformServerNode("compute.xml", name);
    }

    ComputeClientTestSuiteFixture()
    {
        serverNode1 = StartNode("ServerNode1");
        serverNode2 = StartNode("ServerNode2");

        IgniteClientConfiguration cfg;
        cfg.SetEndPoints("127.0.0.1:11110");

        client = IgniteClient::Start(cfg);

        compute = client.GetCompute();
    }

    ~ComputeClientTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

    /**
     * Get default cache.
     *
     * @return Default cache.
     */
    template<typename T>
    cache::CacheClient<int32_t, T> GetDefaultCache()
    {
        return client.GetOrCreateCache<int32_t, T>("default");
    }

protected:
    /** Server node 1. */
    ignite::Ignite serverNode1;

    /** Server node 2. */
    ignite::Ignite serverNode2;

    /** Client. */
    IgniteClient client;

    /** Client compute. */
    compute::ComputeClient compute;
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

BOOST_FIXTURE_TEST_SUITE(ComputeClientTestSuite, ComputeClientTestSuiteFixture)

BOOST_AUTO_TEST_CASE(EchoTaskNull)
{
    int* res = compute.ExecuteJavaTask<int*>(ECHO_TASK, ECHO_TYPE_NULL);

    BOOST_CHECK(res == 0);
}

BOOST_AUTO_TEST_CASE(EchoTaskPrimitives)
{
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
    cache::CacheClient<int32_t, int32_t> cache = GetDefaultCache<int32_t>();

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
    cache::CacheClient<int32_t, ignite::Guid> cache = GetDefaultCache<ignite::Guid>();

    for (int32_t i = 0; i < 100; ++i)
    {
        ignite::Guid value(i * 479001599LL, i * 150209LL);

        cache.Put(ECHO_TYPE_UUID, value);

        ignite::Guid res = compute.ExecuteJavaTask<ignite::Guid>(ECHO_TASK, ECHO_TYPE_UUID);

        BOOST_CHECK_EQUAL(value, res);
    }
}

/**
 * Checks if the error is of type IgniteError::IGNITE_ERR_FUTURE_STATE.
 */
bool IsTimeoutError(const ignite::IgniteError& err)
{
    std::string msgErr(err.GetText());
    std::string expected("Task timed out");

    return msgErr.find(expected) != std::string::npos;
}

BOOST_AUTO_TEST_CASE(TaskWithTimeout)
{
    const int64_t timeout = 500;
    const int64_t taskTimeout = timeout * 100;

    compute::ComputeClient tmCompute = compute.WithTimeout(timeout);

    BOOST_CHECK_EXCEPTION(tmCompute.ExecuteJavaTask<ignite::Guid>(TEST_TASK, taskTimeout),
        ignite::IgniteError, IsTimeoutError);
}

BOOST_AUTO_TEST_CASE(TaskWithNoFailover)
{
    compute::ComputeClient computeWithNoFailover = compute.WithNoFailover();

    BOOST_CHECK(compute.ExecuteJavaTask<bool>(TEST_FAILOVER_TASK));
    BOOST_CHECK(!computeWithNoFailover.ExecuteJavaTask<bool>(TEST_FAILOVER_TASK));
}

BOOST_AUTO_TEST_CASE(TaskWithNoResultCache)
{
    compute::ComputeClient computeWithNoResultCache = compute.WithNoResultCache();

    BOOST_CHECK(compute.ExecuteJavaTask<bool>(TEST_RESULT_CACHE_TASK));
    BOOST_CHECK(!computeWithNoResultCache.ExecuteJavaTask<bool>(TEST_RESULT_CACHE_TASK));
}

BOOST_AUTO_TEST_SUITE_END()
