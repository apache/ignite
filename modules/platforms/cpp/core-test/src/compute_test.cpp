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
#include <boost/chrono.hpp>
#include <boost/thread.hpp>

#include <ignite/ignition.h>
#include <ignite/test_utils.h>

#include <ignite/test_utils.h>

using namespace ignite;
using namespace ignite::compute;
using namespace ignite::common::concurrent;
using namespace ignite_test;

using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct ComputeTestSuiteFixture
{
    Ignite node;

    Ignite MakeNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        const char* config = "cache-test-32.xml";
#else
        const char* config = "cache-test.xml";
#endif
        return StartNode(config, name);
    }

    /*
     * Constructor.
     */
    ComputeTestSuiteFixture() :
        node(MakeNode("ComputeNode1"))
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
    Func1() :
        a(), b(), err()
    {
        // No-op.
    }

    Func1(int32_t a, int32_t b) :
        a(a), b(b), err()
    {
        // No-op.
    }

    Func1(IgniteError err) :
        a(), b(), err(err)
    {
        // No-op.
    }

    virtual std::string Call()
    {
        if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
            throw err;

        std::stringstream tmp;

        tmp << a << '.' << b;

        return tmp.str();
    }

    int32_t a;
    int32_t b;
    IgniteError err;
};

struct Func2 : ComputeFunc<std::string>
{
    Func2() :
        a(), b(), err()
    {
        // No-op.
    }

    Func2(int32_t a, int32_t b) :
        a(a), b(b), err()
    {
        // No-op.
    }

    Func2(IgniteError err) :
        a(), b(), err(err)
    {
        // No-op.
    }

    virtual std::string Call()
    {
        boost::this_thread::sleep_for(boost::chrono::milliseconds(200));

        if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
            throw err;

        std::stringstream tmp;

        tmp << a << '.' << b;

        return tmp.str();
    }

    int32_t a;
    int32_t b;
    IgniteError err;
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
                return false;
            }

            static void GetNull(Func1& dst)
            {
                dst = Func1(0, 0);
            }

            static void Write(BinaryWriter& writer, const Func1& obj)
            {
                writer.WriteInt32("a", obj.a);
                writer.WriteInt32("b", obj.b);
                writer.WriteObject<IgniteError>("err", obj.err);
            }

            static void Read(BinaryReader& reader, Func1& dst)
            {
                dst.a = reader.ReadInt32("a");
                dst.b = reader.ReadInt32("b");
                dst.err = reader.ReadObject<IgniteError>("err");
            }
        };

        template<>
        struct BinaryType<Func2>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("Func2");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "Func2";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static bool IsNull(const Func2& obj)
            {
                return false;
            }

            static void GetNull(Func2& dst)
            {
                dst = Func2(0, 0);
            }

            static void Write(BinaryWriter& writer, const Func2& obj)
            {
                writer.WriteInt32("a", obj.a);
                writer.WriteInt32("b", obj.b);
                writer.WriteObject<IgniteError>("err", obj.err);
            }

            static void Read(BinaryReader& reader, Func2& dst)
            {
                dst.a = reader.ReadInt32("a");
                dst.b = reader.ReadInt32("b");
                dst.err = reader.ReadObject<IgniteError>("err");
            }
        };
    }
}

IGNITE_EXPORTED_CALL void IgniteModuleInit1(IgniteBindingContext& context)
{
    IgniteBinding binding = context.GetBinding();

    binding.RegisterComputeFunc<Func1>();
    binding.RegisterComputeFunc<Func2>();
}

BOOST_FIXTURE_TEST_SUITE(ComputeTestSuite, ComputeTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteCallSyncLocal)
{
    Compute compute = node.GetCompute();

    BOOST_CHECKPOINT("Making Call");
    std::string res = compute.Call<std::string>(Func1(8, 5));

    BOOST_CHECK_EQUAL(res, "8.5");
}

BOOST_AUTO_TEST_CASE(IgniteCallAsyncLocal)
{
    Compute compute = node.GetCompute();

    BOOST_CHECKPOINT("Making Call");
    Future<std::string> res = compute.CallAsync<std::string>(Func2(312, 245));

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EQUAL(res.GetValue(), "312.245");
}

BOOST_AUTO_TEST_CASE(IgniteCallSyncLocalError)
{
    Compute compute = node.GetCompute();

    BOOST_CHECKPOINT("Making Call");

    BOOST_CHECK_EXCEPTION(compute.Call<std::string>(Func1(MakeTestError())), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteCallAsyncLocalError)
{
    Compute compute = node.GetCompute();

    BOOST_CHECKPOINT("Making Call");
    Future<std::string> res = compute.CallAsync<std::string>(Func2(MakeTestError()));

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EXCEPTION(res.GetValue(), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteCallTestRemote)
{
    Ignite node2 = MakeNode("ComputeNode2");
    Compute compute = node.GetCompute();

    BOOST_CHECKPOINT("Making Call");
    compute.CallAsync<std::string>(Func2(8, 5));

    std::string res = compute.Call<std::string>(Func1(42, 24));

    BOOST_CHECK_EQUAL(res, "42.24");
}

BOOST_AUTO_TEST_CASE(IgniteCallTestRemoteError)
{
    Ignite node2 = MakeNode("ComputeNode2");
    Compute compute = node.GetCompute();

    BOOST_CHECKPOINT("Making Call");
    compute.CallAsync<std::string>(Func2(8, 5));

    Future<std::string> res = compute.CallAsync<std::string>(Func2(MakeTestError()));

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EXCEPTION(res.GetValue(), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_SUITE_END()