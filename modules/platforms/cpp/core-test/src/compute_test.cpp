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

enum { RETRIES_FOR_STABLE_TOPOLOGY = 5};

/*
 * Test setup fixture for cache affinity.
 */
struct ComputeTestSuiteFixtureAffinity
{
    static const char* cacheName;

    Ignite node0;
    Ignite node1;
    Ignite node2;

    Ignite MakeNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        const char* config = "cache-test-32.xml";
#else
        const char* config = "affinity-test.xml";
#endif
        return StartNode(config, name);
    }

    /*
     * Constructor.
     */
    ComputeTestSuiteFixtureAffinity()
    {
        node0 = MakeNode("ComputeAffinityNode0");

        BOOST_REQUIRE(WaitForRebalance0());

        node1 = MakeNode("ComputeAffinityNode1");

        BOOST_REQUIRE(WaitForRebalance1());

        node2 = MakeNode("ComputeAffinityNode2");

        BOOST_REQUIRE(WaitForRebalance2());
    }

    /*
     * Destructor.
     */
    ~ComputeTestSuiteFixtureAffinity()
    {
        Ignition::StopAll(false);
    }

    /**
     * Check whether rebalance is complete for the cluster.
     * @return true if complete.
     */
    bool IsRebalanceComplete0()
    {
        return
            node0.GetAffinity<int32_t>(cacheName).MapKeyToNode(0).IsLocal() &&
            node0.GetAffinity<int32_t>(cacheName).MapKeyToNode(1).IsLocal() &&
            node0.GetAffinity<int32_t>(cacheName).MapKeyToNode(6).IsLocal();
    }

    /**
     * Check whether rebalance is complete for the cluster.
     * @return true if complete.
     */
    bool IsRebalanceComplete1()
    {
        return
            node0.GetAffinity<int32_t>(cacheName).MapKeyToNode(0).IsLocal() &&
            node1.GetAffinity<int32_t>(cacheName).MapKeyToNode(1).IsLocal() &&
            node1.GetAffinity<int32_t>(cacheName).MapKeyToNode(6).IsLocal();
    }

    /**
     * Check whether rebalance is complete for the cluster.
     * @return true if complete.
     */
    bool IsRebalanceComplete2()
    {
        return
            node0.GetAffinity<int32_t>(cacheName).MapKeyToNode(0).IsLocal() &&
            node1.GetAffinity<int32_t>(cacheName).MapKeyToNode(1).IsLocal() &&
            node2.GetAffinity<int32_t>(cacheName).MapKeyToNode(6).IsLocal();
    }

    /**
     * Wait for rebalance.
     * @param timeout Timeout to wait.
     * @return True if condition was met, false if timeout has been reached.
     */
    bool WaitForRebalance0(int32_t timeout = 5000)
    {
        return WaitForCondition(boost::bind(&ComputeTestSuiteFixtureAffinity::IsRebalanceComplete0, this), timeout);
    }

    /**
     * Wait for rebalance.
     * @param timeout Timeout to wait.
     * @return True if condition was met, false if timeout has been reached.
     */
    bool WaitForRebalance1(int32_t timeout = 5000)
    {
        return WaitForCondition(boost::bind(&ComputeTestSuiteFixtureAffinity::IsRebalanceComplete1, this), timeout);
    }

    /**
     * Wait for rebalance.
     * @param timeout Timeout to wait.
     * @return True if condition was met, false if timeout has been reached.
     */
    bool WaitForRebalance2(int32_t timeout = 5000)
    {
        return WaitForCondition(boost::bind(&ComputeTestSuiteFixtureAffinity::IsRebalanceComplete2, this), timeout);
    }
};

const char* ComputeTestSuiteFixtureAffinity::cacheName = "test_backups_0";

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

/*
 * Test setup fixture for cluster group.
 */
struct ComputeTestSuiteFixtureClusterGroup
{
    enum NodeType {
        SERVER_NODE_ATTRIBUTE_VALUE0,
        SERVER_NODE_ATTRIBUTE_VALUE1,
        CLIENT_NODE,
    };

    Ignite server0;
    Ignite server1;
    Ignite server2;
    Ignite client;

    Ignite MakeNode(const char* name, NodeType type)
    {
        std::string config;

        switch (type) {
        case SERVER_NODE_ATTRIBUTE_VALUE0:
            config = "compute-server0.xml";
            break;

        case SERVER_NODE_ATTRIBUTE_VALUE1:
            config = "compute-server1.xml";
            break;

        case CLIENT_NODE:
            config = "compute-client.xml";
            break;
        }

#ifdef IGNITE_TESTS_32
        config.replace(config.begin() + config.find(".xml"), config.end(), "-32.xml");
#endif

        return StartNode(config.c_str(), name);
    }

    /*
     * Constructor.
     */
    ComputeTestSuiteFixtureClusterGroup() :
        server0(MakeNode("ServerNode0", SERVER_NODE_ATTRIBUTE_VALUE0)),
        server1(MakeNode("ServerNode1", SERVER_NODE_ATTRIBUTE_VALUE1)),
        server2(MakeNode("ServerNode2", SERVER_NODE_ATTRIBUTE_VALUE1)),
        client(MakeNode("ClientNode", CLIENT_NODE))
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ComputeTestSuiteFixtureClusterGroup()
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

struct Func3 : ComputeFunc<void>
{
    Func3() :
        a(), b(), err()
    {
        // No-op.
    }

    Func3(int32_t a, int32_t b) :
        a(a), b(b), err()
    {
        // No-op.
    }

    Func3(IgniteError err) :
        a(), b(), err(err)
    {
        // No-op.
    }

    virtual void Call()
    {
        boost::this_thread::sleep_for(boost::chrono::milliseconds(200));

        if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
            throw err;

        std::stringstream tmp;

        tmp << a << '.' << b;

        res = tmp.str();
    }

    int32_t a;
    int32_t b;
    IgniteError err;

    static std::string res;
};

std::string Func3::res;

void EmptyDeleter(IgniteEnvironment*)
{
    // No-op.
}

struct FuncAffinityCall : ComputeFunc<int32_t>
{
    FuncAffinityCall() :
        cacheName(), cacheKey(), err()
    {
        // No-op.
    }

    FuncAffinityCall(std::string cacheName, int32_t cacheKey) :
        cacheName(cacheName), cacheKey(cacheKey), err()
    {
        // No-op.
    }

    FuncAffinityCall(IgniteError err) :
        cacheName(), cacheKey(), err(err)
    {
        // No-op.
    }

    virtual int32_t Call()
    {
        Ignite& node = GetIgnite();

        Cache<int32_t, int32_t> cache = node.GetCache<int32_t, int32_t>(cacheName.c_str());

        return cache.LocalPeek(cacheKey, CachePeekMode::PRIMARY);
    }

    std::string cacheName;
    int32_t cacheKey;
    IgniteError err;
};

struct FuncAffinityRun : ComputeFunc<void>
{
    FuncAffinityRun() :
        cacheName(), cacheKey(), err()
    {
        // No-op.
    }

    FuncAffinityRun(std::string cacheName, int32_t cacheKey, int32_t checkKey) :
        cacheName(cacheName), cacheKey(cacheKey), checkKey(checkKey), err()
    {
        // No-op.
    }

    FuncAffinityRun(IgniteError err) :
        cacheName(), cacheKey(), err(err)
    {
        // No-op.
    }

    virtual void Call()
    {
        Ignite& node = GetIgnite();
        Cache<int32_t, int32_t> cache = node.GetCache<int32_t, int32_t>(cacheName.c_str());

        int32_t res = cache.LocalPeek(cacheKey, CachePeekMode::PRIMARY);
        cache.Put(checkKey, res);
        res = cache.Get(checkKey);
    }

    std::string cacheName;
    int32_t cacheKey;
    int32_t checkKey;
    IgniteError err;
};

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<Func1> : BinaryTypeDefaultAll<Func1>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "Func1";
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
        struct BinaryType<Func2> : BinaryTypeDefaultAll<Func2>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "Func2";
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

        template<>
        struct BinaryType<Func3> : BinaryTypeDefaultAll<Func3>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "Func3";
            }

            static void Write(BinaryWriter& writer, const Func3& obj)
            {
                writer.WriteInt32("a", obj.a);
                writer.WriteInt32("b", obj.b);
                writer.WriteObject<IgniteError>("err", obj.err);
            }

            static void Read(BinaryReader& reader, Func3& dst)
            {
                dst.a = reader.ReadInt32("a");
                dst.b = reader.ReadInt32("b");
                dst.err = reader.ReadObject<IgniteError>("err");
            }
        };

        template<>
        struct BinaryType<FuncAffinityCall> : BinaryTypeDefaultAll<FuncAffinityCall>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "FuncAffinityCall";
            }

            static void Write(BinaryWriter& writer, const FuncAffinityCall& obj)
            {
                writer.WriteString("cacheName", obj.cacheName);
                writer.WriteInt32("cacheKey", obj.cacheKey);
                writer.WriteObject<IgniteError>("err", obj.err);
            }

            static void Read(BinaryReader& reader, FuncAffinityCall& dst)
            {
                dst.cacheName = reader.ReadString("cacheName");
                dst.cacheKey = reader.ReadInt32("cacheKey");
                dst.err = reader.ReadObject<IgniteError>("err");
            }
        };

        template<>
        struct BinaryType<FuncAffinityRun> : BinaryTypeDefaultAll<FuncAffinityRun>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "FuncAffinityRun";
            }

            static void Write(BinaryWriter& writer, const FuncAffinityRun& obj)
            {
                writer.WriteString("cacheName", obj.cacheName);
                writer.WriteInt32("cacheKey", obj.cacheKey);
                writer.WriteInt32("checkKey", obj.checkKey);
                writer.WriteObject<IgniteError>("err", obj.err);
            }

            static void Read(BinaryReader& reader, FuncAffinityRun& dst)
            {
                dst.cacheName = reader.ReadString("cacheName");
                dst.cacheKey = reader.ReadInt32("cacheKey");
                dst.checkKey = reader.ReadInt32("checkKey");
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
    binding.RegisterComputeFunc<Func3>();
    binding.RegisterComputeFunc<FuncAffinityCall>();
    binding.RegisterComputeFunc<FuncAffinityRun>();
}

template<typename TK>
std::vector<int32_t> GetPrimaryKeys(size_t num, ClusterNode& node, CacheAffinity<TK>& affinity)
{
    std::vector<int32_t> ret;

    if (!num)
        return ret;

    for (int32_t i = 0; i < INT_MAX; i++)
    {
        if (affinity.IsPrimary(node, i)) {
            ret.push_back(i);

            if (ret.size() >= num)
                return ret;
        }
    }

    BOOST_CHECK(false);

    return ret;
}

BOOST_FIXTURE_TEST_SUITE(ComputeTestSuiteAffinity, ComputeTestSuiteFixtureAffinity)

BOOST_AUTO_TEST_CASE(IgniteAffinityCall)
{
    const int32_t key = 100;
    const int32_t value = 500;

    Cache<int32_t, int32_t> cache = node0.GetCache<int32_t, int32_t>(cacheName);

    cache.Put(key, value);

    CacheAffinity<int> affinity = node0.GetAffinity<int32_t>(cache.GetName());
    Compute compute = node0.GetCompute();

    ClusterNode clusterNode0 = affinity.MapKeyToNode(100);

    BOOST_TEST_CHECKPOINT("Starting local calls loop");

    std::vector<int32_t> aKeys = GetPrimaryKeys(10, clusterNode0, affinity);
    for (size_t i = 0; i < aKeys.size(); i++)
    {
        int32_t res = compute.AffinityCall<int32_t>(cache.GetName(), aKeys[i],
            FuncAffinityCall(cache.GetName(), key));

        BOOST_CHECK_EQUAL(res, value);
    }

    ClusterNode clusterNode1 = node0.GetCluster().GetLocalNode();

    if (clusterNode0.GetId() == clusterNode1.GetId())
        clusterNode1 = node1.GetCluster().GetLocalNode();

    BOOST_REQUIRE_NE(clusterNode0.GetId(), clusterNode1.GetId());
    BOOST_REQUIRE(!affinity.IsPrimary(clusterNode1, key));

    BOOST_TEST_CHECKPOINT("Starting remote calls loop");

    aKeys = GetPrimaryKeys(10, clusterNode1, affinity);
    for (size_t i = 0; i < aKeys.size(); i++)
    {
        int32_t res = compute.AffinityCall<int32_t>(cache.GetName(), aKeys[i],
            FuncAffinityCall(cache.GetName(), key));

        BOOST_CHECK_EQUAL(res, 0);
    }
}

BOOST_AUTO_TEST_CASE(IgniteAffinityCallAsync)
{
    const int32_t key = 100;
    const int32_t value = 500;

    Cache<int32_t, int32_t> cache = node0.GetCache<int32_t, int32_t>(cacheName);

    cache.Put(key, value);

    CacheAffinity<int> affinity = node0.GetAffinity<int32_t>(cache.GetName());
    Compute compute = node0.GetCompute();

    ClusterNode clusterNode0 = affinity.MapKeyToNode(100);

    BOOST_TEST_CHECKPOINT("Starting calls loop");

    std::vector<int32_t> aKeys = GetPrimaryKeys(10, clusterNode0, affinity);
    for (size_t i = 0; i < aKeys.size(); i++)
    {
        Future<int32_t> res = compute.AffinityCallAsync<int32_t>(cache.GetName(), aKeys[i],
            FuncAffinityCall(cache.GetName(), key));

        int32_t resVal = res.GetValue();
        BOOST_CHECK_EQUAL(value, resVal);
    }

    ClusterNode clusterNode1 = node0.GetCluster().GetLocalNode();

    if (clusterNode0.GetId() == clusterNode1.GetId())
        clusterNode1 = node1.GetCluster().GetLocalNode();

    BOOST_REQUIRE_NE(clusterNode0.GetId(), clusterNode1.GetId());
    BOOST_REQUIRE(!affinity.IsPrimary(clusterNode1, key));

    BOOST_TEST_CHECKPOINT("Starting remote calls loop");

    aKeys = GetPrimaryKeys(10, clusterNode1, affinity);
    for (size_t i = 0; i < aKeys.size(); i++)
    {
        Future<int32_t> res = compute.AffinityCallAsync<int32_t>(cache.GetName(), aKeys[i],
            FuncAffinityCall(cache.GetName(), key));

        int32_t resVal = res.GetValue();
        BOOST_CHECK_EQUAL(0, resVal);
    }
}

BOOST_AUTO_TEST_CASE(IgniteAffinityRun)
{
    const int32_t key = 100;
    const int32_t checkKey = -1;
    const int32_t value = 500;

    Cache<int32_t, int32_t> cache = node0.GetCache<int32_t, int32_t>(cacheName);

    cache.Put(key, value);

    CacheAffinity<int> affinity = node0.GetAffinity<int32_t>(cache.GetName());
    Compute compute = node0.GetCompute();

    ClusterNode clusterNode0 = affinity.MapKeyToNode(100);

    BOOST_TEST_CHECKPOINT("Starting calls loop");

    std::vector<int32_t> aKeys = GetPrimaryKeys(10, clusterNode0, affinity);
    for (size_t i = 0; i < aKeys.size(); i++)
    {
        compute.AffinityRun(cache.GetName(), aKeys[i],
            FuncAffinityRun(cache.GetName(), key, checkKey));

        int32_t resVal = cache.Get(checkKey);
        BOOST_CHECK_EQUAL(500, resVal);
    }

    ClusterNode clusterNode1 = node0.GetCluster().GetLocalNode();

    if (clusterNode0.GetId() == clusterNode1.GetId())
        clusterNode1 = node1.GetCluster().GetLocalNode();

    BOOST_REQUIRE_NE(clusterNode0.GetId(), clusterNode1.GetId());
    BOOST_REQUIRE(!affinity.IsPrimary(clusterNode1, key));

    BOOST_TEST_CHECKPOINT("Starting remote calls loop");

    aKeys = GetPrimaryKeys(10, clusterNode1, affinity);
    for (size_t i = 0; i < aKeys.size(); i++)
    {
        compute.AffinityRun(cache.GetName(), aKeys[i],
            FuncAffinityRun(cache.GetName(), key, checkKey));

        int32_t resVal = cache.Get(checkKey);
        BOOST_CHECK_EQUAL(0, resVal);
    }
}

BOOST_AUTO_TEST_CASE(IgniteAffinityRunAsync)
{
    const int32_t key = 100;
    const int32_t checkKey = -1;
    const int32_t value = 500;

    Cache<int32_t, int32_t> cache = node0.GetCache<int32_t, int32_t>(cacheName);

    cache.Put(key, value);

    CacheAffinity<int> affinity = node0.GetAffinity<int32_t>(cache.GetName());
    Compute compute = node0.GetCompute();

    ClusterNode clusterNode0 = affinity.MapKeyToNode(100);

    BOOST_TEST_CHECKPOINT("Starting calls loop");

    std::vector<int32_t> aKeys = GetPrimaryKeys(10, clusterNode0, affinity);
    for (size_t i = 0; i < aKeys.size(); i++)
    {
        Future<void> res = compute.AffinityRunAsync(cache.GetName(), aKeys[i],
            FuncAffinityRun(cache.GetName(), key, checkKey));

        res.GetValue();

        int32_t resVal = cache.Get(checkKey);
        BOOST_CHECK_EQUAL(500, resVal);
    }

    ClusterNode clusterNode1 = node0.GetCluster().GetLocalNode();

    if (clusterNode0.GetId() == clusterNode1.GetId())
        clusterNode1 = node1.GetCluster().GetLocalNode();

    BOOST_REQUIRE_NE(clusterNode0.GetId(), clusterNode1.GetId());
    BOOST_REQUIRE(!affinity.IsPrimary(clusterNode1, key));

    BOOST_TEST_CHECKPOINT("Starting remote calls loop");

    aKeys = GetPrimaryKeys(10, clusterNode1, affinity);
    for (size_t i = 0; i < aKeys.size(); i++)
    {
        Future<void> res = compute.AffinityRunAsync<int32_t>(cache.GetName(), aKeys[i],
            FuncAffinityRun(cache.GetName(), key, checkKey));

        res.GetValue();

        int32_t resVal = cache.Get(checkKey);
        BOOST_CHECK_EQUAL(0, resVal);
    }
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_FIXTURE_TEST_SUITE(ComputeTestSuite, ComputeTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteCallSyncLocal)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Making Call");
    std::string res = compute.Call<std::string>(Func1(8, 5));

    BOOST_CHECK_EQUAL(res, "8.5");
}

BOOST_AUTO_TEST_CASE(IgniteCallAsyncLocal)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Making Call");
    Future<std::string> res = compute.CallAsync<std::string>(Func2(312, 245));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EQUAL(res.GetValue(), "312.245");
}

BOOST_AUTO_TEST_CASE(IgniteCallSyncLocalError)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Making Call");

    BOOST_CHECK_EXCEPTION(compute.Call<std::string>(Func1(MakeTestError())), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteCallAsyncLocalError)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Making Call");
    Future<std::string> res = compute.CallAsync<std::string>(Func2(MakeTestError()));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EXCEPTION(res.GetValue(), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteCallTestRemote)
{
    Ignite node2 = MakeNode("ComputeNode2");
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Making Call");
    compute.CallAsync<std::string>(Func2(8, 5));

    std::string res = compute.Call<std::string>(Func1(42, 24));

    BOOST_CHECK_EQUAL(res, "42.24");
}

BOOST_AUTO_TEST_CASE(IgniteCallTestRemoteError)
{
    Ignite node2 = MakeNode("ComputeNode2");
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Making Call");
    compute.CallAsync<std::string>(Func2(8, 5));

    Future<std::string> res = compute.CallAsync<std::string>(Func2(MakeTestError()));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EXCEPTION(res.GetValue(), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteRunSyncLocal)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Running");
    compute.Run(Func3(8, 5));

    BOOST_CHECK_EQUAL(Func3::res, "8.5");
}

BOOST_AUTO_TEST_CASE(IgniteRunAsyncLocal)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Running");
    Future<void> res = compute.RunAsync(Func3(312, 245));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    res.GetValue();

    BOOST_CHECK_EQUAL(Func3::res, "312.245");
}

BOOST_AUTO_TEST_CASE(IgniteRunSyncLocalError)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Running");

    BOOST_CHECK_EXCEPTION(compute.Run(Func3(MakeTestError())), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteRunAsyncLocalError)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Running");
    Future<void> res = compute.RunAsync(Func3(MakeTestError()));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EXCEPTION(res.GetValue(), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteRunRemote)
{
    Ignite node2 = MakeNode("ComputeNode2");
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Running");
    compute.CallAsync<std::string>(Func2(8, 5));

    compute.Run(Func3(42, 24));

    BOOST_CHECK_EQUAL(Func3::res, "42.24");
}

BOOST_AUTO_TEST_CASE(IgniteRunRemoteError)
{
    Ignite node2 = MakeNode("ComputeNode2");
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Running");
    compute.CallAsync<std::string>(Func2(8, 5));

    Future<void> res = compute.RunAsync(Func3(MakeTestError()));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EXCEPTION(res.GetValue(), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteBroadcastLocalSync)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Broadcasting");
    std::vector<std::string> res = compute.Broadcast<std::string>(Func2(8, 5));

    BOOST_CHECK_EQUAL(res.size(), 1);
    BOOST_CHECK_EQUAL(res[0], "8.5");
}

BOOST_AUTO_TEST_CASE(IgniteBroadcastLocalAsync)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Broadcasting");
    Future< std::vector<std::string> > res = compute.BroadcastAsync<std::string>(Func2(312, 245));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    std::vector<std::string> value = res.GetValue();

    BOOST_CHECK_EQUAL(value.size(), 1);
    BOOST_CHECK_EQUAL(value[0], "312.245");
}

BOOST_AUTO_TEST_CASE(IgniteBroadcastSyncLocalError)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Broadcasting");

    BOOST_CHECK_EXCEPTION(compute.Broadcast(Func2(MakeTestError())), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteBroadcastAsyncLocalError)
{
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Broadcasting");
    Future<void> res = compute.BroadcastAsync(Func2(MakeTestError()));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EXCEPTION(res.GetValue(), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_CASE(IgniteBroadcastRemote)
{
    Ignite node2 = MakeNode("ComputeNode2");
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Broadcasting");
    std::vector<std::string> res = compute.Broadcast<std::string>(Func2(8, 5));

    BOOST_CHECK_EQUAL(res.size(), 2);
    BOOST_CHECK_EQUAL(res[0], "8.5");
    BOOST_CHECK_EQUAL(res[1], "8.5");
}

BOOST_AUTO_TEST_CASE(IgniteBroadcastRemoteError)
{
    Ignite node2 = MakeNode("ComputeNode2");
    Compute compute = node.GetCompute();

    BOOST_TEST_CHECKPOINT("Broadcasting");
    Future< std::vector<std::string> > res = compute.BroadcastAsync<std::string>(Func2(MakeTestError()));

    BOOST_CHECK(!res.IsReady());

    BOOST_TEST_CHECKPOINT("Waiting with timeout");
    res.WaitFor(100);

    BOOST_CHECK(!res.IsReady());

    BOOST_CHECK_EXCEPTION(res.GetValue(), IgniteError, IsTestError);
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_FIXTURE_TEST_SUITE(ComputeTestSuiteClusterGroup, ComputeTestSuiteFixtureClusterGroup)

BOOST_AUTO_TEST_CASE(IgniteGetClusterGroupForServers)
{
    ClusterGroup localGroup = client.GetCluster().AsClusterGroup();
    ClusterGroup group = localGroup.ForServers();

    Compute compute = client.GetCompute(group);

    BOOST_TEST_CHECKPOINT("Broadcasting");
    std::vector<std::string> res = compute.Broadcast<std::string>(Func2(8, 5));

    BOOST_CHECK_EQUAL(res.size(), 3);
    BOOST_CHECK_EQUAL(res[0], "8.5");
    BOOST_CHECK_EQUAL(res[1], "8.5");
    BOOST_CHECK_EQUAL(res[2], "8.5");
}

BOOST_AUTO_TEST_CASE(IgniteGetClusterGroupForAttribute)
{
    ClusterGroup localGroup = client.GetCluster().AsClusterGroup();
    ClusterGroup group1 = localGroup.ForAttribute("TestAttribute", "Value0");
    ClusterGroup group2 = localGroup.ForAttribute("TestAttribute", "Value1");

    Compute compute1 = client.GetCompute(group1);
    Compute compute2 = client.GetCompute(group2);

    BOOST_TEST_CHECKPOINT("Broadcasting1");
    std::vector<std::string> res1 = compute1.Broadcast<std::string>(Func2(8, 5));

    BOOST_CHECK_EQUAL(res1.size(), 1);
    BOOST_CHECK_EQUAL(res1[0], "8.5");

    BOOST_TEST_CHECKPOINT("Broadcasting2");
    std::vector<std::string> res2 = compute2.Broadcast<std::string>(Func2(8, 5));

    BOOST_CHECK_EQUAL(res2.size(), 2);
    BOOST_CHECK_EQUAL(res2[0], "8.5");
    BOOST_CHECK_EQUAL(res2[1], "8.5");
}

BOOST_AUTO_TEST_SUITE_END()
