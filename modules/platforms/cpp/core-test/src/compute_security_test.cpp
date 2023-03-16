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

// Currently we cannot validate exceptions produced by non "affinity" compute methods
// see https://issues.apache.org/jira/browse/IGNITE-19055
#define CHECK_COMPUTE_FAILED(doCompute)                                                                 \
{                                                                                                       \
    int32_t errCode = -1;                                                                               \
                                                                                                        \
    try {                                                                                               \
        doCompute;                                                                                      \
    }                                                                                                   \
    catch (IgniteError& err) {                                                                          \
        errCode = err.GetCode();                                                                        \
                                                                                                        \
        if (errCode == IgniteError::IGNITE_ERR_GENERIC) {                                               \
            BOOST_ASSERT(std::string(err.GetText()).find("Authorization failed") != std::string::npos); \
        }                                                                                               \
    }                                                                                                   \
                                                                                                        \
    BOOST_ASSERT(errCode != -1);                                                                        \
}                                                                                                       \

/** */
struct ComputeSecurityTestSuiteFixture {
    /** */
    Ignite node;

    /** */
    ComputeSecurityTestSuiteFixture() : node(StartNode("compute-security.xml", "test-node")) {
        // No-op.
    }

    /** */
    ~ComputeSecurityTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }
};

struct AbstractCallable : ComputeFunc<int> {
    virtual int Call() {
        return 42;
    }
};

/** */
struct AllowedCallable : AbstractCallable { };

/** */
struct ForbiddenCallable : AbstractCallable { };

/** */
struct AbstractRunnable : ComputeFunc<void> {
    virtual void Call() {
        // No-op.
    }
};

/** */
struct AllowedRunnable : AbstractRunnable { };

/** */
struct ForbiddenRunnable : AbstractRunnable { };

namespace ignite {
    namespace binary {
        template<>
        struct BinaryType<AllowedCallable> : BinaryTypeDefaultAll<AllowedCallable> {
            static void GetTypeName(std::string &dst) {
                dst = "AllowedCallable";
            }

            static void Write(BinaryWriter&, const AllowedCallable&) {}

            static void Read(BinaryReader&, AllowedCallable&) {}
        };

        template<>
        struct BinaryType<ForbiddenCallable> : BinaryTypeDefaultAll<ForbiddenCallable> {
            static void GetTypeName(std::string &dst) {
                dst = "ForbiddenCallable";
            }

            static void Write(BinaryWriter&, const ForbiddenCallable&) {}

            static void Read(BinaryReader&, ForbiddenCallable&) {}
        };

        template<>
        struct BinaryType<AllowedRunnable> : BinaryTypeDefaultAll<AllowedRunnable> {
            static void GetTypeName(std::string &dst) {
                dst = "AllowedRunnable";
            }

            static void Write(BinaryWriter&, const AllowedRunnable&) {}

            static void Read(BinaryReader&, AllowedRunnable&) {}
        };

        template<>
        struct BinaryType<ForbiddenRunnable> : BinaryTypeDefaultAll<ForbiddenRunnable> {
            static void GetTypeName(std::string &dst) {
                dst = "ForbiddenRunnable";
            }

            static void Write(BinaryWriter&, const ForbiddenRunnable&) {}

            static void Read(BinaryReader&, ForbiddenRunnable&) {}
        };
    }
}

BOOST_FIXTURE_TEST_SUITE(ComputeSecurityTestSuite, ComputeSecurityTestSuiteFixture)

    BOOST_AUTO_TEST_CASE(TestComputeSecurity) {
        Compute compute = node.GetCompute();


        BOOST_ASSERT(42 == compute.Call<int>(AllowedCallable()));
        CHECK_COMPUTE_FAILED(compute.Call<int>(ForbiddenCallable()));

        BOOST_ASSERT(42 == compute.CallAsync<int>(AllowedCallable()).GetValue());
        CHECK_COMPUTE_FAILED(compute.CallAsync<int>(ForbiddenCallable()).GetValue());

        BOOST_ASSERT(42 == compute.AffinityCall<int>("default", 0, AllowedCallable()));
        CHECK_COMPUTE_FAILED(compute.AffinityCall<int>("default", 0, ForbiddenCallable()));

        BOOST_ASSERT(42 == compute.AffinityCallAsync<int>("default", 0, AllowedCallable()).GetValue());
        CHECK_COMPUTE_FAILED(compute.AffinityCallAsync<int>("default", 0, ForbiddenCallable()).GetValue());

        compute.Run(AllowedRunnable());
        CHECK_COMPUTE_FAILED(compute.Run(ForbiddenRunnable()));

        compute.RunAsync(AllowedRunnable()).GetValue();
        CHECK_COMPUTE_FAILED(compute.RunAsync(ForbiddenRunnable()).GetValue());

        compute.AffinityRun("default", 0, AllowedRunnable());
        CHECK_COMPUTE_FAILED(compute.AffinityRun("default", 0, ForbiddenRunnable()));

        compute.AffinityRunAsync("default", 0, AllowedRunnable()).GetValue();
        CHECK_COMPUTE_FAILED(compute.AffinityRunAsync("default", 0, ForbiddenRunnable()).GetValue());

        BOOST_ASSERT(42 == compute.Broadcast<int>(AllowedCallable()).front());
        CHECK_COMPUTE_FAILED(compute.Broadcast<int>(ForbiddenCallable()));

        BOOST_ASSERT(42 == compute.BroadcastAsync<int>(AllowedCallable()).GetValue().front());
        CHECK_COMPUTE_FAILED(compute.BroadcastAsync<int>(ForbiddenCallable()).GetValue());

        compute.Broadcast(AllowedRunnable());
        CHECK_COMPUTE_FAILED(compute.Broadcast(ForbiddenRunnable()));

        compute.BroadcastAsync(AllowedRunnable()).GetValue();
        CHECK_COMPUTE_FAILED(compute.BroadcastAsync(ForbiddenRunnable()).GetValue());
    }

BOOST_AUTO_TEST_SUITE_END()
