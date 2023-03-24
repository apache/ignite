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

/** */
struct ComputeSecurityTestSuiteFixture {
    Ignite node;

    ComputeSecurityTestSuiteFixture() : node(StartNode("compute-security.xml", "test-node")) {
        // No-op.
    }

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

struct AllowedCallable : AbstractCallable { };
struct ForbiddenCallable : AbstractCallable { };

struct AbstractRunnable : ComputeFunc<void> {
    virtual void Call() {
        // No-op.
    }
};

struct AllowedRunnable : AbstractRunnable { };
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
    // Currently we cannot validate security exception messages produced by non "affinity" compute methods
    // see https://issues.apache.org/jira/browse/IGNITE-19055
    BOOST_AUTO_TEST_CASE(TestComputeSecurity) {
        Compute compute = node.GetCompute();

        BOOST_CHECK_EQUAL(42, compute.Call<int>(AllowedCallable()));
        BOOST_CHECK_THROW(compute.Call<int>(ForbiddenCallable()), IgniteError);

        BOOST_CHECK_EQUAL(42, compute.CallAsync<int>(AllowedCallable()).GetValue());
        BOOST_CHECK_THROW(compute.CallAsync<int>(ForbiddenCallable()).GetValue(), IgniteError);

        BOOST_CHECK_EQUAL(42, compute.AffinityCall<int>("default", 0, AllowedCallable()));
        BOOST_CHECK_THROW(compute.AffinityCall<int>("default", 0, ForbiddenCallable()), IgniteError);

        BOOST_CHECK_EQUAL(42, compute.AffinityCallAsync<int>("default", 0, AllowedCallable()).GetValue());
        BOOST_CHECK_THROW(compute.AffinityCallAsync<int>("default", 0, ForbiddenCallable()).GetValue(), IgniteError);

        compute.Run(AllowedRunnable());
        BOOST_CHECK_THROW(compute.Run(ForbiddenRunnable()), IgniteError);

        compute.RunAsync(AllowedRunnable()).GetValue();
        BOOST_CHECK_THROW(compute.RunAsync(ForbiddenRunnable()).GetValue(), IgniteError);

        compute.AffinityRun("default", 0, AllowedRunnable());
        BOOST_CHECK_THROW(compute.AffinityRun("default", 0, ForbiddenRunnable()), IgniteError);

        compute.AffinityRunAsync("default", 0, AllowedRunnable()).GetValue();
        BOOST_CHECK_THROW(compute.AffinityRunAsync("default", 0, ForbiddenRunnable()).GetValue(), IgniteError);

        BOOST_CHECK_EQUAL(42, compute.Broadcast<int>(AllowedCallable()).front());
        BOOST_CHECK_THROW(compute.Broadcast<int>(ForbiddenCallable()), IgniteError);

        BOOST_CHECK_EQUAL(42, compute.BroadcastAsync<int>(AllowedCallable()).GetValue().front());
        BOOST_CHECK_THROW(compute.BroadcastAsync<int>(ForbiddenCallable()).GetValue(), IgniteError);

        compute.Broadcast(AllowedRunnable());
        BOOST_CHECK_THROW(compute.Broadcast(ForbiddenRunnable()), IgniteError);

        compute.BroadcastAsync(AllowedRunnable()).GetValue();
        BOOST_CHECK_THROW(compute.BroadcastAsync(ForbiddenRunnable()).GetValue(), IgniteError);
    }

BOOST_AUTO_TEST_SUITE_END()
