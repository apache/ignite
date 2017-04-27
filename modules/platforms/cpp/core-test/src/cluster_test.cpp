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
using namespace ignite::common::concurrent;

using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct ClusterTestSuiteFixture
{
    Ignite node;

    /*
     * Constructor.
     */
    ClusterTestSuiteFixture() :
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
    ~ClusterTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }
};

BOOST_FIXTURE_TEST_SUITE(ClusterTestSuite, ClusterTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteImplProjection)
{
    impl::IgniteImpl* impl = impl::IgniteImpl::GetFromProxy(node);

    BOOST_REQUIRE(impl != 0);
    BOOST_REQUIRE(impl->GetProjection().IsValid());
}

BOOST_AUTO_TEST_CASE(IgniteImplForServers)
{
    impl::IgniteImpl* impl = impl::IgniteImpl::GetFromProxy(node);

    BOOST_REQUIRE(impl != 0);

    SharedPointer<impl::cluster::ClusterGroupImpl> clusterGroup = impl->GetProjection();

    BOOST_REQUIRE(clusterGroup.IsValid());

    IgniteError err;

    BOOST_REQUIRE(clusterGroup.Get()->ForServers(err).IsValid());
}

BOOST_AUTO_TEST_SUITE_END()