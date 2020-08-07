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

#include <test_utils.h>

using namespace ignite::thin;
using namespace boost::unit_test;

class IgniteTxTestSuiteFixture
{
public:
    IgniteTxTestSuiteFixture()
    {
        serverNode = ignite_test::StartCrossPlatformServerNode("cache.xml", "ServerNode");
    }

    ~IgniteTxTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

private:
    /** Server node. */
    ignite::Ignite serverNode;
};

BOOST_FIXTURE_TEST_SUITE(IgniteClientTestSuite, IgniteTxTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestGetPut)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int, int> cache =
        client.GetCache<int, int>("partitioned");

    cache.Put(1, 1);

    transactions::ClientTransactions transactions = client.ClientTransactions();

    transactions::ClientTransaction tx = transactions.txStart();

    cache.Put(1, 10);

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    tx.rollback();

    BOOST_CHECK_EQUAL(1, cache.Get(1));

    tx = transactions.txStart();

    cache.Put(1, 10);

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    tx.close();

    BOOST_CHECK_EQUAL(1, cache.Get(1));
}

BOOST_AUTO_TEST_SUITE_END()
