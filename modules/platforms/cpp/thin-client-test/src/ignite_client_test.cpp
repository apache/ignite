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
#include <boost/bind.hpp>

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <test_utils.h>

using namespace ignite::thin;
using namespace boost::unit_test;

class IgniteClientTestSuiteFixture
{
public:
    IgniteClientTestSuiteFixture()
    {
        // No-op.
    }

    ~IgniteClientTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

    /**
     * Wait for connections.
     * @return True if condition was met, false if timeout has been reached.
     */
    bool WaitForConnections(size_t expected, int32_t timeout = 5000)
    {
        return ignite_test::WaitForCondition(
                boost::bind(&IgniteClientTestSuiteFixture::CheckActiveConnections, this, expected),
                timeout);
    }

    /**
     * Check that if client started with given configuration and connection limit then the actual number of active
     * connections is equal to the expected value.
     *
     * @param cfg Client configuration.
     * @param limit Limit to set
     * @param expect Expected connections number.
     */
    void CheckConnectionsNum(IgniteClientConfiguration &cfg, uint32_t limit, size_t expect)
    {
        cfg.SetConnectionsLimit(limit);
        IgniteClient client = IgniteClient::Start(cfg);

        BOOST_CHECK(WaitForConnections(expect));
        BOOST_CHECK_EQUAL(GetActiveConnections(), expect);
    }

    /**
     * Check number of active connections.
     *
     * @param expect connections to expect.
     * @return @c true on success.
     */
    bool CheckActiveConnections(size_t expect)
    {
        return GetActiveConnections() == expect;
    }

    /**
     * Get Number of active connections.
     *
     * @return Number of active connections.
     */
    static size_t GetActiveConnections()
    {
        size_t connected = ignite_test::GetLineOccurrencesInFile("logs/ignite-log-0.txt", "Client connected");
        size_t disconnected = ignite_test::GetLineOccurrencesInFile("logs/ignite-log-0.txt", "Client disconnected");

        return connected - disconnected;
    }

    /**
     * Start node with logging.
     *
     * @param id Node id. Used to identify node and log.
     */
    ignite::Ignite StartNodeWithLog(const std::string& id)
    {
        std::string nodeName = "ServerNode" + id;
        return ignite_test::StartCrossPlatformServerNode("with-logging-0.xml", nodeName.c_str());
    }
};

BOOST_FIXTURE_TEST_SUITE(IgniteClientTestSuite, IgniteClientTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteClientConnection)
{
    ignite::Ignite serverNode = ignite_test::StartCrossPlatformServerNode("cache.xml", "ServerNode");

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient::Start(cfg);
}

BOOST_AUTO_TEST_CASE(IgniteClientConnectionFailover)
{
    ignite::Ignite serverNode = ignite_test::StartCrossPlatformServerNode("cache.xml", "ServerNode");

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11109..11111");

    IgniteClient::Start(cfg);
}

BOOST_AUTO_TEST_CASE(IgniteClientConnectionLimit)
{
    ignite::common::DeletePath("logs");

    ignite::Ignite serverNode0 = StartNodeWithLog("0");
    ignite::Ignite serverNode1 = StartNodeWithLog("1");
    ignite::Ignite serverNode2 = StartNodeWithLog("2");

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");

    CheckConnectionsNum(cfg, 0, 3);
    CheckConnectionsNum(cfg, 1, 1);
    CheckConnectionsNum(cfg, 2, 2);
    CheckConnectionsNum(cfg, 3, 3);
    CheckConnectionsNum(cfg, 4, 3);
    CheckConnectionsNum(cfg, 100500, 3);
}

BOOST_AUTO_TEST_SUITE_END()
