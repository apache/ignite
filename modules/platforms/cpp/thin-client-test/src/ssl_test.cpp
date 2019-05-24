/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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

class SslTestSuiteFixture
{
public:
    ignite::Ignite StartSslNode()
    {
        return ignite_test::StartCrossPlatformServerNode("ssl.xml", "ServerNode");
    }
    
    ignite::Ignite StartNonSslNode()
    {
        return ignite_test::StartCrossPlatformServerNode("non-ssl.xml", "ServerNode");
    }

    SslTestSuiteFixture()
    {
        // No-op.
    }

    ~SslTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

    std::string GetConfigFile(const std::string& file)
    {
        using namespace ignite::common;
        std::stringstream pathBuilder;

        pathBuilder << ignite_test::GetTestConfigDir() << Fs << "ssl" << Fs << file;

        return pathBuilder.str();
    }
};

BOOST_FIXTURE_TEST_SUITE(SslTestSuite, SslTestSuiteFixture)

BOOST_AUTO_TEST_CASE(SslConnectionSuccess)
{
    StartSslNode();

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    cfg.SetSslMode(SslMode::REQUIRE);
    cfg.SetSslCertFile(GetConfigFile("client_full.pem"));
    cfg.SetSslKeyFile(GetConfigFile("client_full.pem"));
    cfg.SetSslCaFile(GetConfigFile("ca.pem"));

    IgniteClient::Start(cfg);
}

BOOST_AUTO_TEST_CASE(SslConnectionReject)
{
    StartSslNode();

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    cfg.SetSslMode(SslMode::REQUIRE);
    cfg.SetSslCertFile(GetConfigFile("client_unknown.pem"));
    cfg.SetSslKeyFile(GetConfigFile("client_unknown.pem"));
    cfg.SetSslCaFile(GetConfigFile("ca.pem"));

    BOOST_CHECK_THROW(IgniteClient::Start(cfg), ignite::IgniteError);
}

BOOST_AUTO_TEST_CASE(SslConnectionReject2)
{
    StartSslNode();

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    cfg.SetSslMode(SslMode::DISABLE);

    BOOST_CHECK_THROW(IgniteClient::Start(cfg), ignite::IgniteError);
}

BOOST_AUTO_TEST_CASE(SslConnectionTimeout)
{
    StartNonSslNode();

    IgniteClientConfiguration cfg;
 
    cfg.SetEndPoints("127.0.0.1:11110");
    
    cfg.SetSslMode(SslMode::REQUIRE);
    cfg.SetSslCertFile(GetConfigFile("client_full.pem"));
    cfg.SetSslKeyFile(GetConfigFile("client_full.pem"));
    cfg.SetSslCaFile(GetConfigFile("ca.pem"));

    BOOST_CHECK_THROW(IgniteClient::Start(cfg), ignite::IgniteError);
}

BOOST_AUTO_TEST_SUITE_END()
