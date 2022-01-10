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

#include <ignite/common/utils.h>

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <test_utils.h>

using namespace ignite::thin;
using namespace boost::unit_test;

class SslTestSuiteFixture
{
public:
    ignite::Ignite StartSslNode(const std::string& name = "ServerNode")
    {
        return ignite_test::StartCrossPlatformServerNode("ssl.xml", name.c_str());
    }
    
    ignite::Ignite StartNonSslNode(const std::string& name = "ServerNode")
    {
        return ignite_test::StartCrossPlatformServerNode("non-ssl.xml", name.c_str());
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

BOOST_AUTO_TEST_CASE(SslCacheClientPutAllGetAll)
{
    StartSslNode("node1");
    StartSslNode("node2");

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11110");

    cfg.SetSslMode(SslMode::REQUIRE);
    cfg.SetSslCertFile(GetConfigFile("client_full.pem"));
    cfg.SetSslKeyFile(GetConfigFile("client_full.pem"));
    cfg.SetSslCaFile(GetConfigFile("ca.pem"));

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
            client.CreateCache<int32_t, std::string>("test");

    enum { BATCH_SIZE = 20000 };

    std::map<int32_t, std::string> values;
    std::set<int32_t> keys;

    for (int32_t j = 0; j < BATCH_SIZE; ++j)
    {
        int32_t key = BATCH_SIZE + j;

        values[key] = "value_" + ignite::common::LexicalCast<std::string>(key);
        keys.insert(key);
    }

    cache.PutAll(values);

    std::map<int32_t, std::string> retrieved;
    cache.GetAll(keys, retrieved);

    BOOST_REQUIRE(values == retrieved);
}

BOOST_AUTO_TEST_CASE(SslCacheClientPutGet)
{
    StartSslNode("node1");
    StartSslNode("node2");

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11110");

    cfg.SetSslMode(SslMode::REQUIRE);
    cfg.SetSslCertFile(GetConfigFile("client_full.pem"));
    cfg.SetSslKeyFile(GetConfigFile("client_full.pem"));
    cfg.SetSslCaFile(GetConfigFile("ca.pem"));

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
            client.CreateCache<int32_t, std::string>("test");

    enum { OPS_NUM = 100 };
    for (int32_t j = 0; j < OPS_NUM; ++j)
    {
        int32_t key = OPS_NUM + j;
        std::string value = "value_" + ignite::common::LexicalCast<std::string>(key);

        cache.Put(key, value);
        std::string retrieved = cache.Get(key);

        BOOST_REQUIRE_EQUAL(value, retrieved);
    }
}

BOOST_AUTO_TEST_SUITE_END()
