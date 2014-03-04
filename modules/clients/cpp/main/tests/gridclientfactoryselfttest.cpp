/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#ifndef _MSC_VER
#define BOOST_TEST_DYN_LINK
#endif

#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <map>
#include <vector>
#include <string>
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>

#include <gridgain/gridgain.hpp>
#include "gridclientfactoryfixture.hpp"

using namespace std;

BOOST_AUTO_TEST_SUITE(GridClientFactorySelfTest)

static string SERVER_ADDRESS = "127.0.0.1";
static int TCP_PORT = 10080;
static string CREDS = "s3cret";

GridClientConfiguration clientConfig() {
    GridClientConfiguration clientConfig;

    vector<GridSocketAddress> servers;

    servers.push_back(GridSocketAddress(SERVER_ADDRESS, TCP_PORT));

    clientConfig.servers(servers);

    GridClientProtocolConfiguration protoCfg;

    protoCfg.protocol(TCP);

    protoCfg.credentials(CREDS);

    clientConfig.protocolConfiguration(protoCfg);

    return clientConfig;
}

BOOST_FIXTURE_TEST_CASE(testDataProjectionCommands, GridClientFactoryFixture1<clientConfig>) {
    GridUuid clientId = client->id();
    TGridClientDataPtr dataPrj = client->data("partitioned");

    // Cache command self tests.

    // Prepare the test data.
    GridClientVariant key("KEY");
    GridClientVariant val("VAL");

    // Put and get into the cache.
    dataPrj->put(key, val);
    GridClientVariant val1 = dataPrj->get(key);

    BOOST_CHECK(val1 == val);

    // Remove the key from the cache and check that key has been deleted.

    dataPrj->remove(key);
    val1 = dataPrj->get(key);

    BOOST_CHECK(!val1.hasAnyValue());

    // Create the collection for elements in the cache.
    TGridClientVariantMap keyValColl;
    std::vector<GridClientVariant> keysColl;

    for (int64_t idx = 0; idx < 10; ++idx) {
        std::string val("VALUE#");

        val += boost::lexical_cast<std::string>(idx);

        keyValColl[idx] = val;

        keysColl.push_back(idx);
    }

    dataPrj->putAll(keyValColl);

    TGridClientVariantMap keyValsColl1 = dataPrj->getAll(keysColl);

    for(size_t i = 0; i < keysColl.size(); ++i)
        BOOST_CHECK(keyValColl[keysColl[i]] == keyValsColl1[keysColl[i]]);
}

BOOST_AUTO_TEST_SUITE_END()
