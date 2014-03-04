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
#include <cstdio>
#include <iostream>
#include <memory>

#include <boost/uuid/uuid_io.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>

#include <gridgain/gridgain.hpp>
#include "gridclientfactoryfixture.hpp"
#include "gridgain/impl/utils/gridthreadpool.hpp"
#include "gridgain/impl/utils/gridfutureimpl.hpp"

#include "gridtestcommon.hpp"

using namespace std;

/** Local cache name. */
static const string LOCAL_CACHE_NAME = "local";

/** Name of the cache with store. */
static const string LOCAL_CACHE_WITH_STORE_NAME = "local.store";

/** Expected topology size. */
static const int TOP_SIZE = 2;

/**
 * Configuration for TCP client.
 */
class TcpConfig {
public:
    /**
     * Invocation operator.
     *
     * @return Client configuration.
     */
    GridClientConfiguration operator()() const {
        GridClientConfiguration clientConfig;

        vector<GridSocketAddress> servers;

        servers.push_back(GridSocketAddress("127.0.0.1", TEST_TCP_PORT));

        clientConfig.servers(servers);

        GridClientProtocolConfiguration protoCfg;

        protoCfg.credentials(CREDS);

        protoCfg.protocol(TCP);

        clientConfig.protocolConfiguration(protoCfg);

        return clientConfig;
    }
};

/**
 * Configuration for TCP client over router.
 */
class TcpRouterConfig {
public:
    GridClientConfiguration operator()() const {
        GridClientConfiguration clientConfig;

        vector<GridSocketAddress> routers;

        routers.push_back(GridSocketAddress("127.0.0.1", 12100));

        clientConfig.routers(routers);

        GridClientProtocolConfiguration protoCfg;

        protoCfg.credentials(CREDS);

        protoCfg.protocol(TCP);

        clientConfig.protocolConfiguration(protoCfg);

        return clientConfig;
    }
};

/**
 * Configuration for HTTP client.
 */
class HttpConfig {
public:
    /**
     * Invocation operator.
     *
     * @return Client configuration.
     */
    GridClientConfiguration operator()() const {
        GridClientConfiguration clientConfig;

        vector<GridSocketAddress> servers;

        servers.push_back(GridSocketAddress("127.0.0.1", TEST_HTTP_PORT));

        clientConfig.servers(servers);

        GridClientProtocolConfiguration protoCfg;

        protoCfg.credentials(CREDS);

        protoCfg.protocol(HTTP);

        clientConfig.protocolConfiguration(protoCfg);

        return clientConfig;
    }
};

/**
 * Configuration for HTTP client over router.
 */
class HttpRouterConfig {
public:
    /**
     * Invocation operator.
     *
     * @return Client configuration.
     */
    GridClientConfiguration operator()() const {
        GridClientConfiguration clientConfig;

        vector<GridSocketAddress> routers;

        routers.push_back(GridSocketAddress("127.0.0.1", 12200));

        clientConfig.routers(routers);

        GridClientProtocolConfiguration protoCfg;

        protoCfg.credentials(CREDS);

        protoCfg.protocol(HTTP);

        clientConfig.protocolConfiguration(protoCfg);

        return clientConfig;
    }
};

#ifdef GRIDGAIN_ROUTER_TEST

/** Test configuration list. */
typedef boost::mpl::list<TcpRouterConfig, HttpRouterConfig> TestCfgs;

/** Test configuration list for TCP only transport. */
typedef boost::mpl::list<TcpRouterConfig> TestCfgsTcpOnly;

#else

/** Test configuration list. */
typedef boost::mpl::list<TcpConfig, HttpConfig> TestCfgs;

/** Test configuration list for TCP only transport. */
typedef boost::mpl::list<TcpConfig> TestCfgsTcpOnly;

#endif

BOOST_AUTO_TEST_SUITE(GridClientAbstractSelfTest)

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testPut, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientPtr client = this->client(CfgT());

    TGridClientDataPtr data = client->data(CACHE_NAME);

    client->compute()->refreshNode(GridUuid::randomUuid(), true, true);

    multithreaded([&] {
        GridClientVariant key(genRandomUniqueString("key"));
        GridClientVariant val("val");

        // Put and get into the cache.
        data->put(key, val);
        GridClientVariant v = data->get(key);

        SYNC_CHECK_EQUAL(v, val);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testPutAsync, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant varKey(genRandomUniqueString("key"));
        GridClientVariant varValue("val");

        TGridBoolFuturePtr keyFut = data->putAsync(varKey, varValue);

        keyFut->get();

        TGridClientFutureVariant dataFut = data->getAsync(varKey);

        SYNC_CHECK_EQUAL(dataFut->get(), varValue);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testPutAsyncException, CfgT, TestCfgs, GridClientFactoryFixture2) {
   TGridClientDataPtr data = client(CfgT())->data(CACHE_NAME);

   GridClientFactory::stopAll(); //stop the client to get GridClientClosedException from Future->get()

   GridClientVariant varKey(genRandomUniqueString("key"));
   GridClientVariant varValue("val");

   TGridBoolFuturePtr keyFut = data->putAsync(varKey, varValue); //no exception should happen here

   BOOST_CHECK_THROW( keyFut->get(), GridClientClosedException );

   TGridClientFutureVariant dataFut = data->getAsync(varKey); //no exception should happen here

   BOOST_CHECK_THROW( dataFut->get(), GridClientClosedException );
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testPutAll, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant val1("val1");
        GridClientVariant key2(genRandomUniqueString("key2"));
        GridClientVariant val2("val2");

        // Create the collection for elements in the cache.
        TGridClientVariantMap m;

        m[key1] = val1;
        m[key2] = val2;

        data->putAll(m);

        TGridClientVariantSet keys;

        keys.push_back(key1);
        keys.push_back(key2);

        m = data->getAll(keys);

        SYNC_CHECK_EQUAL(m.size(), 2);
        SYNC_CHECK_EQUAL(m[key1], val1);
        SYNC_CHECK_EQUAL(m[key2], val2);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testPutAllWithCornerCases, CfgT, TestCfgs, GridClientFactoryFixture2) {
   TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

   GridClientVariant key1(""); // Empty string key.
   GridClientVariant val1("val1");
   GridClientVariant key2("key2");
   GridClientVariant val2; // Null value.
   GridClientVariant key3; // Null key.
   GridClientVariant val3("val3");

   TGridClientVariantMap m;
   m[key1] = val1;

   data->putAll(m);

   TGridClientVariantSet keys;
   keys.push_back(key1);

   m = data->getAll(keys);

   BOOST_CHECK_EQUAL(m.size(), 1);
   BOOST_CHECK_EQUAL(m[key1], val1);

   m.clear();

   m[key2] = val2;

   BOOST_CHECK_THROW(data->putAll(m), GridClientException);

   m.clear();

   m[key3] = val3;

   BOOST_CHECK_THROW(data->putAll(m), GridClientException);
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testPutAllAsyncException, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = client(CfgT())->data(CACHE_NAME);

    GridClientFactory::stopAll(); //stop the client to get GridClientClosedException from Future->get()

    GridClientVariant key1(genRandomUniqueString("key1"));
    GridClientVariant val1("val1");
    GridClientVariant key2(genRandomUniqueString("key2"));
    GridClientVariant val2("val2");

    // Create the collection for elements in the cache.
    TGridClientVariantMap m;

    m[key1] = val1;
    m[key2] = val2;

    TGridBoolFuturePtr putFut = data->putAllAsync(m);

    BOOST_CHECK_THROW( putFut->get(), GridClientClosedException );

    TGridClientVariantSet keys;

    keys.push_back(key1);
    keys.push_back(key2);

    TGridClientFutureVariantMap getFut = data->getAllAsync(keys);

    BOOST_CHECK_THROW( getFut->get(), GridClientClosedException );
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testGet, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant val1("val1");

        SYNC_CHECK(data->put(key1, val1));

        GridClientVariant v = data->get(key1);

        SYNC_CHECK_EQUAL(v, val1);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testGetAsync, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant val1("val1");

        TGridBoolFuturePtr fut1 = data->putAsync(key1, val1);

        fut1->get();

        SYNC_CHECK(fut1->get());

        TGridClientFutureVariant fut = data->getAsync(key1);

        SYNC_CHECK_EQUAL(fut->get(), val1);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testGetAll, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant val1("val1");

        SYNC_CHECK(data->put(key1, val1));

        GridClientVariant key2(genRandomUniqueString("key2"));
        GridClientVariant val2("val2");

        SYNC_CHECK(data->put(key2, val2));

        std::vector<GridClientVariant> keys;

        keys.push_back(key1);
        keys.push_back(key2);

        TGridClientVariantMap m = data->getAll(keys);

        SYNC_CHECK_EQUAL(m.size(), 2);
        SYNC_CHECK_EQUAL(m[key1], val1);
        SYNC_CHECK_EQUAL(m[key2], val2);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testGetAllAsync, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant val1("val1");

        TGridBoolFuturePtr fut1 = data->putAsync(key1, val1);

        SYNC_CHECK(fut1->get());

        GridClientVariant key2(genRandomUniqueString("key2"));
        GridClientVariant val2("val2");

        TGridBoolFuturePtr fut2 = data->putAsync(key2, val2);

        SYNC_CHECK(fut2->get());

        std::vector<GridClientVariant> keys;

        keys.push_back(key1);
        keys.push_back(key2);

        TGridClientFutureVariantMap fut = data->getAllAsync(keys);

        fut->get();

        SYNC_CHECK(fut->success());

        TGridClientVariantMap cacheVals = fut->result();

        SYNC_CHECK_EQUAL(cacheVals.size(), 2);
        SYNC_CHECK_EQUAL(cacheVals[key1], val1);
        SYNC_CHECK_EQUAL(cacheVals[key2], val2);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testRemove, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant key2(genRandomUniqueString("key2"));

        SYNC_CHECK(data->put(key1, "val1"));
        SYNC_CHECK(data->put(key2, "val2"));

        SYNC_CHECK(data->remove(key1));
        SYNC_CHECK(!data->remove("wrongKey"));

        GridClientVariant val = data->get(key1);

        SYNC_CHECK(!val.hasAnyValue());

        val = data->get(key2);

        SYNC_CHECK_EQUAL(val, "val2");
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testRemoveAsync, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant key2(genRandomUniqueString("key2"));

        TGridBoolFuturePtr fut1 = data->putAsync(key1, "val1");
        TGridBoolFuturePtr fut2 = data->putAsync(key2, "val2");

        fut1->get();
        fut2->get();

        fut1 = data->removeAsync(key1);

        fut1->get();

        GridClientVariant val = data->get(key1);

        SYNC_CHECK(!val.hasAnyValue());

        val = data->get(key2);

        SYNC_CHECK_EQUAL(val, "val2");
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testRemoveAsyncException, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    GridClientFactory::stopAll(); //stop the client to get GridClientClosedException from Future->get()

    GridClientVariant key1(genRandomUniqueString("key1"));

    TGridBoolFuturePtr fut = data->removeAsync(key1);

    BOOST_CHECK_THROW( fut->get(), GridClientClosedException );

    vector<GridClientVariant> keys;

    keys.push_back(key1);

    fut = data->removeAllAsync(keys);

    BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testRemoveAll, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant key2(genRandomUniqueString("key2"));

        SYNC_CHECK(data->put(key1, "val1"));
        SYNC_CHECK(data->put(key2, "val2"));

        vector<GridClientVariant> keys;

        keys.push_back(key1);
        keys.push_back(key2);

        data->removeAll(keys);

        SYNC_CHECK(!data->get(key1).hasAnyValue());
        SYNC_CHECK(!data->get(key2).hasAnyValue());
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testRemoveAllAsync, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant key2(genRandomUniqueString("key2"));

        TGridBoolFuturePtr fut1 = data->putAsync(key1, "val1");
        TGridBoolFuturePtr fut2 = data->putAsync(key2, "val2");

        SYNC_CHECK(fut1->get());
        SYNC_CHECK(fut2->get());

        vector<GridClientVariant> keys;

        keys.push_back(key1);
        keys.push_back(key2);

        TGridBoolFuturePtr fut = data->removeAllAsync(keys);

        SYNC_CHECK(fut->get());

        TGridClientVariantMap vals = data->getAll(keys);

        SYNC_CHECK_EQUAL(vals.size(), 0);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testReplace, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));

        SYNC_CHECK(!data->replace(key1, "val1"));

        SYNC_CHECK(data->put(key1, "val1"));

        SYNC_CHECK(data->replace(key1, "val2"));

        SYNC_CHECK(data->get(key1) == "val2");
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testReplaceAsyncException, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    GridClientFactory::stopAll(); //stop the client to get GridClientClosedException from Future->get()

    GridClientVariant key1(genRandomUniqueString("key1"));

    TGridBoolFuturePtr fut = data->replaceAsync(key1, "val1");

    BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testCompareAndSet, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));
        GridClientVariant val1("val1");
        GridClientVariant wrongVal("wrongVal");
        GridClientVariant newVal("newVal");
        GridClientVariant nullValue;

        SYNC_CHECK(data->put(key1, val1));
        SYNC_CHECK(data->cas(key1, nullValue, nullValue));

        GridClientVariant val = data->get(key1);

        SYNC_CHECK(!val.hasAnyValue());

        SYNC_CHECK(!data->cas(key1, nullValue, val1));

        SYNC_CHECK(data->put(key1, val1));
        SYNC_CHECK(!data->cas(key1, nullValue, wrongVal));

        val = data->get(key1);

        SYNC_CHECK(val == val1);

        SYNC_CHECK(data->cas(key1, nullValue, val1));

        val = data->get(key1);

        SYNC_CHECK(!val.hasAnyValue());

        SYNC_CHECK(data->cas(key1, val1, nullValue));

        val = data->get(key1);

        SYNC_CHECK(val == val1);

        SYNC_CHECK(!data->cas(key1, newVal, nullValue));

        val = data->get(key1);

        SYNC_CHECK(val == val1);

        SYNC_CHECK(!data->cas(key1, val1, wrongVal));

        val = data->get(key1);

        SYNC_CHECK(val == val1);

        SYNC_CHECK(data->cas(key1, newVal, val1));

        val = data->get(key1);

        SYNC_CHECK(val == newVal);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testCompareAndSetAsyncException, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    GridClientFactory::stopAll(); //stop the client to get GridClientClosedException from Future->get()

    GridClientVariant key1(genRandomUniqueString("key1"));
    GridClientVariant val1("val1");
    GridClientVariant newVal("newVal");

    TGridBoolFuturePtr fut = data->casAsync(key1, val1, newVal);

    BOOST_CHECK_THROW(fut->get(), GridClientClosedException);
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testMetrics, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        GridClientVariant key1(genRandomUniqueString("key1"));

        SYNC_CHECK(data->put(key1, "val1"));

        GridClientVariant val = data->get(key1);

        SYNC_CHECK(val == "val1");

        GridClientDataMetrics metrics = data->metrics();

        SYNC_CHECK(metrics.reads() > 0);
        SYNC_CHECK(metrics.writes() > 0);
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testMetricsAsyncException, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    GridClientFactory::stopAll(); //stop the client to get GridClientClosedException from Future->get()

    TGridClientFutureDataMetrics fut = data->metricsAsync();

    BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
}

static std::string getRandomUuid() {
    boost::uuids::uuid u; // initialize uuid

    std::string s = boost::lexical_cast<std::string>(u);

    return s;
}

static void doCheckNodeDetails(TGridClientNodeList nodes, bool includeAttr, bool includeMetric) {
    BOOST_CHECK(nodes.size() > 0);

    for (size_t in = 0; in < nodes.size(); ++in) {
        TGridClientNodePtr node = nodes[in];

        BOOST_CHECK(node->getTcpAddresses().size() > 0);

        TGridClientVariantMap caches = node->getCaches();

        BOOST_CHECK_EQUAL(caches.size(), CACHE_CNT);

        for (auto it = caches.begin(); it != caches.end(); ++it) {
            GridClientVariant varName = it->first;
            GridClientVariant varVal = it->second;

            BOOST_CHECK(varName.hasString() || !varName.hasAnyValue());

            if (!varName.hasAnyValue() || CACHE_NAME == varName.getString() ||
                  LOCAL_CACHE_NAME == varName.getString()) {
               BOOST_CHECK(varVal.hasLong() ?
                    (GridClientCache::LOCAL == varVal.getLong()) :
                     varVal.toString() == "LOCAL" || varVal.toString() == "PARTITIONED");
            }
            else if ("replicated" == varName.getString())
               BOOST_CHECK(varVal.hasLong() ?
                    GridClientCache::REPLICATED == varVal.getLong() :
                    varVal.toString() == "REPLICATED");
            else if ("partitioned" == varName.getString())
               BOOST_CHECK(varVal.hasLong() ?
                    GridClientCache::PARTITIONED == varVal.getLong() :
                    varVal.toString() == "PARTITIONED");
        }
    }
}

static void doRefreshNode(TGridClientComputePtr compute, GridUuid id, bool includeAttr, bool includeMetric) {
    TGridClientNodePtr node = compute->refreshNode(id, includeAttr, includeMetric);

    BOOST_CHECK(node->getTcpAddresses().size() > 0);

    TGridClientVariantMap caches = node->getCaches();

    BOOST_CHECK_EQUAL(caches.size(), CACHE_CNT);

    for (auto it = caches.begin(); it != caches.end(); ++it) {
        GridClientVariant varName = it->first;
        GridClientVariant varVal = it->second;

        BOOST_CHECK(varName.hasString() || !varName.hasAnyValue());

        if (!varName.hasAnyValue() || (CACHE_NAME == varName.getString()) ||
              (LOCAL_CACHE_NAME == varName.getString())) {
           BOOST_CHECK(varVal.hasLong() ?
                (GridClientCache::LOCAL == varVal.getLong()) :
                 varVal.toString() == "LOCAL" || varVal.toString() == "PARTITIONED");
        }
        else if ("replicated" == varName.getString())
           BOOST_CHECK(varVal.hasLong() ?
                GridClientCache::REPLICATED == varVal.getLong() :
                varVal.toString() == "REPLICATED");
        else if ("partitioned" == varName.getString())
           BOOST_CHECK(varVal.hasLong() ?
                GridClientCache::PARTITIONED == varVal.getLong() :
                varVal.toString() == "PARTITIONED");
    }
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testNodeRefresh, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientPtr client = this->client(CfgT());

    TGridClientDataPtr data = client->data(CACHE_NAME);

    TGridClientComputePtr compute = client->compute();

    std::string uuid = getRandomUuid();

    TGridClientNodePtr node = compute->refreshNode(uuid, true, false);

    TGridClientNodeList nodes = compute->nodes();

    BOOST_CHECK(nodes.size() > 0);

    node = nodes[0];

    doRefreshNode(compute, node->getNodeId(), false, false);

    doRefreshNode(compute, node->getNodeId(), false, true);

    doRefreshNode(compute, node->getNodeId(), true, false);

    doRefreshNode(compute, node->getNodeId(), true, true);
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testTopology, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientPtr client = this->client(CfgT());

    TGridClientDataPtr data = client->data(CACHE_NAME);

    TGridClientComputePtr compute = client->compute();

    TGridClientNodeList nodes = compute->refreshTopology(false, false);
    doCheckNodeDetails(nodes, false, false);

    nodes = compute->refreshTopology(false, true);
    doCheckNodeDetails(nodes, false, true);

    nodes = compute->refreshTopology(true, false);
    doCheckNodeDetails(nodes, true, false);

    nodes = compute->refreshTopology(true, true);
    doCheckNodeDetails(nodes, true, true);
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testCompute, CfgT, TestCfgs, GridClientFactoryFixture2) {
    static const string taskName = "org.gridgain.client.GridClientStringLengthTask"; // Takes a single string argument.

    CfgT cfg = CfgT();
    GridClientProtocol proto = cfg().protocolConfiguration().protocol();

    TGridClientPtr client = this->client(cfg);
    TGridClientDataPtr data = client->data(CACHE_NAME);

    GridClientVariant affKey(genRandomUniqueString("affKey1"));
    GridClientVariant affKeyVal("affKeyVal1");

    // Put the affinity key.
    BOOST_REQUIRE(data->put(affKey, affKeyVal));

    TGridClientComputePtr compute = client->compute();

    GridClientVariant val1("val1");

    {
        GridClientVariant result = compute->execute(taskName, val1);

        if (proto == TCP)
            BOOST_CHECK_EQUAL(result.getInt(), val1.getString().length());
        else
            // HTTP operates only with string results.
            BOOST_CHECK_EQUAL(result.getString(), boost::lexical_cast<string>(val1.getString().length()));
    }

    {
        TGridClientFutureVariant fut = compute->executeAsync(taskName, val1);

        if (proto == TCP)
            BOOST_CHECK_EQUAL(fut->get().getInt(), val1.getString().length());
        else
            // HTTP operates only with string results.
            BOOST_CHECK_EQUAL(fut->get().getString(), boost::lexical_cast<string>(val1.getString().length()));
    }

    {
        GridClientVariant result = compute->affinityExecute(taskName, CACHE_NAME, affKey, val1);

        if (proto == TCP)
            BOOST_CHECK_EQUAL(result.getInt(), val1.getString().length());
        else
            // HTTP operates only with string results.
            BOOST_CHECK_EQUAL(result.getString(), boost::lexical_cast<string>(val1.getString().length()));
    }

    {
        TGridClientFutureVariant fut = compute->affinityExecuteAsync(taskName, CACHE_NAME, affKey, val1);

        if (proto == TCP)
            BOOST_CHECK_EQUAL(fut->get().getInt(), val1.getString().length());
        else
            // HTTP operates only with string results.
            BOOST_CHECK_EQUAL(fut->get().getString(), boost::lexical_cast<string>(val1.getString().length()));
    }

    {
        TGridClientFutureVariant fut = compute->executeAsync(taskName, GridClientVariant(123));

        if (proto == TCP) {
            // Should throw error, because only string arguments are supported by the task.
            BOOST_CHECK_THROW(fut->get(), GridClientException);
        }
        else
            // In HTTP, however, all types are converted to strings, so, the task
            // should return string length (also represented as string).
            BOOST_CHECK_EQUAL(GridClientVariant("3"), fut->get());
    }
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testAffinity, CfgT, TestCfgs, GridClientFactoryFixture2) {
    static const string taskName = "org.gridgain.client.GridClientGetAffinityTask";

    TGridClientPtr client = this->client(CfgT());
    TGridClientDataPtr data = client->data(CACHE_NAME);
    TGridClientComputePtr compute = client->compute();

    // The test only makes sense for 2 or more nodes.
    BOOST_REQUIRE(compute->nodes().size() > 1);

    for (int i = 0; i < 100; i++) {
        ostringstream os; os << "key" << i;

        string key = os.str();

        // Get server affinity node ID.
        GridClientVariant srvAffNodeId = compute->execute(taskName, GridClientVariant(CACHE_NAME + ":" + key));

        BOOST_REQUIRE(srvAffNodeId.hasString());

        // Get client affinity node ID.
        GridUuid cliAffNodeId = data->affinity(GridClientVariant(key));

        // Ensure they match.
        BOOST_CHECK_EQUAL(srvAffNodeId.getString(), cliAffNodeId.uuid());
    }
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testComputeAsyncException, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientComputePtr compute = this->client(CfgT())->compute();

    GridClientFactory::stopAll(); // Stop the client to get GridClientClosedException.

    {
        TGridClientFutureVariant fut = compute->executeAsync("myTask"); //shouldn't throw anything

        BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
    }

    {
        TGridClientFutureVariant fut = compute->affinityExecuteAsync("myTask", "myCache", genRandomUniqueString("key1"));

        BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
    }

    {
        TGridFutureStringList fut = compute->logAsync(1, 2);

        BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
    }

    {
        TGridFutureStringList fut = compute->logAsync("somePath", 1, 2);

        BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
    }

    {
        TGridClientNodeFuturePtr fut = compute->refreshNodeAsync(getRandomUuid(), true, true);

        BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
    }

    {
        TGridClientNodeFuturePtr fut = compute->refreshNodeAsync(std::string(""), true, true);

        BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
    }

    {
        TGridClientNodeFutureList fut = compute->refreshTopologyAsync(true, true);

        BOOST_CHECK_THROW( fut->get(), GridClientClosedException );
    }
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testGracefulShutdown, CfgT, TestCfgs, GridClientFactoryFixture2) {
    static const string taskName = "org.gridgain.client.GridSleepTestTask";

    CfgT cfg = CfgT();
    GridClientProtocol proto = cfg().protocolConfiguration().protocol();

    TGridClientPtr client = this->client(cfg);
    TGridClientComputePtr compute = client->compute();

    GridClientVariant val1("val1");

    TGridClientFutureVariant fut = compute->executeAsync(taskName, val1);

    boost::this_thread::sleep(boost::posix_time::seconds(1)); // To ensure the task is taken from the queue.

    GridClientFactory::stop(client->id(), true);

    // Wait for the task to complete (this shouldn't throw).
    fut->get();

    if (proto == TCP)
        BOOST_CHECK_EQUAL(fut->result().getInt(), val1.getString().length());
    else
        // HTTP operates only with string results.
        BOOST_CHECK_EQUAL(fut->result().getString(), boost::lexical_cast<string>(val1.getString().length()));
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testForceShutdown, CfgT, TestCfgs, GridClientFactoryFixture2) {
    // This task pauses for a while before returning the result.
    static const string taskName = "org.gridgain.client.GridSleepTestTask";

    TGridClientPtr client = this->client(CfgT());
    TGridClientComputePtr compute = client->compute();

    GridClientVariant val1("val1");

    TGridClientFutureVariant fut = compute->executeAsync(taskName, val1);

	boost::this_thread::sleep(boost::posix_time::seconds(1)); // To ensure we call stop during the pause.

    GridClientFactory::stop(client->id(), false);

    // Wait for the task to complete, this should throw an exception.
    BOOST_CHECK_THROW(fut->get(), GridClientClosedException);
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testFutureTimeoutExpired, CfgT, TestCfgs, GridClientFactoryFixture2) {
    // This task pauses for a while before returning the result.
    static const string taskName = "org.gridgain.client.GridSleepTestTask";

    TGridClientPtr client = this->client(CfgT());
    TGridClientComputePtr compute = client->compute();

    GridClientVariant val1("val1");

    TGridClientFutureVariant fut = compute->executeAsync(taskName, val1);

    // Wait for only 1 second, this should throw an exception.
    BOOST_CHECK_THROW(fut->get(boost::posix_time::seconds(1)), GridClientFutureTimeoutException);
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testFutureTimeoutNotExpired, CfgT, TestCfgs, GridClientFactoryFixture2) {
    // This task pauses for a while before returning the result.
    static const string taskName = "org.gridgain.client.GridSleepTestTask";

    CfgT cfg = CfgT();
    GridClientProtocol proto = cfg().protocolConfiguration().protocol();

    TGridClientPtr client = this->client(cfg);
    TGridClientComputePtr compute = client->compute();

    GridClientVariant val1("val1");

    TGridClientFutureVariant fut = compute->executeAsync(taskName, val1);

    // Wait for only 10 seconds (more than task lasts), this should return once task is completed.
    BOOST_CHECK_NO_THROW(fut->get(boost::posix_time::seconds(50)));

    if (proto == TCP)
        BOOST_CHECK_EQUAL(fut->result().getInt(), val1.getString().length());
    else
        // HTTP operates only with string results.
        BOOST_CHECK_EQUAL(fut->result().getString(), boost::lexical_cast<string>(val1.getString().length()));
}

bool doSleep(const boost::posix_time::time_duration& time) {
    boost::this_thread::sleep(time);

    return true;
}

BOOST_AUTO_TEST_CASE(testBoolFutureTimeoutExpired) {
    std::shared_ptr<GridThreadPool> threadPool(new GridThreadPool(1));

    GridBoolFutureImpl* futImpl = new GridBoolFutureImpl(threadPool);

    TGridBoolFuturePtr fut(futImpl);

    boost::packaged_task<bool> pt(boost::bind(&doSleep, boost::posix_time::seconds(5)));

    futImpl->task(pt);

    //wait for only 1 second, this should throw an exception
    BOOST_CHECK_THROW( fut->get(boost::posix_time::seconds(1)), GridClientFutureTimeoutException );
}

BOOST_AUTO_TEST_CASE(testBoolFutureTimeoutNotExpired) {
    std::shared_ptr<GridThreadPool> threadPool(new GridThreadPool(1));

    GridBoolFutureImpl* futImpl = new GridBoolFutureImpl(threadPool);

    TGridBoolFuturePtr fut(futImpl);

    boost::packaged_task<bool> pt(boost::bind(&doSleep, boost::posix_time::seconds(1)));

    futImpl->task(pt);

    // wait for 5 seconds (less, than task lasts), this should return a result once
    // the function is run
    BOOST_CHECK_EQUAL( fut->get(boost::posix_time::seconds(5)), true );
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testFutureGetOnStoppedClient, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    TGridBoolFuturePtr lastFuture; // This will be the last future in task queue.

    for(unsigned int i = 0; i < 1000; i++) {
        GridClientVariant varKey(genRandomUniqueString("key"));
        GridClientVariant varValue("val");

        lastFuture = data->putAsync(varKey, varValue);
    }

    // Stop client from separate thread.
    boost::thread stopThread([] {
        GridClientFactory::stopAll(false);
    });

    // While the stopThread stops the client, this thread already
    // hangs in lastFuture->get().
    // The stopThread should cancel the corresponding task and
    // kick this thread out with exception.
    BOOST_CHECK_THROW(lastFuture->get(), std::exception);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(testTopologyListener, CfgT, TestCfgs) {
    GridClientConfiguration cfg = CfgT()();

    GridTestTopologyListener* topLsnr = new GridTestTopologyListener();

    TGridClientTopologyListenerPtr topLsnrPtr(topLsnr);

    TGridClientTopologyListenerList topLsnrList;

    topLsnrList.push_back(topLsnrPtr);

    cfg.topologyListeners(topLsnrList);

    TGridClientPtr client = GridClientFactory::start(cfg);

    BOOST_CHECK_EQUAL(topLsnr->getNodesCount(), TOP_SIZE);

    GridClientFactory::stopAll(false);
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testPutBigEntries, CfgT, TestCfgsTcpOnly, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    for (int32_t i = 0; i < 100; i++) {
        ostringstream oss;

        for (int32_t j = 0; j < 1024 * 1024 * 2; j++)
            oss << i;

        string v = oss.str();

        string key = boost::lexical_cast<string>(i);

        cout << "Putting key #" << i << "\n";

        data->put(key, v);

        cout << "Getting key #" << i << "\n";

        GridClientVariant val = data->get(key);

        BOOST_CHECK_EQUAL(v, val.getString());

        cout << "Removing key #" << i << "\n";

        data->remove(key);
    }
}

// TODO: enable for HTTP after fixing GG-3273
BOOST_FIXTURE_TEST_CASE_TEMPLATE(testPutBigEntriesMultithreaded, CfgT, TestCfgsTcpOnly, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(CACHE_NAME);

    multithreaded([&] {
        string keyPref = genRandomUniqueString();

        for (int32_t i = 0; i < 10; i++) {
            ostringstream oss;

            for (int32_t j = 0; j < 1024 * 1024 * 2; j++)
                oss << i;

            string v = oss.str();

            string key = keyPref + "-" + boost::lexical_cast<string>(i);

            data->put(key, v);

            GridClientVariant val = data->get(key);

            SYNC_CHECK_EQUAL(v, val.getString());

            data->remove(key);
        }
    });
}

BOOST_FIXTURE_TEST_CASE_TEMPLATE(testCacheFlags, CfgT, TestCfgs, GridClientFactoryFixture2) {
    TGridClientDataPtr data = this->client(CfgT())->data(LOCAL_CACHE_WITH_STORE_NAME);

    std::set<GridClientCacheFlag> skipStoreSet;

    skipStoreSet.insert(GridClientCacheFlag::SKIP_STORE);

    TGridClientDataPtr readData = data->flagsOn(skipStoreSet);
    TGridClientDataPtr writeData = readData->flagsOff(skipStoreSet);

    BOOST_CHECK(skipStoreSet == readData->flags());
    BOOST_CHECK(writeData->flags().empty());

    for (int i = 0; i < 100; i++) {
        GridClientVariant key = genRandomUniqueString();
        GridClientVariant val = genRandomUniqueString();

        // Put entry into cache & store.
        BOOST_CHECK(writeData->put(key, val));

        BOOST_CHECK_EQUAL(val, readData->get(key));
        BOOST_CHECK_EQUAL(val, writeData->get(key));

        // Remove from cache, skip store.
        BOOST_CHECK(readData->remove(key));

        BOOST_CHECK(!readData->get(key).hasAnyValue());
        BOOST_CHECK_EQUAL(val, writeData->get(key));
        BOOST_CHECK_EQUAL(val, readData->get(key));

        // Remove from cache and from store.
        BOOST_CHECK(writeData->remove(key));

        BOOST_CHECK(!readData->get(key).hasAnyValue());
        BOOST_CHECK(!writeData->get(key).hasAnyValue());
    }
}

BOOST_AUTO_TEST_SUITE_END()
