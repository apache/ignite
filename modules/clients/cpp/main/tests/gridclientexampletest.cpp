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

#include <map>
#include <vector>
#include <string>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <sstream>

#include <boost/unordered_map.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include <gridgain/gridgain.hpp>
#include "gridclientfactoryfixture.hpp"

BOOST_AUTO_TEST_SUITE(GridExamples)

using namespace std;

static string SERVER_ADDRESS = "127.0.0.1";
static string CACHE_NAME = "partitioned";
static int KEYS_CNT = 10;
static int TEST_TCP_PORT = 10080;
static string CREDS = "s3cret";

GridClientConfiguration clientConfig1() {
    GridClientConfiguration clientConfig;

    vector<GridClientSocketAddress> servers;

    servers.push_back(GridClientSocketAddress(SERVER_ADDRESS, TEST_TCP_PORT));

    clientConfig.servers(servers);

    GridClientProtocolConfiguration protoCfg;

    protoCfg.credentials(CREDS);

    clientConfig.protocolConfiguration(protoCfg);

    return clientConfig;
}

GridClientConfiguration clientConfig2() {
    GridClientConfiguration clientConfig;

    vector<GridClientSocketAddress> servers;

    GridClientProtocolConfiguration protoCfg;

    protoCfg.credentials(CREDS);

    clientConfig.protocolConfiguration(protoCfg);

    servers.push_back(GridClientSocketAddress(SERVER_ADDRESS, TEST_TCP_PORT));

    clientConfig.servers(servers);

    return clientConfig;
}

BOOST_FIXTURE_TEST_CASE(clientCacheExample, GridClientFactoryFixture1<clientConfig1>) {
    TGridClientComputePtr cc = client->compute();

    TGridClientNodeList nodes = cc->nodes();

    BOOST_REQUIRE(nodes.size() > 0);

    // Random node ID.
    GridClientUuid randNodeId = nodes[0]->getNodeId();

    // Get client projection of grid partitioned cache.
    TGridClientDataPtr rmtCache = client->data(CACHE_NAME);

    TGridClientVariantSet keys;

    // Put some values to the cache.
    for (int32_t i = 0; i < KEYS_CNT; i++) {
        ostringstream oss;

        oss << "val-" << i;

        string v = oss.str();

        rmtCache->put(i, v);

        GridClientUuid nodeId = rmtCache->affinity(i);

        keys.push_back(i);
    }

    TGridClientNodeList nodelst;
    TGridClientNodePtr p = client->compute()->node(randNodeId);

    nodelst.push_back(p);

    // Pin a remote node for communication. All further communication
    // on returned projection will happen through this pinned node.
    TGridClientDataPtr prj = rmtCache->pinNodes(nodelst);

    GridClientVariant val = prj->get((int32_t)0);

    TGridClientVariantMap vals = prj->getAll(keys);

    TGridBoolFuturePtr futPut = prj->putAsync((int32_t) 0, "new value for 0");

    boost::unordered_map<GridClientUuid, TGridClientVariantMap> keyVals;

    GridClientVariant key0 = GridClientVariant((int32_t)0);

    for (int32_t i = 0; i < KEYS_CNT; i++) {
        GridClientUuid nodeId = rmtCache->affinity((int32_t)0);

        TGridClientVariantMap* m;

        if (keyVals.find(nodeId) == keyVals.end())
            keyVals[nodeId] = TGridClientVariantMap();

        m = &(keyVals[nodeId]);

        ostringstream oss;

        oss << "updated-val-" << i;

        string v = oss.str();

        (*m)[i] = v;
    }

    for (auto iter = keyVals.begin();
            iter != keyVals.end(); ++iter)
       rmtCache->putAll(iter->second);

    vector<TGridBoolFuturePtr> futs;

    for (auto iter = keyVals.begin();
            iter != keyVals.end(); ++iter) {
       TGridBoolFuturePtr fut = rmtCache->putAllAsync(iter->second);

       futs.push_back(fut);
    }

    // Asynchronous gets, too.
    TGridClientFutureVariant futVal = rmtCache->getAsync(key0);

    futVal->get();

    // Multiple values can be fetched at once. Here we batch our get
    // requests by affinity nodes to ensure least amount of network trips.
    for (auto iter = keyVals.begin();
            iter != keyVals.end(); ++iter) {
       GridClientUuid uuid = iter->first;

       TGridClientVariantMap m = iter->second;

       TGridClientVariantSet keys;

       for (auto miter = m.begin(); miter != m.end(); ++miter )
           keys.push_back(miter->first);

       // Since all keys in our getAll(...) call are mapped to the same primary node,
       // grid cache client will pick this node for the request, so we only have one
       // network trip here.

       TGridClientVariantMap map = rmtCache->getAll(keys);
    }

    // Multiple values may be retrieved asynchronously, too.
    // Here we retrieve all keys at once. Since this request
    // will be sent to some grid node, this node may not be
    // the primary node for all keys and additional network
    // trips will have to be made within grid.

    TGridClientFutureVariantMap futVals = rmtCache->getAllAsync(keys);

    futVals->get();

    // Contents of cache may be removed one by one synchronously.
    // Again, this operation is affinity aware and only the primary
    // node for the key is contacted.
    rmtCache->remove(key0);

    // ... and asynchronously.
    TGridBoolFuturePtr futRes = rmtCache->removeAsync((int32_t)1);

    // Multiple entries may be removed at once synchronously...
    TGridClientVariantSet keysRemove;

    keysRemove.push_back((int32_t)2);
    keysRemove.push_back((int32_t)3);

    rmtCache->removeAll(keysRemove);

    // ... and asynchronously.
    keysRemove.clear();

    keysRemove.push_back((int32_t)4);
    keysRemove.push_back((int32_t)5);

    TGridBoolFuturePtr rmvFut = rmtCache->removeAllAsync(keysRemove);

    // Values may also be replaced.
    rmtCache->replace(key0, "newest value for 0");

    // Asynchronous replace is supported, too.
    futRes = rmtCache->replaceAsync(key0, "newest value for 0");

    rmtCache->put(key0, (int32_t)0);

    // Compare and set are implemented, too.
    rmtCache->cas(key0, "newest cas value for 0", (int32_t)0);

    // CAS can be asynchronous.
    futRes = rmtCache->casAsync(key0, "newest cas value for 0", (int32_t)0);

    // It's possible to obtain cache metrics using data client API.
    GridClientDataMetrics metrics = rmtCache->metrics();

    TGridClientFutureDataMetrics futMetrics = rmtCache->metricsAsync();

    futMetrics->get();
}

BOOST_FIXTURE_TEST_CASE(clientComputeExample, GridClientFactoryFixture1<clientConfig2>) {
    TGridClientComputePtr clientCompute = client->compute();

    TGridClientNodeList nodes = clientCompute->nodes();

    BOOST_REQUIRE(nodes.size() > 0);

    GridClientUuid randNodeId = nodes[0]->getNodeId();

    TGridClientNodePtr p = clientCompute->node(randNodeId);

    TGridClientComputePtr prj = clientCompute->projection(*p);

    vector<GridClientUuid> uuids;

    uuids.push_back(randNodeId);

    nodes = prj->nodes(uuids);

    std::function < bool(const GridClientNode&) > filter = [&randNodeId](const GridClientNode& n) { return n.getNodeId() == randNodeId; };
    nodes = prj->nodes(filter);

    // Information about nodes may be refreshed explicitly.
    TGridClientNodePtr clntNode = prj->refreshNode(randNodeId, true, true);

    TGridClientNodeFuturePtr futClntNode = prj->refreshNodeAsync(randNodeId, false, false);

    futClntNode->get();

    // Nodes may also be refreshed by IP address.
    string clntAddr = SERVER_ADDRESS;

    vector<GridClientSocketAddress> addrs = clntNode->getTcpAddresses();

    if (addrs.size() > 0)
        clntAddr = addrs[0].host();

    clntNode = prj->refreshNode(clntAddr, true, true);

    // Asynchronous version.
    futClntNode = prj->refreshNodeAsync(clntAddr, false, false);

    futClntNode->get();

    // Topology as a whole may be refreshed, too.
    TGridClientNodeList top = prj->refreshTopology(true, true);

    // Asynchronous version.
    TGridClientNodeFutureList topFut = prj->refreshTopologyAsync(false, false);

    topFut->get();

    try {
        vector<string> log = prj->log(0, 1);

        // Log entries may be fetched asynchronously.
        TGridFutureStringList futLog = prj->logAsync(1, 2);

        futLog->get();

        // Log file name can also be specified explicitly.
        log = prj->log("work/log/gridgain.log", 0, 1);

        // Asynchronous version supported as well.
        futLog = prj->logAsync("work/log/gridgain.log", 1, 2);

        futLog->get();
    }
    catch (const GridClientException&) {
    }
}

BOOST_AUTO_TEST_SUITE_END()
