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

#include <boost/shared_ptr.hpp>
#include <boost/test/unit_test.hpp>

#include "gridgain/gridclientvariant.hpp"

#include "gridgain/impl/gridclienttopology.hpp"
#include "gridgain/impl/gridclientdataprojection.hpp"
#include "gridgain/impl/gridclientpartitionedaffinity.hpp"
#include "gridgain/impl/gridclientshareddata.hpp"
#include "gridgain/impl/cmd/gridclienttcpcommandexecutor.hpp"
#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/hash/gridclientvarianthasheableobject.hpp"
#include "gridgain/impl/hash/gridclientsimpletypehasheableobject.hpp"
#include "gridgain/impl/hash/gridclientstringhasheableobject.hpp"
#include "gridgain/impl/hash/gridclientfloathasheableobject.hpp"
#include "gridgain/impl/hash/gridclientdoublehasheableobject.hpp"
#include "gridgain/impl/marshaller/gridnodemarshallerhelper.hpp"

using namespace std;

static const char* nodeUuids[] = { "878bc124-d7d8-4c63-af87-7607a1551efa", "ed4c4f89-96a8-4072-a750-72a2eca9249a" };

static const char* keyNames[] = { "key1", "key2" };

static const int intKeyNames[] = { 123 };

static GridHashIdResolver hashIdResolver = [] (const GridClientNode& node) {
    return GridClientVariant(node.getNodeId());
};

TNodesSet buildNodeSet(const char* ids[], int cnt) {
    TNodesSet nodes;

    for (int i = 0; i < cnt; ++i) {
        GridClientNode node;
        GridClientNodeMarshallerHelper helper(node);

        helper.setNodeId(GridClientUuid(nodeUuids[i]));

        nodes.insert(node);
    }

    return nodes;
}

GridClientConfiguration affinityClientConfiguration() {
    GridClientConfiguration cfg;
    vector<GridClientDataConfiguration> dataCfgVec;

    {
        GridClientDataConfiguration dataCfg;
        GridClientPartitionAffinity* aff = new GridClientPartitionAffinity();

        aff->setDefaultReplicas(512);
        aff->setHashIdResolver(hashIdResolver);

        dataCfg.name("cacheOne");
        dataCfg.affinity(shared_ptr<GridClientDataAffinity>(aff));

        dataCfgVec.push_back(dataCfg);
    }

    {
        GridClientDataConfiguration dataCfg;
        GridClientPartitionAffinity* aff = new GridClientPartitionAffinity();

        aff->setDefaultReplicas(512);
        aff->setHashIdResolver(hashIdResolver);

        dataCfg.name("cacheTwo");
        dataCfg.affinity(shared_ptr<GridClientDataAffinity>(aff));

        dataCfgVec.push_back(dataCfg);
    }

    cfg.dataConfiguration(dataCfgVec);

    return cfg;
}

/**
 *
 */
class NoopProjectionListener : public GridClientProjectionListener {
    virtual void onNodeIoFailed(const GridClientNode& n) {
        // No-op.
    }
};

BOOST_AUTO_TEST_SUITE(GridClientAffinitySelftTest)

BOOST_AUTO_TEST_CASE(testSimplePartitionedAffinity) {
    GridClientPartitionAffinity aff;

    aff.setDefaultReplicas(512);
    aff.setHashIdResolver(hashIdResolver);

    TNodesSet nodes(buildNodeSet(nodeUuids, (signed) (sizeof(nodeUuids) / sizeof(*nodeUuids))));

    BOOST_CHECK_EQUAL(
            GridClientUuid(nodeUuids[0]),
            aff.getNode(nodes, GridClientVariantHasheableObject(GridClientVariant(keyNames[0])))->getNodeId());

    BOOST_CHECK_EQUAL(
            GridClientUuid(nodeUuids[0]),
            aff.getNode(nodes, GridClientVariantHasheableObject(GridClientVariant(keyNames[1])))->getNodeId());

    BOOST_CHECK_EQUAL(
            GridClientUuid(nodeUuids[1]),
            aff.getNode(nodes, GridClientVariantHasheableObject(GridClientVariant(intKeyNames[0])))->getNodeId());
}

BOOST_AUTO_TEST_CASE(testCacheProjection) {
    GridClientConfiguration clientCfg(affinityClientConfiguration());

    TGridClientSharedDataPtr sharedData(
            new GridClientSharedData(GridClientUuid::randomUuid(), clientCfg,
                    std::shared_ptr<GridClientCommandExecutorPrivate>(
                            new GridClientTcpCommandExecutor(
                                    boost::shared_ptr<GridClientConnectionPool>(new GridClientConnectionPool(
                                            clientCfg))))));
    TNodesSet nodes;

    {
        // node 1 hosts the caches "cacheOne" and "cacheTwo"
        GridClientNode node;
        GridClientNodeMarshallerHelper helper(node);

        helper.setNodeId(GridClientUuid(nodeUuids[0]));

        TGridClientVariantMap cacheNames;
        cacheNames.insert(make_pair(GridClientVariant("cacheOne"), GridClientVariant("REPLICATED")));
        cacheNames.insert(make_pair(GridClientVariant("cacheTwo"), GridClientVariant("REPLICATED")));
        helper.setCaches(cacheNames);

        nodes.insert(node);
    }

    {
        // node 2 only hosts the cache "cacheOne"
        GridClientNode node;
        GridClientNodeMarshallerHelper helper(node);

        helper.setNodeId(GridClientUuid(nodeUuids[1]));

        TGridClientVariantMap cacheNames;
        cacheNames.insert(make_pair(GridClientVariant("cacheOne"), GridClientVariant("REPLICATED")));
        helper.setCaches(cacheNames);

        nodes.insert(node);
    }

    sharedData->topology()->update(nodes);

    TGridThreadPoolPtr tp(new GridThreadPool(1)); //just for stub

    NoopProjectionListener prjLsnr;

    unique_ptr<GridClientDataProjectionImpl> cache1(
        new GridClientDataProjectionImpl(
            sharedData,
            prjLsnr,
            "cacheOne",
            TGridClientNodePredicatePtr(),
            tp,
            std::set<GridClientCacheFlag>()));

    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[0]), cache1->affinity(GridClientVariant(keyNames[0])));
    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[0]), cache1->affinity(GridClientVariant(keyNames[1])));
    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[1]), cache1->affinity(GridClientVariant(intKeyNames[0])));

    unique_ptr<GridClientDataProjectionImpl> cache2(
        new GridClientDataProjectionImpl(
            sharedData,
            prjLsnr,
            "cacheTwo",
            TGridClientNodePredicatePtr(),
            tp,
            std::set<GridClientCacheFlag>()));

    // Both keys must map to the same node.
    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[0]), cache2->affinity(GridClientVariant(keyNames[0])));
    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[0]), cache2->affinity(GridClientVariant(keyNames[1])));
    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[0]), cache2->affinity(GridClientVariant(intKeyNames[0])));
}

BOOST_AUTO_TEST_CASE(testCacheTopologyChange) {
    GridClientConfiguration clientCfg(affinityClientConfiguration());

    TGridClientSharedDataPtr sharedData(
            new GridClientSharedData(GridClientUuid::randomUuid(), clientCfg,
                    std::shared_ptr<GridClientCommandExecutorPrivate>(
                            new GridClientTcpCommandExecutor(
                                    boost::shared_ptr<GridClientConnectionPool>(new GridClientConnectionPool(
                                            clientCfg))))));

    TNodesSet nodes;
    {
        // Initial topology with just one node.
        GridClientNode node;
        GridClientNodeMarshallerHelper helper(node);

        helper.setNodeId(GridClientUuid(nodeUuids[0]));

        TGridClientVariantMap cacheNames;
        cacheNames.insert(make_pair(GridClientVariant("cacheOne"), GridClientVariant("REPLICATED")));
        helper.setCaches(cacheNames);

        nodes.insert(node);
    }

    sharedData->topology()->update(nodes);

    TGridThreadPoolPtr tp(new GridThreadPool(1)); //just for stub

    NoopProjectionListener prjLsnr;

    unique_ptr<GridClientDataProjectionImpl> cache(
        new GridClientDataProjectionImpl(
            sharedData,
            prjLsnr,
            "cacheOne",
            TGridClientNodePredicatePtr(),
            tp,
            std::set<GridClientCacheFlag>()));

    // Both keys must map to the same node.
    BOOST_CHECK(GridClientUuid(nodeUuids[0])==cache->affinity(GridClientVariant(keyNames[0])));
    BOOST_CHECK(GridClientUuid(nodeUuids[0])==cache->affinity(GridClientVariant(keyNames[1])));

    {
        // Now add a new node.
        GridClientNode node;
        GridClientNodeMarshallerHelper helper(node);

        helper.setNodeId(GridClientUuid(nodeUuids[1]));

        TGridClientVariantMap cacheNames;
        cacheNames.insert(make_pair(GridClientVariant("cacheOne"), GridClientVariant("REPLICATED")));
        helper.setCaches(cacheNames);

        nodes.insert(node);
    }

    sharedData->topology()->update(nodes);

    // Now keys map to different nodes.
    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[0]), cache->affinity(GridClientVariant(keyNames[0])));
    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[0]), cache->affinity(GridClientVariant(keyNames[1])));
    BOOST_CHECK_EQUAL(GridClientUuid(nodeUuids[1]), cache->affinity(GridClientVariant(intKeyNames[0])));
}

/**
 * Create node with specified node id and replica count.
 *
 * @param nodeId Node id.
 * @param replicaCnt Node partitioned affinity replica count.
 * @return New node with specified node id and replica count.
 */
GridClientNode createNode(string nodeId, int replicaCnt) {
    GridClientNode node;
    GridClientNodeMarshallerHelper helper(node);

    helper.setNodeId(GridClientUuid(nodeId));
    helper.setReplicaCount(replicaCnt);

    return node;
}

TNodesSet nodesListToSet(const TNodesList& list) {
    TNodesSet ret;

    for (auto i = list.begin(); i != list.end(); i++)
        ret.insert(*i);

    return ret;
}

BOOST_AUTO_TEST_CASE(testPartitionedAffinity) {
    GridClientPartitionAffinity aff;

    aff.setHashIdResolver(hashIdResolver);

    TNodesList nodes;

    nodes.push_back(createNode("000ea4cd-f449-4dcb-869a-5317c63bd619", 50));
    nodes.push_back(createNode("010ea4cd-f449-4dcb-869a-5317c63bd62a", 60));
    nodes.push_back(createNode("0209ec54-ff53-4fdb-8239-5a3ac1fb31bd", 70));
    nodes.push_back(createNode("0309ec54-ff53-4fdb-8239-5a3ac1fb31ef", 80));
    nodes.push_back(createNode("040c9b94-02ae-45a6-9d5c-a066dbdf2636", 90));
    nodes.push_back(createNode("050c9b94-02ae-45a6-9d5c-a066dbdf2747", 100));
    nodes.push_back(createNode("0601f916-4357-4cfe-a7df-49d4721690bf", 110));
    nodes.push_back(createNode("0702f916-4357-4cfe-a7df-49d4721691c0", 120));

    TNodesSet nodesSet = nodesListToSet(nodes);

    {
        typedef GridStringHasheableObject KeyT;

        std::map<KeyT, int> data;

        data[KeyT("")] = 4;
        data[KeyT("asdf")] = 4;
        data[KeyT("224ea4cd-f449-4dcb-869a-5317c63bd619")] = 5;
        data[KeyT("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd")] = 2;
        data[KeyT("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636")] = 2;
        data[KeyT("d8f1f916-4357-4cfe-a7df-49d4721690bf")] = 7;
        data[KeyT("c77ffeae-78a1-4ee6-a0fd-8d197a794412")] = 3;
        data[KeyT("35de9f21-3c9b-4f4a-a7d5-3e2c6cb01564")] = 1;
        data[KeyT("d67eb652-4e76-47fb-ad4e-cd902d9b868a")] = 7;

        for (auto i = data.begin();
                i != data.end(); i++) {
            int nodeIdx = i->second;

            GridClientUuid exp = nodes[nodeIdx].getNodeId();
            GridClientUuid act = aff.getNode(nodesSet, i->first)->getNodeId();

            BOOST_CHECK_EQUAL( exp, act );
        }
    }

    {
        typedef GridInt64Hasheable KeyT;

        std::map<KeyT, int> data;

        data[1234567890L] = 7;
        data[12345678901L] = 2;
        data[123456789012L] = 1;
        data[1234567890123L] = 0;
        data[12345678901234L] = 1;
        data[123456789012345L] = 6;
        data[1234567890123456L] = 7;
        data[-1 * (int64_t)23456789012345L] = 4;
        data[-1 * (int64_t)2345678901234L] = 1;
        data[-1 * (int64_t)234567890123L] = 5;
        data[-1 * (int64_t)23456789012L] = 5;
        data[-1 * (int64_t)2345678901L] = 7;
        data[-1 * (int64_t)234567890L] = 4;
        data[0x8000000000000000L] = 4;
        data[0x7fffffffffffffffL] = 4;

        for (auto i = data.begin();
                i != data.end(); i++) {
            GridClientUuid exp = nodes[i->second].getNodeId();
            GridClientUuid act = aff.getNode(nodesSet, i->first)->getNodeId();

            BOOST_CHECK_EQUAL( exp, act );
        }
    }

    {
        typedef GridInt32Hasheable KeyT;

        std::map<KeyT, int> data;

        data[0] = 4;
        data[1] = 7;
        data[12] = 5;
        data[123] = 6;
        data[1234] = 4;
        data[12345] = 6;
        data[123456] = 6;
        data[1234567] = 6;
        data[12345678] = 0;
        data[123456789] = 7;
        data[1234567890] = 7;
        data[-234567890] = 7;
        data[-23456789] = 7;
        data[-2345678] = 0;
        data[-234567] = 6;
        data[-23456] = 6;
        data[-2345] = 6;
        data[-234] = 7;
        data[-23] = 5;
        data[-2] = 4;
        data[0x80000000] = 4;
        data[0x7fffffff] = 7;

        for (auto i = data.begin();
                i != data.end(); i++) {
            GridClientUuid exp = nodes[i->second].getNodeId();
            GridClientUuid act = aff.getNode(nodesSet, i->first)->getNodeId();

            BOOST_CHECK_EQUAL( exp, act );
        }
    }

    {
        typedef GridDoubleHasheableObject KeyT;

        std::map<KeyT, int> data;

        data[+1.1] = 3;
        data[-10.01] = 4;
        data[+100.001] = 4;
        data[-1000.0001] = 4;
        data[+1.7976931348623157E+308] = 6;
        data[-1.7976931348623157E+308] = 6;
        data[+4.9E-324] = 7;
        data[-4.9E-324] = 7;

        for (auto i = data.begin();
                i != data.end(); i++) {
            GridClientUuid exp = nodes[i->second].getNodeId();
            GridClientUuid act = aff.getNode(nodesSet, i->first)->getNodeId();

            BOOST_CHECK_EQUAL( exp, act );
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
