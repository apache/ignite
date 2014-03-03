/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

/**
 * <summary>
 * This example demonstrates use of GridGain C++ remote client API. To compile this example
 * you first need to compile the API, located in GRIDGAIN_HOME/modules/clients/cpp (see README
 * file for compilation instructions).
 * <p>
 * To execute this example you should start one or more instances of <c>GridClientCacheExampleNodeStartup</c>
 * Java class which will start up a GridGain node with proper configuration (you can compile
 * and run this class from your favourite IDE).
 * <p>
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to <c>ggstart.{sh|bat}</c> script, like so:
 * <c>ggstart.sh examples/config/example-cache.xml'</c>, but this example will only work if
 * the GRIDGAIN_HOME/examples classes are in the node's classpath.
 * <p>
 * After node has been started this example creates a client connection and performs some
 * In-Memory Data Grid related operations.
 * </summary>
 */

#include "gridclientapiexample.hpp"

#include <unordered_map>

using namespace std;

/**
 * Runs an In-Memory Data Grid client example.
 *
 * @param client A client reference.
 */
void clientDataExample(TGridClientPtr& client) {
    TGridClientComputePtr cc = client->compute();

    TGridClientNodeList nodes = cc->nodes();

    if (nodes.empty()) {
        cerr << "Failed to connect to grid in cache example, make sure that it is started and connection "
                "properties are correct." << endl;

        GridClientFactory::stopAll();

        return;
    }

    cout << "Current grid topology: " << nodes.size() << endl;

    // Random node ID.
    GridUuid randNodeId = nodes[0]->getNodeId();

    // Get client projection of grid partitioned cache.
    TGridClientDataPtr rmtCache = client->data(CACHE_NAME);

    TGridClientVariantSet keys;

    // Put some values to the cache.
    for (int32_t i = 0; i < KEYS_CNT; i++) {
        ostringstream oss;

        oss << "val-" << i;

        string v = oss.str();

        string key = boost::lexical_cast<string>(i);

        rmtCache->put(key, v);

        GridUuid nodeId = rmtCache->affinity(key);

        cout << ">>> Storing key " << key << " on node " << nodeId << endl;

        keys.push_back(key);
    }

    TGridClientNodeList nodelst;
    TGridClientNodePtr p = client->compute()->node(randNodeId);

    nodelst.push_back(p);

    // Pin a remote node for communication. All further communication
    // on returned projection will happen through this pinned node.
    TGridClientDataPtr prj = rmtCache->pinNodes(nodelst);

    GridClientVariant key0 = GridClientVariant(boost::lexical_cast<string>(0));

    GridClientVariant key6 = GridClientVariant(boost::lexical_cast<string>(6));

    GridClientVariant val = prj->get(key0);

    cout << ">>> Loaded single value: " << val.debugString() << endl;

    TGridClientVariantMap vals = prj->getAll(keys);

    cout << ">>> Loaded multiple values, size: " << vals.size() << endl;

    for (TGridClientVariantMap::const_iterator iter = vals.begin(); iter != vals.end(); ++iter)
        cout << ">>> Loaded cache entry [key=" << iter->first <<
                ", val=" << iter->second << ']' << endl;

    TGridBoolFuturePtr futPut = prj->putAsync(key0, "new value for 0");

    cout << ">>> Result of asynchronous put: " << (futPut->get() ? "success" : "failure") << endl;

    unordered_map<GridUuid, TGridClientVariantMap> keyVals;

    GridUuid nodeId = rmtCache->affinity(key0);

    for (int32_t i = 0; i < KEYS_CNT; i++) {
        ostringstream oss;

        oss << "updated-val-" << i;

        string v = oss.str();

        string key = boost::lexical_cast<string>(i);

        keyVals[nodeId][key] = v;
    }

    for (unordered_map<GridUuid, TGridClientVariantMap>::iterator iter = keyVals.begin();
            iter != keyVals.end(); ++iter)
       rmtCache->putAll(iter->second);

    vector<TGridBoolFuturePtr> futs;

    for (unordered_map<GridUuid, TGridClientVariantMap>::iterator iter = keyVals.begin();
            iter != keyVals.end(); ++iter) {
       TGridBoolFuturePtr fut = rmtCache->putAllAsync(iter->second);

       futs.push_back(fut);
    }

    for (vector<TGridBoolFuturePtr>::iterator iter = futs.begin(); iter != futs.end(); ++iter)
       cout << ">>> Result of async putAll: " << (iter->get()->get() ? "success" : "failure") << endl;

    cout << ">>> Value for key " << key0.debugString() << " is " <<
            rmtCache->get(key0).debugString() << endl;

    // Asynchronous gets, too.
    TGridClientFutureVariant futVal = rmtCache->getAsync(key0);

    cout << ">>> Asynchronous value for key " << key0.debugString() << " is " <<
        futVal->get().debugString() << endl;

    // Multiple values can be fetched at once. Here we batch our get
    // requests by affinity nodes to ensure least amount of network trips.
    for (unordered_map<GridUuid, TGridClientVariantMap>::iterator iter = keyVals.begin();
            iter != keyVals.end(); ++iter) {
       GridUuid uuid = iter->first;

       TGridClientVariantMap m = iter->second;

       TGridClientVariantSet keys;

       for (TGridClientVariantMap::const_iterator miter = m.begin(); miter != m.end(); ++miter )
           keys.push_back(miter->first);

       // Since all keys in our getAll(...) call are mapped to the same primary node,
       // grid cache client will pick this node for the request, so we only have one
       // network trip here.

       TGridClientVariantMap map = rmtCache->getAll(keys);

       cout << ">>> Values from node [nodeId=" + uuid.uuid() + ", values=[" << endl;

       for (TGridClientVariantMap::const_iterator miter = map.begin(); miter != map.end(); ++miter) {
           if (miter != map.begin())
               cout << ", " << endl;
           cout << "[key=" << miter->first << ", value=" << miter->second << ']';
       }

       cout << ']' << endl;
    }

    // Multiple values may be retrieved asynchronously, too.
    // Here we retrieve all keys at once. Since this request
    // will be sent to some grid node, this node may not be
    // the primary node for all keys and additional network
    // trips will have to be made within grid.

    TGridClientVariantMap map = rmtCache->getAllAsync(keys)->get();

    cout << ">>> Values retrieved asynchronously: " << endl;

    for (TGridClientVariantMap::const_iterator miter = map.begin(); miter != map.end(); ++miter) {
        if (miter != map.begin())
            cout << ", " << endl;

        cout << "[key=" << miter->first << ", value=" << miter->second << ']';
    }

    cout << endl;

    // Contents of cache may be removed one by one synchronously.
    // Again, this operation is affinity aware and only the primary
    // node for the key is contacted.
    bool res = rmtCache->remove(key0);

    cout << ">>> Result of removal: " << (res ? "success" : "failure") << endl;

    // ... and asynchronously.
    TGridBoolFuturePtr futRes = rmtCache->removeAsync(boost::lexical_cast<string>(1));

    cout << ">>> Result of asynchronous removal: " << (futRes->get() ? "success" : "failure") << endl;

    // Multiple entries may be removed at once synchronously...
    TGridClientVariantSet keysRemove;

    keysRemove.push_back(boost::lexical_cast<string>(2));
    keysRemove.push_back(boost::lexical_cast<string>(3));

    bool rmvRslt = rmtCache->removeAll(keysRemove);

    cout << ">>> Result of removeAll: " << (rmvRslt ? "success" : "failure") << endl;

    // ... and asynchronously.
    keysRemove.clear();

    keysRemove.push_back(boost::lexical_cast<string>(4));
    keysRemove.push_back(boost::lexical_cast<string>(5));

    TGridBoolFuturePtr rmvFut = rmtCache->removeAllAsync(keysRemove);

    cout << ">>> Result of asynchronous removeAll: " << (rmvFut->get() ? "success" : "failure") << endl;

    // Values may also be replaced.
    res = rmtCache->replace(key6, "newer value for 6");

    cout << ">>> Result for replace for existent key6 is " << (res ? "success" : "failure") << endl;

    // Asynchronous replace is supported, too. This one is expected to fail, since key0 doesn't exist.
    futRes = rmtCache->replaceAsync(key0, "newest value for 0");

    res = futRes->get();

    cout << ">>> Result for asynchronous replace for nonexistent key0 is " <<
            (res ? "success" : "failure") << endl;

    rmtCache->put(key0, boost::lexical_cast<string>(0));

    // Compare and set are implemented, too.
    res = rmtCache->cas(key0, "newest cas value for 0", boost::lexical_cast<string>(0));

    cout << ">>> Result for put using cas is " << (res ? "success" : "failure") << endl;

    // CAS can be asynchronous.
    futRes = rmtCache->casAsync(key0, boost::lexical_cast<string>(0), "newest cas value for 0");

    res = futRes->get();

    // Expected to fail.
    cout << ">>> Result for put using asynchronous cas is "
        << (res? "success" : "failure") << endl;

    // It's possible to obtain cache metrics using data client API.
    GridClientDataMetrics metrics = rmtCache->metrics();
    cout << ">>> Cache metrics : " << metrics << endl;

    TGridClientFutureDataMetrics futMetrics = rmtCache->metricsAsync();

    cout << ">>> Cache asynchronous metrics: " << futMetrics->get() << endl;

    key0 = GridClientVariant("0");

    GridClientVariant val0("new value for 0");

    rmtCache->put(key0, val0);

    randNodeId = nodes[0]->getNodeId();

    TGridClientComputePtr clientCompute = client->compute();

    cout << "RandNodeId is " << randNodeId.uuid() << endl;

    p = clientCompute->node(randNodeId);

    TGridClientComputePtr compPrj = clientCompute->projection(*p);

    GridClientVariant rslt = compPrj->affinityExecute("org.gridgain.examples.client.api.GridClientExampleTask", CACHE_NAME, key0);

    cout << ">>> Affinity execute : there are totally " << rslt.toString() << " test entries on the grid" <<
            endl;

    futVal = compPrj->affinityExecuteAsync("org.gridgain.examples.client.api.GridClientExampleTask", CACHE_NAME, key0);

    cout << ">>> Affinity execute async : there are totally " << futVal->get().toString() <<
        " test entries on the grid" << endl;
}

/**
 * Main method.
 *
 * @return Result code.
 */
int main () {
    try {
        GridClientConfiguration cfg = clientConfiguration();

        cout << "The client will try to connect to the following addresses:" << endl;

        vector<GridSocketAddress> srvrs = cfg.servers();

        for (vector<GridSocketAddress>::iterator i = srvrs.begin(); i < srvrs.end(); i++)
            cout << i->host() << ":" << i->port() << endl;

        TGridClientPtr client = GridClientFactory::start(cfg);

        clientDataExample(client);
    }
    catch(exception& e) {
        cerr << "Caught unhandled exception: " << e.what() << endl;
    }

    GridClientFactory::stopAll();
}
