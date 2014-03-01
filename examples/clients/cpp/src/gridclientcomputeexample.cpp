// @cpp.file.header

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
 * To execute this example you should start one or more instances of <c>GridClientExampleNodeStartup</c>
 * Java class which will start up a GridGain node with proper configuration (you can compile
 * and run this class from your favourite IDE).
 * <p>
 * You can also start a stand-alone GridGain instance by passing the path
 * to configuration file to <c>ggstart.{sh|bat}</c> script, like so:
 * <c>ggstart.sh examples/config/example-cache.xml'</c>, but this example will only work if
 * the GRIDGAIN_HOME/examples classes are in the node's classpath.
 * <p>
 * After node has been started this example creates a client connection and performs some
 * Compute Grid related operations.
 * </summary>
 */

#include "gridclientapiexample.hpp"

using namespace std;

/**
 * Predicate for filtering nodes by UUIDs.
 */
class GridUuidNodePredicate : public GridClientPredicate<GridClientNode> {
public:
    GridUuidNodePredicate(const GridUuid& u) : uu(u) {
    }

    bool apply(const GridClientNode& node) const {
        return node.getNodeId() == uu;
    }

private:
    GridUuid uu;
};

/**
 * Runs a Compute Grid client example.
 *
 * @param client A client reference.
 */
void clientComputeExample(TGridClientPtr& client) {
    TGridClientComputePtr clientCompute = client->compute();

    TGridClientNodeList nodes = clientCompute->nodes();

    if (nodes.empty()) {
        cerr << "Failed to connect to grid in compute example, make sure that it is started and connection "
                "properties are correct." << endl;

        GridClientFactory::stopAll();

        return;
    }

    cout << "Current grid topology: " << nodes.size() << endl;

    GridUuid randNodeId = nodes[0]->getNodeId();

    cout << "RandNodeId is " << randNodeId.uuid() << endl;

    TGridClientNodePtr p = clientCompute->node(randNodeId);

    TGridClientComputePtr prj = clientCompute->projection(*p);

    GridClientVariant rslt = prj->execute("org.gridgain.examples.misc.client.api.ClientExampleTask");

    cout << ">>> GridClientNode projection : there are totally " << rslt.toString() <<
        " test entries on the grid" << endl;

    TGridClientNodeList prjNodes;

    prjNodes.push_back(p);

    prj = clientCompute->projection(prjNodes);

    rslt = prj->execute("org.gridgain.examples.misc.client.api.ClientExampleTask");

    cout << ">>> Collection execution : there are totally " << rslt.toString() <<
        " test entries on the grid" << endl;

    GridUuidNodePredicate* uuidPredicate = new GridUuidNodePredicate(randNodeId);

    TGridClientNodePredicatePtr predPtr(uuidPredicate);

    prj = clientCompute->projection(predPtr);

    rslt = prj->execute("org.gridgain.examples.misc.client.api.ClientExampleTask");

    cout << ">>> Predicate execution : there are totally " << rslt.toString() <<
        " test entries on the grid" << endl;

    // Balancing - may be random or round-robin. Users can create
    // custom load balancers as well.
    TGridClientLoadBalancerPtr balancer(new GridClientRandomBalancer());

    prj = clientCompute->projection(predPtr, balancer);

    rslt = prj->execute("org.gridgain.examples.misc.client.api.ClientExampleTask");

    cout << ">>> Predicate execution with balancer : there are totally " << rslt.toString() <<
            " test entries on the grid" << endl;

    // Now let's try round-robin load balancer.
    balancer = TGridClientLoadBalancerPtr(new GridClientRoundRobinBalancer());

    prj = prj->projection(prjNodes, balancer);

    rslt = prj->execute("org.gridgain.examples.misc.client.api.ClientExampleTask");

    cout << ">>> GridClientNode projection : there are totally " << rslt.toString() <<
            " test entries on the grid" << endl;

    TGridClientFutureVariant futVal = prj->executeAsync("org.gridgain.examples.misc.client.api.ClientExampleTask");

    cout << ">>> Execute async : there are totally " << futVal->get().toString() <<
       " test entries on the grid" << endl;

    vector<GridUuid> uuids;

    uuids.push_back(randNodeId);

    nodes = prj->nodes(uuids);

    cout << ">>> Nodes with UUID " << randNodeId.uuid() << " : ";

    for (size_t i = 0 ; i < nodes.size(); i++) {
        if (i != 0)
            cout << ", ";

        cout << *(nodes[i]);
    }

    cout << endl;

    // Nodes may also be filtered with predicate. Here
    // we create projection which only contains local node.
    GridUuidNodePredicate* uuidNodePredicate = new GridUuidNodePredicate(randNodeId);

    TGridClientNodePredicatePtr predFullPtr(uuidNodePredicate);

    nodes = prj->nodes(predFullPtr);

    cout << ">>> Nodes filtered with predicate : ";

    for (size_t i = 0 ; i < nodes.size(); i++) {
        if (i != 0)
            cout << ", ";

        cout << *(nodes[i]);
    }

    cout << endl;

    // Information about nodes may be refreshed explicitly.
    TGridClientNodePtr clntNode = prj->refreshNode(randNodeId, true, true);

    cout << ">>> Refreshed node : " << *clntNode << endl;

    TGridClientNodeFuturePtr futClntNode = prj->refreshNodeAsync(randNodeId, false, false);

    cout << ">>> Refreshed node asynchronously : " << *futClntNode->get() << endl;

    // Nodes may also be refreshed by IP address.
    string clntAddr = "127.0.0.1";

    vector<GridSocketAddress> addrs = clntNode->availableAddresses(TCP);

    if (addrs.size() > 0)
        clntAddr = addrs[0].host();

    clntNode = prj->refreshNode(clntAddr, true, true);

    cout << ">>> Refreshed node by IP : " << *clntNode << endl;

    // Asynchronous version.
    futClntNode = prj->refreshNodeAsync(clntAddr, false, false);

    cout << ">>> Refreshed node by IP asynchronously : " << *futClntNode->get() << endl;

    // Topology as a whole may be refreshed, too.
    TGridClientNodeList top = prj->refreshTopology(true, true);

    cout << ">>> Refreshed topology : ";

    for (size_t i = 0 ; i < top.size(); i++) {
        if (i != 0)
            cout << ", ";

        cout << *top[i];
    }

    cout << endl;

    // Asynchronous version.
    TGridClientNodeFutureList topFut = prj->refreshTopologyAsync(false, false);

    cout << ">>> Refreshed topology asynchronously : ";

    top = topFut->get();

    for (size_t i = 0; i < top.size(); i++) {
        if (i != 0)
            cout << ", ";

        cout << *top[i];
    }

    cout << endl;

    try {
        vector<string> log = prj->log(0, 1);

        cout << ">>> First log lines : " << endl;

        cout << log[0] << endl;
        cout << log[1] << endl;

        // Log entries may be fetched asynchronously.
        TGridFutureStringList futLog = prj->logAsync(1, 2);

        log = futLog->get();

        cout << ">>> First log lines fetched asynchronously : " << endl;
        cout << log[0] << endl;
        cout << log[1] << endl;

        // Log file name can also be specified explicitly.
        log = prj->log("work/log/gridgain.log", 0, 1);

        cout << ">>> First log lines from log file work/log/gridgain.log : " << endl;
        cout << log[0] << endl;
        cout << log[1] << endl;

        // Asynchronous version supported as well.
        futLog = prj->logAsync("work/log/gridgain.log", 1, 2);

        log = futLog->get();

        cout << ">>> First log lines from log file work/log/gridgain.log fetched asynchronously : " << endl;
        cout << log[0] << endl;
        cout << log[1] << endl;
    }
    catch (GridClientException&) {
        cout << "Log file was not found " << endl;
    }

    cout << "End of example." << endl;
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

        clientComputeExample(client);
    }
    catch(exception& e) {
        cerr << "Caught unhandled exception: " << e.what() << endl;
    }

    GridClientFactory::stopAll();
}


