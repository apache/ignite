// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <vector>
#include <sstream>

#include <gridgain/gridgain.hpp>

#include <boost/lexical_cast.hpp>

using namespace std;

static string SERVER_ADDRESS = "127.0.0.1";
static string CACHE_NAME = "partitioned";
static int KEYS_CNT = 10;
static int TCP_PORT = 12100;
static int HTTP_PORT = 8081;

GridClientConfiguration tcpClientConfiguration() {
    GridClientConfiguration clientConfig;

    vector<GridSocketAddress> routers;

    cout << "Connecting to router " << SERVER_ADDRESS << ", port " << TCP_PORT << endl;

    routers.push_back(GridSocketAddress(SERVER_ADDRESS, TCP_PORT));

    clientConfig.routers(routers);

    GridClientProtocolConfiguration protoCfg;
    protoCfg.protocol(TCP);
    protoCfg.credentials("s3cret");

    clientConfig.protocolConfiguration(protoCfg);

    return clientConfig;
}

GridClientConfiguration httpClientConfiguration() {
    GridClientConfiguration clientConfig;

    vector<GridSocketAddress> routers;

    cout << "Connecting to router " << SERVER_ADDRESS << ", port " << HTTP_PORT << endl;

    routers.push_back(GridSocketAddress(SERVER_ADDRESS, HTTP_PORT));

    clientConfig.routers(routers);

    GridClientProtocolConfiguration protoCfg;
    protoCfg.protocol(HTTP);

    clientConfig.protocolConfiguration(protoCfg);

    return clientConfig;
}

int main() {
    TGridClientPtr client = GridClientFactory::start(tcpClientConfiguration());

    TGridClientComputePtr cc = client->compute();

    TGridClientNodeList nodes = cc->nodes();

    if (nodes.empty()) {
        cerr << "Failed to connect to grid in cache example, make sure that it is started and connection "
                "properties are correct." << endl;

        GridClientFactory::stopAll();

        return EXIT_FAILURE;
    }

    cout << "Current grid topology: " << nodes.size() << endl;

    for (TGridClientNodeList::iterator i = nodes.begin(); i != nodes.end(); i++) {
        cout << **i << endl;
    }

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

        string key=boost::lexical_cast<string>(i);

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

    return EXIT_SUCCESS;
}
