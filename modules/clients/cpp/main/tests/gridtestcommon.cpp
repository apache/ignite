/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridtestcommon.hpp"

#include <ctime>
#include <vector>

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "gridgain/impl/utils/gridutil.hpp"

using namespace std;

static boost::uuids::random_generator uuidGen;
static boost::mutex rndMux;
boost::mutex checkMux;

string genRandomUniqueString(string prefix) {
    boost::lock_guard<boost::mutex> g(rndMux);

    std::stringstream ss;

    ss << prefix << uuidGen();

    return ss.str();
}

/**
 * Returns current system time in milliseconds.
 */
int64_t currentTimeMillis() {
    return GridUtil::currentTimeMillis();
}

/**
 * Returns a random string, based on the prefix.
 *
 * @param prefix A prefix for random string.
 * @return Random string.
 */
string getRandomName(string prefix) {
   // Initialize random seed.
    static class InitSeed {
    public:
        InitSeed() {
            srand((unsigned)time(NULL));
        }
    } seed;

   // Generate the random node index.
   int randomIdx = rand();

   return  prefix + "_" + boost::lexical_cast<std::string>(randomIdx);
}

#ifdef GRIDGAIN_ROUTER_TEST

static int TEST_TCP_ROUTER_PORT = 12100;

GridClientConfiguration clientConfig() {
    GridClientConfiguration clientConfig;

    vector<GridClientSocketAddress> routers;

    routers.push_back(GridClientSocketAddress("127.0.0.1", TEST_TCP_ROUTER_PORT));

    clientConfig.routers(routers);

    GridClientProtocolConfiguration protoCfg;

    protoCfg.credentials(CREDS);

    protoCfg.protocol(TCP);

    clientConfig.protocolConfiguration(protoCfg);

    return clientConfig;
}

#else //GRIDGAIN_ROUTER_TEST not defined (no router)

GridClientConfiguration clientConfig() {
    GridClientConfiguration clientConfig;

    vector<GridClientSocketAddress> servers;

    servers.push_back(GridClientSocketAddress("127.0.0.1", TEST_TCP_PORT));

    clientConfig.servers(servers);

    GridClientProtocolConfiguration protoCfg;

    protoCfg.credentials(CREDS);

    protoCfg.protocol(TCP);

    clientConfig.protocolConfiguration(protoCfg);

    return clientConfig;
}

#endif
