/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridclientapiexample.hpp"

#include <vector>

using namespace std;

GridClientConfiguration clientConfiguration() {
    GridClientConfiguration clientConfig;

    vector<GridSocketAddress> servers;

//    To enable communication with GridGain instance by HTTP, not by TCP, uncomment the following lines
//    and comment push_back with TCP.
//    ================================
//    GridClientProtocolConfiguration protoCfg;
//
//    protoCfg.protocol(HTTP);
//
//    clientConfig.setProtocolConfiguration(protoCfg);
//
//    servers.push_back(GridSocketAddress(SERVER_ADDRESS, GridClientProtocolConfiguration::DFLT_HTTP_PORT));

    for (int i = TCP_PORT; i < TCP_PORT + MAX_NODES; i++)
        servers.push_back(GridSocketAddress(SERVER_ADDRESS, i));

    clientConfig.servers(servers);

    return clientConfig;
}
