/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/gridclientconfiguration.hpp"
#include "gridgain/loadbalancer/gridclientrandombalancer.hpp"
#include "gridgain/loadbalancer/gridclientrandomrouterbalancer.hpp"

using namespace std;

class GridClientConfiguration::Impl {
public:
    Impl() : topologyCacheEnabled(true), topRefreshFreq(DFLT_TOP_REFRESH_FREQ),
        loadBalancer(TGridClientLoadBalancerPtr(new GridClientRandomBalancer())),
        threadPoolSize(DFLT_THREAD_POOL_SIZE),
        routerBalancer(TGridClientRouterBalancerPtr(new GridClientRandomRouterBalancer())),
        maxConnectionIdleTime(DFLT_MAX_CONN_IDLE_TIME),
        portableIdRslvr(nullptr) {}

    Impl(const Impl& other) : topologyCacheEnabled(other.topologyCacheEnabled),topRefreshFreq(other.topRefreshFreq),
        loadBalancer(other.loadBalancer), protoCfg(other.protoCfg), srvrs(other.srvrs),
        threadPoolSize(DFLT_THREAD_POOL_SIZE),
        routers(other.routers), routerBalancer(other.routerBalancer),
        maxConnectionIdleTime(other.maxConnectionIdleTime), topLsnrs(other.topLsnrs), dataCfgs(other.dataCfgs),
        portableIdRslvr(other.portableIdRslvr) {}

    /** Flag indicating if topology cache is enabled. */
    bool topologyCacheEnabled;

    /** Topology refresh frequency in milliseconds. */
    long topRefreshFreq;

    /** Load balancer. */
    TGridClientLoadBalancerPtr loadBalancer;

    /** Protocol configuration. */
    GridClientProtocolConfiguration protoCfg;

    /** Initial list of servers to connect to. */
    vector<GridClientSocketAddress> srvrs;

    /** Number of threads in the client thread pool. */
    size_t threadPoolSize;

    /** List of routers to connect to. */
    vector<GridClientSocketAddress> routers;

    TGridClientRouterBalancerPtr routerBalancer;

    /** Max idle time for client connection. */
    int64_t maxConnectionIdleTime;

    /** Initial toplogy listeners list. */
    TGridClientTopologyListenerList topLsnrs;

    /** Cache configuration. */
    std::vector<GridClientDataConfiguration> dataCfgs;

    /** Portable field id resolver. */
    GridPortableIdResolver* portableIdRslvr;
};

/** Default public constructor. */
GridClientConfiguration::GridClientConfiguration()
    : pimpl(new Impl) {
}

/**
* Copy constructor.
*
* @param other Another instance of GridClientConfiguration.
*/
GridClientConfiguration::GridClientConfiguration(const GridClientConfiguration& other)
    : pimpl(new Impl(*other.pimpl)){
}

GridClientConfiguration& GridClientConfiguration::operator=(const GridClientConfiguration& rhs){
    if (this != &rhs) {
        delete pimpl;

        pimpl = new Impl(*rhs.pimpl);
    }

    return *this;
}

GridClientConfiguration::~GridClientConfiguration(){
    delete pimpl;
}

/**
* Enables client to cache topology internally, so it does not have to
* be always refreshed. Topology cache will be automatically refreshed
* in the background every {@link #getTopologyRefreshFrequency()} interval.
*
* @return true if topology cache is enabled, false otherwise.
*/
bool GridClientConfiguration::isEnableTopologyCache() const {
    return pimpl->topologyCacheEnabled;
}

/**
* Sets a new value for topologyCacheEnabled flag.
*/
void GridClientConfiguration::enableTopologyCache(bool enable) {
    pimpl->topologyCacheEnabled = enable;
}

/**
* Gets topology refresh frequency.
*
* @return Topology refresh frequency in milliseconds.
*/
long GridClientConfiguration::topologyRefreshFrequency() const {
    return pimpl->topRefreshFreq;
}

/**
* Sets a new value for topology refresh frequency in milliseconds.
*/
void GridClientConfiguration::topologyRefreshFrequency(long frequency) {
    pimpl->topRefreshFreq = frequency;
}

/**
* Default balancer to be used for computational client. It can be overridden
* for different compute instances.
*
* @return Default balancer to be used for computational client.
*/
TGridClientLoadBalancerPtr GridClientConfiguration::loadBalancer() const {
    return pimpl->loadBalancer;
}

/**
    * Sets a new balancer.
    */
void GridClientConfiguration::loadBalancer(TGridClientLoadBalancerPtr balancer) {
    pimpl->loadBalancer = balancer;
}

/**
* Collection of GridClientSocketAddress {@code 'host:port'} pairs representing
* remote grid servers used to establish initial connection to
* the grid. Once connection is established, GridGain will get
* a full view on grid topology and will be able to connect to
* any available remote node.
*
* @return Collection of  GridClientSocketAddress representing remote
* grid servers.
*/
std::vector<GridClientSocketAddress> GridClientConfiguration::servers() const {
    return pimpl->srvrs;
}

/**
* Sets a new collection of servers to communicate with.
*/
void GridClientConfiguration::servers(const vector<GridClientSocketAddress>& servers) {
    pimpl->srvrs = servers;
}

/**
* Returns configuration parameters used to establish the connection to the remote node.
*
* @ return GridClientProtocolConfiguration - The class defines the main parameters used to establish the connection
* with the remote node.
*/
GridClientProtocolConfiguration GridClientConfiguration::protocolConfiguration() const {
    return pimpl->protoCfg;
}

/**
 * Sets a new value for a protocol configuration.
 */
void GridClientConfiguration::protocolConfiguration(const GridClientProtocolConfiguration& config) {
    pimpl->protoCfg = config;
}

/**
 * Returns a number of threads in the client thread pool, used to perform async operations.
 */
size_t GridClientConfiguration::threadPoolSize() const {
    return pimpl->threadPoolSize;
}

/**
 * Sets the number of threads in the client thread pool, used to perform async operations.
 */
void GridClientConfiguration::threadPoolSize(size_t size) {
    pimpl->threadPoolSize = size;
}

/**
 * Collection of GridClientSocketAddress <tt>'host:port'</tt> pairs representing
 * routers used to establish initial connection to
 * the grid.
 * This configuration parameter will not be used and
 * direct grid connection will be established if
 * {@link #getServers()} returns empty value.
 *
 * @return Collection of  GridClientSocketAddress representing remote
 * routers.
 */
std::vector<GridClientSocketAddress> GridClientConfiguration::routers() const {
    return pimpl->routers;
}

/**
 * Sets a new collection of routers to communicate with.
 *
 * @param routers New routers list.
 */
void GridClientConfiguration::routers(const std::vector<GridClientSocketAddress>& routers) {
    pimpl->routers = routers;
}

TGridClientRouterBalancerPtr GridClientConfiguration::routerBalancer() const {
    return pimpl->routerBalancer;
}

void GridClientConfiguration::routerBalancer(TGridClientRouterBalancerPtr routerBalancerPtr) {
    pimpl->routerBalancer = routerBalancerPtr;
}

int64_t GridClientConfiguration::maxConnectionIdleTime() const {
    return pimpl->maxConnectionIdleTime;
}

void GridClientConfiguration::maxConnectionIdleTime(int64_t time) {
    pimpl->maxConnectionIdleTime = time;
}

TGridClientTopologyListenerList GridClientConfiguration::topologyListeners() const {
    return pimpl->topLsnrs;
}

void GridClientConfiguration::topologyListeners(const TGridClientTopologyListenerList& topLsnrs) {
    pimpl->topLsnrs = topLsnrs;
}

std::vector<GridClientDataConfiguration> GridClientConfiguration::dataConfiguration() const {
    return pimpl->dataCfgs;
}

void GridClientConfiguration::dataConfiguration(const std::vector<GridClientDataConfiguration>& cfgs) {
    pimpl->dataCfgs = cfgs;
}

void GridClientConfiguration::portableIdResolver(GridPortableIdResolver* portableIdRslvr) {
    pimpl->portableIdRslvr = portableIdRslvr;
}

GridPortableIdResolver* GridClientConfiguration::portableIdResolver() const {
    return pimpl->portableIdRslvr;
}
