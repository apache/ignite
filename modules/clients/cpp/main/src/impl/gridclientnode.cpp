// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <sstream>
#include <stdexcept>

#include "gridgain/gridclientvariant.hpp"
#include "gridgain/gridclientnode.hpp"
#include "gridgain/impl/utils/gridutil.hpp"
#include "gridgain/impl/marshaller/gridnodemarshallerhelper.hpp"

using namespace std;

class GridClientNode::Impl {
public:
    /** Default constructor. */
    Impl() : tcpPort(-1), jettyPort(-1), routerTcpPort(-1), routerJettyPort(-1),
        replicaCount(-1) {}

    /**
     * Copy costructor.
     *
     * @param other Node to copy data from.
     */
    Impl(const Impl& other) : nodeId(other.nodeId), tcpAddrs(other.tcpAddrs), jettyAddrs(other.jettyAddrs),
        tcpPort(other.tcpPort), jettyPort(other.jettyPort), metrics(other.metrics), attrs(other.attrs),
        dfltCacheMode(other.dfltCacheMode), caches(other.caches), routerAddress(other.routerAddress),
        routerTcpPort(other.routerTcpPort), routerJettyPort(other.routerJettyPort),
        replicaCount(other.replicaCount), consistentId(other.consistentId) {
    }

    /** Node ID */
    GridUuid nodeId;

    /** REST TCP addresses. */
    vector<string> tcpAddrs;

    /** REST HTTP addresses. */
    vector<string> jettyAddrs;

    /** Rest binary port. */
    int tcpPort;

    /** Rest HTTP(S) port. */
    int jettyPort;

    /** Metrics. */
    GridClientNodeMetricsBean metrics;

    /** Node attributes. */
    TGridClientVariantMap attrs;

    /** Mode for cache with {@code null} name. */
    string dfltCacheMode;

    /** Node caches. */
    TGridClientVariantMap caches;

    /** Router address. */
    string routerAddress;

    /** Router TCP port. */
    int routerTcpPort;

    /** Router HTTP port. */
    int routerJettyPort;

    /** Replicas count. */
    int replicaCount;

    /** Consistent ID. */
    GridClientVariant consistentId;
};

GridClientNode::GridClientNode(): pimpl(new Impl) {
}

GridClientNode::GridClientNode(const GridClientNode& other): pimpl(new Impl(*other.pimpl)){
}

GridClientNode& GridClientNode::operator=(const GridClientNode& rhs){
    if (this != &rhs) {
        delete pimpl;

        pimpl=new Impl(*rhs.pimpl);
    }

    return *this;
}

GridClientNode::~GridClientNode(){
    delete pimpl;
}

GridUuid GridClientNode::getNodeId() const {
    return pimpl->nodeId;
}

GridClientVariant GridClientNode::getConsistentId() const {
    return pimpl->consistentId;
}

vector<string> GridClientNode::getTcpAddresses() const {
    return pimpl->tcpAddrs;
}

vector<string> GridClientNode::getJettyAddresses() const {
    return pimpl->jettyAddrs;
}

GridClientNodeMetricsBean GridClientNode::getMetrics() const {
    return pimpl->metrics;
}

TGridClientVariantMap GridClientNode::getAttributes() const {
    return pimpl->attrs;
}

int GridClientNode::getTcpPort() const {
    return pimpl->tcpPort;
}

int GridClientNode::getJettyPort() const {
    return pimpl->jettyPort;
}

TGridClientVariantMap GridClientNode::getCaches() const {
    return pimpl->caches;
}

std::string GridClientNode::getDefaultCacheMode() const {
    return pimpl->dfltCacheMode;
}

/**
* Returns a list of available addresses by protocol.
*
* @param proto Protocol - TCP or HTTP
* @return List of host/port pairs.
*/
std::vector<GridSocketAddress> GridClientNode::availableAddresses(GridClientProtocol proto) const {
    std::vector<GridSocketAddress> sockAddrs;
    std::vector<std::string>* addrs;
    int port;

    switch (proto) {
    case TCP:
        addrs = &pimpl->tcpAddrs;
        port = pimpl->tcpPort;

        break;

    case HTTP:
        addrs = &pimpl->jettyAddrs;
        port = pimpl->jettyPort;

        break;

    default:
        throw std::logic_error("Unknown protocol.");
    }

    sockAddrs.reserve(addrs->size());

    for (auto iter = addrs->begin(); iter != addrs->end(); ++iter)
        sockAddrs.push_back(GridSocketAddress(*iter, port));

    return sockAddrs;
}

std::string GridClientNode::getRouterAddress() const {
    return pimpl->routerAddress;
}

int GridClientNode::getRouterTcpPort() const {
    return pimpl->routerTcpPort;
}

int GridClientNode::getRouterJettyPort() const {
    return pimpl->routerJettyPort;
}

int GridClientNode::getReplicaCount() const {
    return pimpl->replicaCount;
}

std::ostream& operator<<(std::ostream &out, const GridClientNode &n){
    out << "GridClientNode [nodeId=" << n.getNodeId().uuid();
    out << ", tcpAddrs=";
    GridUtil::toStream(out, n.getTcpAddresses());
    out << ", jettyAddrs=";
    GridUtil::toStream(out, n.getJettyAddresses());
    out << ", binaryPort=" << n.getTcpPort();
    out << ", jettyPort=" << n.getJettyPort();

    if (!n.getRouterAddress().empty()) {
        out << ", routerAddress=" << n.getRouterAddress()
                << ", routerTcpPort=" << n.getRouterTcpPort()
                << ", routerJettyPort=" << n.getRouterJettyPort();
    }

    out << ']';

    return out;
}

std::string GridClientNode::toString() const {
    std::ostringstream os;

    os << *this;

    return os.str();
}

void GridNodeMarshallerHelper::setNodeId(const GridUuid& pNodeId){
     node_.pimpl->nodeId = pNodeId;
}

void GridNodeMarshallerHelper::setTcpAddresses(const std::vector<std::string>& tcpAddrs) {
    node_.pimpl->tcpAddrs = tcpAddrs;
}

void GridNodeMarshallerHelper::setJettyAddresses(const std::vector<std::string>& jettyAddrs) {
    node_.pimpl->jettyAddrs = jettyAddrs;
}

void GridNodeMarshallerHelper::setMetrics(const GridClientNodeMetricsBean& pMetrics) {
    node_.pimpl->metrics = pMetrics;
}

void GridNodeMarshallerHelper::setAttributes(const TGridClientVariantMap& pAttrs) {
    node_.pimpl->attrs = pAttrs;
}

void GridNodeMarshallerHelper::setTcpPort(int pTcpPort) {
    node_.pimpl->tcpPort = pTcpPort;
}

void GridNodeMarshallerHelper::setJettyPort(int pJettyPort) {
    node_.pimpl->jettyPort = pJettyPort;
}

void GridNodeMarshallerHelper::setCaches(const TGridClientVariantMap& pCaches) {
    node_.pimpl->caches = pCaches;
}

void GridNodeMarshallerHelper::setDefaultCacheMode(const string& pDfltCacheMode) {
    node_.pimpl->dfltCacheMode = pDfltCacheMode;
}

void GridNodeMarshallerHelper::setConsistentId(const GridClientVariant& pId) {
    node_.pimpl->consistentId = pId;
}

void GridNodeMarshallerHelper::setReplicaCount(int count) {
    node_.pimpl->replicaCount = count;
}

void GridNodeMarshallerHelper::setRouterAddress(const std::string& routerAddress) {
    node_.pimpl->routerAddress = routerAddress;
}

void GridNodeMarshallerHelper::setRouterTcpPort(int tcpPort) {
    node_.pimpl->routerTcpPort = tcpPort;
}

void GridNodeMarshallerHelper::setRouterJettyPort(int jettyPort) {
    node_.pimpl->routerJettyPort = jettyPort;
}
