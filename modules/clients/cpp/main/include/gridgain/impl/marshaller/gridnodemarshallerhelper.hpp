// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_MARSHALLER_ACCESSOR_INCLUDED
#define GRID_MARSHALLER_ACCESSOR_INCLUDED

#include <string>
#include <vector>

#include "gridgain/gridclienttypedef.hpp"

class GridUuid;
class GridClientNodeMetricsBean;

class GridNodeMarshallerHelper {
public:
    GridNodeMarshallerHelper(const GridClientNode& node) : node_(node){}

    /**
     * Sets node ID.
     *
     * @param pNodeId Node ID.
     */
    void setNodeId(const GridUuid& pNodeId);

    /**
     * Sets node consistent ID.
     *
     * @param pId Node consistent ID.
     */
    void setConsistentId(const GridClientVariant& pId);

    /**
     * Sets REST TCP server addresses.
     *
     * @param pIntAddrs List of address strings.
     */
    void setTcpAddresses(const std::vector<std::string>& pIntAddrs);

    /**
     * Sets REST HTTP server addresses.
     *
     * @param pExtAddrs List of address strings.
     */
    void setJettyAddresses(const std::vector<std::string>& pExtAddrs);

    /**
     * Sets metrics.
     *
     * @param pMetrics Metrics.
     */
    void setMetrics(const GridClientNodeMetricsBean& pMetrics);

    /**
     * Sets attributes.
     *
     * @param pAttrs Attributes.
     */
    void setAttributes(const TGridClientVariantMap& pAttrs);

    /**
     * Sets REST binary protocol port.
     *
     * @param pTcpPort Port on which REST binary protocol is bound.
     */
    void setTcpPort(int pTcpPort);

    /**
     * Sets REST http port.
     *
     * @param pJettyPort REST http port.
     */
    void setJettyPort(int pJettyPort);

    /**
     * Sets configured node caches.
     *
     * @param pCaches std::map where key is cache name and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").
     */
    void setCaches(const TGridClientVariantMap& pCaches);

    /**
     * Sets mode for default cache.
     *
     * @param dfltCacheMode Default cache mode.
     */
    void setDefaultCacheMode(const std::string& dfltCacheMode);

    /**
     * Sets the router address.
     *
     * @param routerAddress Router address.
     */
    void setRouterAddress(const std::string& routerAddress);

    /**
     * Sets the router TCP port.
     *
     * @param tcpPort Router TCP port.
     */
    void setRouterTcpPort(int tcpPort);

    /**
     * Sets the router HTTP port.
     *
     * @param jettyPort Router HTTP port.
     */
    void setRouterJettyPort(int jettyPort);

    /**
     * Sets the number of replicas for this node.
     *
     * @param count Replicas count.
     */
    void setReplicaCount(int count);

private:
    const GridClientNode& node_;
};

#endif // GRID_MARSHALLER_ACCESSOR_INCLUDED
