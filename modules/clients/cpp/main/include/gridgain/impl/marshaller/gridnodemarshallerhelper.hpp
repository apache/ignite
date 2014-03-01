/* @cpp.file.header */

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
    void setTcpAddresses(std::vector < GridSocketAddress >& pIntAddrs);

    /**
     * Sets REST HTTP server addresses.
     *
     * @param pExtAddrs List of address strings.
     */
    void setJettyAddresses(std::vector < GridSocketAddress >& pExtAddrs);

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
     * Sets the router TCP address.
     *
     * @param routerAddress Router address.
     */
    void setRouterJettyAddress(GridSocketAddress& routerAddress);

    /**
     * Sets the router HTTP address.
     *
     * @param routerAddress Router address.
     */
    void setRouterTcpAddress(GridSocketAddress& routerAddress);

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
