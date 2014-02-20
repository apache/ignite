// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_NODE_HPP_INCLUDED
#define GRID_CLIENT_NODE_HPP_INCLUDED

#include <string>
#include <set>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclientnodemetricsbean.hpp>
#include <gridgain/gridclientuuid.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridsocketaddress.hpp>
#include <gridgain/gridclientprotocol.hpp>

/**
 * Grid client node bean.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GRIDGAIN_API GridClientNode {
public:
    /** Default constructor. */
    GridClientNode();

    /**
     * Copy constructor.

     * @param other Node instance.
     */
    GridClientNode(const GridClientNode& other);

    /**
     * Assignment operator override.
     *
     * @param rhs Right-hand side of the assignment operator.
     * @return This instance of the class.
     */
    GridClientNode& operator=(const GridClientNode& rhs);

    /** Destructor */
    virtual ~GridClientNode();

    /**
     * Gets node ID.
     *
     * @return Node Id.
     */
    GridUuid getNodeId() const;

    /**
     * Gets node consistent ID.
     *
     * @return Node consistent Id.
     */
    GridClientVariant getConsistentId() const;

    /**
     * Gets REST TCP server addresses.
     *
     * @return List of address strings.
     */
    std::vector<std::string> getTcpAddresses() const;

    /**
     * Gets REST HTTP server addresses.
     *
     * @return List of address strings.
     */
    std::vector<std::string> getJettyAddresses() const;

    /**
     * Gets metrics.
     *
     * @return Metrics.
     */
    GridClientNodeMetricsBean getMetrics() const;

    /**
     * Gets attributes.
     *
     * @return Attributes.
     */
    TGridClientVariantMap getAttributes() const;

    /**
     * Gets REST binary protocol port.
     *
     * @return Port on which REST binary protocol is bound.
     */
    int getTcpPort() const;

    /**
     * Gets REST http protocol port.
     *
     * @return Http port.
     */
    int getJettyPort() const;

    /**
     * Gets configured node caches.
     *
     * @return std::map where key is cache name and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").
     */
    TGridClientVariantMap getCaches() const;

    /**
     * Gets mode for cache with null name.
     *
     * @return Default cache mode.
     */
    std::string getDefaultCacheMode() const;

    /**
     * Returns a list of available addresses by protocol.
     *
     * @param proto Protocol - TCP or HTTP
     * @return List of host/port pairs.
     */
    std::vector<GridSocketAddress> availableAddresses(GridClientProtocol proto) const;

    /**
     * Returns the router address.
     *
     * @return Router address (IP or host name) or empty string, if absent.
     */
    std::string getRouterAddress() const;

    /**
     * Returns the router TCP port.
     *
     * @return Router TCP port number or -1, if absent.
     */
    int getRouterTcpPort() const;

    /**
     * Gets the router HTTP port.
     *
     * @return Router HTTP port number or -1, if absent.
     */
    int getRouterJettyPort() const;

    /**
     * Returns the number of replicas for this node.
     *
     * @return Replicas count.
     */
    int getReplicaCount() const;

    /**
     * Returns a string representation of this node, useful for debug
     * and monitoring.
     *
     * @return A string representation.
     */
    std::string toString() const;

private:
    class Impl;
    Impl* pimpl;

    friend class GridNodeMarshallerHelper;
};

/**
 * Prints node to stream
 *
 * @param out Stream to output node to.
 * @param n Node.
 */
GRIDGAIN_API std::ostream& operator<<(std::ostream &out, const GridClientNode &n);

/** Client node comparator for set. */
struct ClientNodeComparator {
    bool operator() (const GridClientNode& n1, const GridClientNode& n2) const {
        return n1.getNodeId() < n2.getNodeId();
    }
};

/** Typedef for node set. */
typedef std::set<GridClientNode, ClientNodeComparator> TNodesSet;

#endif
