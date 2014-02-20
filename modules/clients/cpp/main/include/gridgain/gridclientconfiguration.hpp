// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_CONFIGURATION_HPP_INCLUDED
#define GRID_CLIENT_CONFIGURATION_HPP_INCLUDED

#include <gridgain/gridclientprotocolconfiguration.hpp>
#include <gridgain/gridclientdataconfiguration.hpp>
#include <gridgain/gridsocketaddress.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridconf.hpp>

/**
 * Client configuration class.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GRIDGAIN_API GridClientConfiguration {
public:
    /** Default topology refresh frequency in milliseconds. */
    enum { DFLT_TOP_REFRESH_FREQ = 5000, DFLT_THREAD_POOL_SIZE = 4, DFLT_MAX_CONN_IDLE_TIME = 30000 };

    /** Default public constructor. */
    GridClientConfiguration();

    /**
     * Copy constructor.
     *
     * @param other Another instance of GridClientConfiguration.
     */
    GridClientConfiguration(const GridClientConfiguration& other);

    /**
     * Assignment operator override.
     *
     * @param rhs Right-hand side of the assignment operator.
     * @return This instance of the class.
     */
    GridClientConfiguration& operator=(const GridClientConfiguration& rhs);

    /** Destructor. */
    virtual ~GridClientConfiguration();

    /**
    * Enables client to cache topology internally, so it does not have to
    * be always refreshed. Topology cache will be automatically refreshed
    * in the background every {@link #getTopologyRefreshFrequency()} interval.
    *
    * @return true if topology cache is enabled, false otherwise.
    */
    bool isEnableTopologyCache() const;

    /**
     * Sets a new value for topologyCacheEnabled flag.
     */
    void enableTopologyCache(bool enable);

    /**
    * Returns topology refresh frequency.
    *
    * @return Topology refresh frequency in milliseconds.
    */
    long topologyRefreshFrequency() const;

    /**
     * Sets a new value for topology refresh frequency in milliseconds.
     */
    void topologyRefreshFrequency(long frequency);

    /**
    * Default balancer to be used for the compute grid client. It can be overridden
    * for different compute instances.
    *
    * @return Default balancer to be used for computational client.
    */
    TGridClientLoadBalancerPtr loadBalancer() const;

    /**
     * Sets a new balancer.
     */
    void loadBalancer(TGridClientLoadBalancerPtr balancer);

    /**
    * Collection of GridSocketAddress <tt>'host:port'</tt> pairs representing
    * remote grid servers used to establish initial connection to
    * the grid. Once connection is established, GridGain will get
    * a full view on grid topology and will be able to connect to
    * any available remote node.
    *
    * @return Collection of  GridSocketAddress representing remote
    * grid servers.
    */
    std::vector<GridSocketAddress> servers() const;

    /**
     * Sets a new collection of servers to communicate with.
     */
     void servers(const std::vector<GridSocketAddress>& servers);

    /**
    * Returns configuration parameters used to establish the connection to the remote node.
    *
    * @ return GridClientProtocolConfiguration - The class defines the main parameters used to establish the connection
    * with the remote node.
    */
    GridClientProtocolConfiguration protocolConfiguration() const;

    /**
     * Sets a new value for a protocol configuration.
     */
    void protocolConfiguration(const GridClientProtocolConfiguration& config);

    /**
     * Returns a number of threads in the client thread pool, used to perform async operations.
     *
     * @return Current size of a thread pool.
     */
    size_t threadPoolSize() const;

    /**
     * Sets the number of threads in the client thread pool, used to perform async operations.
     */
    void threadPoolSize(size_t size);

    /**
    * Collection of GridSocketAddress <tt>'host:port'</tt> pairs representing
    * routers used to establish initial connection to
    * the grid.
    * This configuration parameter will not be used and
    * direct grid connection will be established if
    * {@link #getServers()} returns empty value.
    *
    * @return Collection of  GridSocketAddress representing remote
    * routers.
    */
    std::vector<GridSocketAddress> routers() const;

    /**
     * Sets a new collection of routers to communicate with.
     *
     * @param routers New routers list.
     */
     void routers(const std::vector<GridSocketAddress>& routers);

     /**
      * Returns a router balancer implementation for this
      * client configuration.
      *
      * @returns Router balancer instance.
      */
     TGridClientRouterBalancerPtr routerBalancer() const;

     /**
      * Sets a router balancer implementation for this client
      * configuration.
      *
      * @param routerBalancerPtr A router balancer implementation.
      */
     void routerBalancer(TGridClientRouterBalancerPtr routerBalancerPtr);

     /**
      * Gets maximum amount of time that client connection can be idle before it is closed.
      *
      * @return Maximum idle time in milliseconds.
      */
     int64_t maxConnectionIdleTime() const;

     /**
      * Sets maximum amount of time that client connection can be idle before it is closed.
      *
      * @param time Maximum idle time in milliseconds.
      */
     void maxConnectionIdleTime(int64_t time);

     /**
      * Gets the initial toplogy listener list for the client.
      *
      * @return Topology listener list.
      */
     TGridClientTopologyListenerList topologyListeners() const;

     /**
      * Sets initial list of topology listeners, that are added to the client
      * before it starts.
      *
      * After the client has started, the listener list can be managed with
      * addTopologyListener() and removeToplogyListener() client methods.
      *
      * @param topLsnr Toplogy listeners list to add to the client.
      */
     void topologyListeners(const TGridClientTopologyListenerList& topLsnrs);

     /**
      * Gets the client configuration for caches.
      *
      * @return Client configuration for caches.
      */
     std::vector<GridClientDataConfiguration> dataConfiguration() const;

     /**
      * Sets the client configuration for caches.
      *
      * @param cfgs Client configuration for caches.
      */
     void dataConfiguration(const std::vector<GridClientDataConfiguration>& cfgs);

private:
    class Impl;
    Impl* pimpl;
};

#endif
