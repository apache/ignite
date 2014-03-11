/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client;

import org.gridgain.client.balancer.*;

/**
 * Java client data configuration.
 */
public class GridClientDataConfiguration {
    /** Grid cache name. */
    private String name;

    /** Affinity. */
    private GridClientDataAffinity affinity;

    /** Balancer for pinned mode. */
    private GridClientLoadBalancer balancer = new GridClientRandomBalancer();

    /**
     * Creates empty configuration.
     */
    public GridClientDataConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     */
    public GridClientDataConfiguration(GridClientDataConfiguration cfg) {
        // Preserve alphabetic order for maintenance.
        affinity = cfg.getAffinity();
        balancer = cfg.getPinnedBalancer();
        name = cfg.getName();
    }

    /**
     * Gets name of remote cache.
     *
     * @return Remote cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets grid cache name for this configuration.
     *
     * @param name Cache name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets affinity to be used to communicate with remote cache.
     * Affinity allows to contact exactly the node where the data is and therefore
     * avoiding extra network hops.
     * <p>
     * Use {@link GridClientPartitionAffinity} as default affinity communication to
     * work with remote partitioned caches.
     *
     * @return Cache affinity to use.
     */
    public GridClientDataAffinity getAffinity() {
        return affinity;
    }

    /**
     * Sets client data affinity for this configuration.
     * Affinity allows to contact exactly the node where the data is and therefore
     * avoiding extra network hops.
     * <p>
     * Use {@link GridClientPartitionAffinity} as default affinity communication to
     * work with remote partitioned caches.
     *
     * @param affinity Client data affinity.
     */
    public void setAffinity(GridClientDataAffinity affinity) {
        this.affinity = affinity;
    }

    /**
     * Gets balancer to use for pinned nodes. See {@link GridClientLoadBalancer}
     * for more information.
     *
     * @return Node balancer for pinned mode.
     */
    public GridClientLoadBalancer getPinnedBalancer() {
        return balancer;
    }

    /**
     * Sets balancer for pinned mode for this configuration.
     *
     * @param balancer Balancer that will be used in pinned mode.
     */
    public void setBalancer(GridClientLoadBalancer balancer) {
        this.balancer = balancer;
    }
}
