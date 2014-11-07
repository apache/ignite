/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Service deployment descriptor. Contains all service deployment configuration, and also
 * deployment topology snapshot as well as origin node ID.
 * <p>
 * Service descriptors can be retrieved by calling {@link GridServices#deployedServices()} method.
 */
public interface GridServiceDescriptor extends Serializable {
    /**
     * Gets service name.
     *
     * @return Service name.
     */
    public String name();

    /**
     * Gets service class.
     *
     * @return Service class.
     */
    public Class<? extends GridService> serviceClass();

    /**
     * Gets maximum allowed total number of deployed services in the grid, {@code 0} for unlimited.
     *
     * @return Maximum allowed total number of deployed services in the grid, {@code 0} for unlimited.
     */
    public int totalCount();

    /**
     * Gets maximum allowed number of deployed services on each node, {@code 0} for unlimited.
     *
     * @return Maximum allowed total number of deployed services on each node, {@code 0} for unlimited.
     */
    public int maxPerNodeCount();

    /**
     * Gets cache name used for key-to-node affinity calculation. This parameter is optional
     * and is set only when key-affinity service was deployed.
     *
     * @return Cache name, possibly {@code null}.
     */
    @Nullable public String cacheName();

    /**
     * Gets affinity key used for key-to-node affinity calculation. This parameter is optional
     * and is set only when key-affinity service was deployed.
     *
     * @return Affinity key, possibly {@code null}.
     */
    @Nullable public <K> K affinityKey();

    /**
     * Gets ID of grid node that initiated the service deployment.
     *
     * @return ID of grid node that initiated the service deployment.
     */
    public UUID originNodeId();

    /**
     * Gets service deployment topology snapshot. Service topology snapshot is represented
     * by number of service instances deployed on a node mapped to node ID.
     *
     * @return Map of number of service instances per node ID.
     */
    public Map<UUID, Integer> topologySnapshot();
}
