/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.tcp.*;

import java.util.*;

/**
 * Metrics store interface for {@link GridTcpDiscoverySpi}.
 */
public interface GridTcpDiscoveryMetricsStore {
    /**
     * Callback invoked when SPI context is initialized after {@link GridTcpDiscoverySpi#spiStart(String)}
     * method is completed, SPI context can be stored for future access.
     *
     * @param spiCtx Spi context.
     * @throws GridSpiException In case of error.
     */
    public void onSpiContextInitialized(GridSpiContext spiCtx) throws GridSpiException;

    /**
     * Callback invoked prior to stopping grid before SPI context is destroyed.
     * Note that invoking SPI context after this callback is complete is considered
     * illegal and may produce unknown results.
     */
    public void onSpiContextDestroyed();

    /**
     * Updates local node metrics in the store.
     *
     * @param locNodeId Id of the local node (caller node).
     * @param metrics Local node metrics.
     * @throws GridSpiException If an error occurs.
     */
    public void updateLocalMetrics(UUID locNodeId, GridNodeMetrics metrics) throws GridSpiException;

    /**
     * Gets metrics map for provided nodes.
     *
     * @param nodeIds Nodes to get metrics for.
     * @return Map, containing metrics for provided nodes. Potentially empty, but never null.
     * @throws GridSpiException If an error occurs.
     */
    public Map<UUID, GridNodeMetrics> metrics(Collection<UUID> nodeIds)  throws GridSpiException;

    /**
     * Removes metrics of provided nodes from the store.
     *
     * @param nodeIds Nodes to remove metrics of.
     * @throws GridSpiException If an error occurs.
     */
    public void removeMetrics(Collection<UUID> nodeIds) throws GridSpiException;

    /**
     * Gets all node IDs currently contained in the store.
     *
     * @return Node IDs currently contained in the store.
     * @throws GridSpiException if an error occurs.
     */
    public Collection<UUID> allNodeIds() throws GridSpiException;

    /**
     * Gets metrics expire time in milliseconds.
     *
     * @return Metrics expire time.
     */
    public int getMetricsExpireTime();
}
