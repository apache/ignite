// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.topology.basic;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.topology.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * This class provides basic implementation for topology SPI. This implementation
 * always returns either all available remote grid nodes, remote and local nodes, or only
 * a local node.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 *      <li>{@link #setLocalNode(boolean)} - whether or not to return local node (default is {@code true}).</li>
 *      <li>{@link #setRemoteNodes(boolean)} - whether or not to return remote nodes (default is {@code true}).</li>
 * </ul>
 *
 * @author @java.author
 * @version @java.version
 */
@GridSpiInfo(
    author = /*@java.spi.author*/"GridGain Systems",
    url = /*@java.spi.url*/"www.gridgain.com",
    email = /*@java.spi.email*/"support@gridgain.com",
    version = /*@java.spi.version*/"x.x")
@GridSpiMultipleInstancesSupport(true)
public class GridBasicTopologySpi extends GridSpiAdapter implements GridTopologySpi, GridBasicTopologySpiMBean {
    /** */
    private boolean isLocNode = true;

    /** */
    private boolean isRmtNodes = true;

    /** */
    @GridLocalNodeIdResource
    private UUID locNodeId;

    /** Injected grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /** {@inheritDoc} */
    @Override public boolean isLocalNode() {
        return isLocNode;
    }

    /**
     * Sets the flag on whether or not return local node.
     *
     * @param isLocNode {@code true} to return local node, {@code false} otherwise.
     */
    @GridSpiConfiguration(optional = true)
    public void setLocalNode(boolean isLocNode) {
        this.isLocNode = isLocNode;
    }

    /** {@inheritDoc} */
    @Override public boolean isRemoteNodes() {
        return isRmtNodes;
    }

    /**
     * Sets the flag on whether or not return available remote nodes.
     *
     * @param isRmtNodes {@code true} to return remote nodes, {@code false} otherwise.
     */
    @GridSpiConfiguration(optional = true)
    public void setRemoteNodes(boolean isRmtNodes) {
        this.isRmtNodes = isRmtNodes;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> getTopology(GridComputeTaskSession ses, Collection<? extends GridNode> grid)
        throws GridSpiException {
        return isLocNode && isRmtNodes ? (Collection<GridNode>)grid :
            isLocNode ? Collections.singletonList(getSpiContext().localNode()) :
            isRmtNodes ? (Collection<GridNode>)F.view(grid, F.remoteNodes(locNodeId)) :
            Collections.<GridNode>emptyList();
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        // Check parameters.
        assertParameter(isLocNode || isRmtNodes, "isLocalNode == true || isRmtNodes == true");

        registerMBean(gridName, this, GridBasicTopologySpiMBean.class);

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("isLocalNode", isLocNode));
            log.debug(configInfo("isRmtNodes", isRmtNodes));
        }

        // Ack ok start.
        if (log.isDebugEnabled()) {
            log.debug(startInfo());
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridBasicTopologySpi.class, this);
    }
}
