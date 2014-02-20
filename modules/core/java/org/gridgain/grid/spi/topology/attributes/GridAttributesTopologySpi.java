// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.topology.attributes;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.topology.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.util.*;

/**
 * This class provides attribute based implementation for topology SPI.
 * This implementation always returns all nodes (local and remote) that
 * have attributes provided in configuration with given values. If no
 * attributes were provided, all nodes, local and remote, will be included
 * into topology.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 *      <li>{@link #setAttributes(Map)} - Map of attributes and their values that nodes should have.</li>
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
public class GridAttributesTopologySpi extends GridSpiAdapter implements GridTopologySpi,
    GridAttributesTopologySpiMBean {
    /** Injected grid logger. */
    @GridLoggerResource private GridLogger log;

    /** Named attributes. */
    private Map<String, ?> attrs;

    /** {@inheritDoc} */
    @Override public Map<String, ?> getAttributes() {
        return attrs;
    }

    /**
     * Sets attributes that all nodes should have, to be in topology.
     *
     * @param attrs Map of node attributes.
     */
    @GridSpiConfiguration(optional = true)
    public void setAttributes(Map<String, ?> attrs) {
        this.attrs = attrs;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> getTopology(GridComputeTaskSession ses, Collection<? extends GridNode> grid)
        throws GridSpiException {
        Collection<GridNode> top = new ArrayList<>(grid.size());

        for (GridNode node : grid) {
            Map<String, Object> nodeAttrs = node.attributes();

            if (attrs != null && nodeAttrs != null) {
                if (!U.containsAll(nodeAttrs, attrs))
                    continue;
            }
            else if (nodeAttrs == null && attrs != null)
                continue;

            top.add(node);

            if (log.isDebugEnabled())
                log.debug("Included node into topology: " + node);
        }

        return top;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        registerMBean(gridName, this, GridAttributesTopologySpiMBean.class);

        // Ack parameters.
        if (log.isDebugEnabled())
            log.debug(configInfo("attrs", attrs));

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
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
        return S.toString(GridAttributesTopologySpi.class, this);
    }
}
