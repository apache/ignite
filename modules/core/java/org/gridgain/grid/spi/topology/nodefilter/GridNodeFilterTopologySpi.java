// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.topology.nodefilter;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.topology.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * This class provides implementation for topology SPI based on {@link GridBiPredicate}.
 * The implementation returns nodes that are accepted by {@link GridBiPredicate} provided
 * in configuration. If no predicate filter was provided, all nodes, local and remote,
 * will be included into topology.
 * <p>
 * This topology allows for fine grained node provisioning for grid task execution. Nodes
 * can be filtered based on any parameter available on {@link GridNode}. For example,
 * you can filter nodes based on operating system, number of CPU's, available heap memory,
 * average job execution time, current CPU load, any node attribute and about 50 more metrics
 * available in {@link GridNodeMetrics}. Here some of the methods on {@link GridNode} interface
 * which may be used for filtering:
 * <ul>
 * <li>
 *  {@link GridNode#attributes()} - attributes attached to a grid node. Node
 *  attributes are specified in grid configuration via {@link GridConfiguration#getUserAttributes()}
 *  parameter. Note that all system and environment properties are automatically pre-set as
 *  node attributes for every node.
 * </li>
 * <li>
 *  {@link GridNode#metrics()} - about {@code 50} node metrics parameters that are periodically
 *  updated, such as heap, CPU, job counts, average job execution times, etc...
 * </li>
 * </ul>
 * <h1 class="header">Apache JEXL Predicate Filter</h1>
 * GridGain also comes with {@link GridBiJexlPredicate} implementation which allows you
 * to conveniently filter nodes based on Apache JEXL expression language. Refer to
 * <a href="http://commons.apache.org/jexl/">Apache JEXL</a> documentation for specifics of
 * JEXL expression language. {@link GridBiJexlPredicate} allows for a fairly simple way to
 * provide complex SLA-based task topology specifications. For example, the configuration
 * examples below show how the SPI can be configured with {@link GridBiJexlPredicate} to
 * include all Windows XP nodes with more than one processor or core and that are not loaded
 * over 50%.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 *      <li>
 *          {@link #setFilter(GridBiPredicate)} - Node predicate filter
 *          that should be used for decision to accept node.
 *      </li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * GridNodeFilterTopologySpi needs to be explicitly configured.
 * <pre name="code" class="java">
 * GridNodeFilterTopologySpi topSpi = new GridNodeFilterTopologySpi();
 *
 * GridJexlPredicate2&lt;GridNode, GridComputeTaskSession&gt; filter = new GridJexlPredicate2&lt;GridNode, GridComputeTaskSession&gt;(
 *     "node.metrics().availableProcessors &gt; 1 && " +
 *     "node.metrics().averageCpuLoad &lt; 0.5 && " +
 *     "node.attributes().get('os.name') == 'Windows XP'", "node", "ses");
 *
 * // Add filter.
 * topSpi.setFilter(filter);
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override topology SPI.
 * cfg.setTopologySpi(topSpi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridNodeFilterTopologySpi can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *       ...
 *       &lt;property name="topologySpi"&gt;
 *           &lt;bean class="org.gridgain.grid.spi.topology.nodefilter.GridNodeFilterTopologySpi"&gt;
 *               &lt;property name="filter"&gt;
 *                    &lt;bean class="org.gridgain.grid.lang.GridJexlPredicate2"&gt;
 *                        &lt;constructor-arg&gt;
 *                            &lt;value&gt;
 *                                &lt;![CDATA[elem1.metrics().availableProcessors > 1 &&
 *                                elem1.metrics().averageCpuLoad < 0.5 &&
 *                                elem1.attributes().get('os.name') == 'Windows XP']]&gt;
 *                            &lt;/value&gt;
 *                        &lt;/constructor-arg&gt;
 *                    &lt;/bean&gt;
 *                &lt;/property&gt;
 *           &lt;/bean&gt;
 *       &lt;/property&gt;
 *       ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
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
public class GridNodeFilterTopologySpi extends GridSpiAdapter implements GridTopologySpi,
    GridNodeFilterTopologySpiMBean {
    /** Injected grid logger. */
    @GridLoggerResource private GridLogger log;

    /** Configured predicate node filter. */
    private GridBiPredicate<GridNode, GridComputeTaskSession> filter;

    /** {@inheritDoc} */
    @Override public GridBiPredicate<GridNode, GridComputeTaskSession> getFilter() {
        return filter;
    }

    /**
     * Sets filter for nodes to be included into task topology.
     *
     * @param filter Filter to use.
     * @see GridBiJexlPredicate
     */
    @GridSpiConfiguration(optional = true)
    public void setFilter(GridBiPredicate<GridNode, GridComputeTaskSession> filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        if (filter == null) {
            U.warn(log, "'Filter' configuration parameter is not set for GridNodeFilterTopologySpi " +
                "(all nodes will be accepted).");
        }

        registerMBean(gridName, this, GridNodeFilterTopologySpiMBean.class);

        // Ack parameters.
        if (log.isDebugEnabled()) {
            log.debug(configInfo("filter", filter));

            // Ack ok start.
            log.debug(startInfo());
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled()) {
            log.debug(stopInfo());
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> getTopology(GridComputeTaskSession ses, Collection<? extends GridNode> grid)
        throws GridSpiException {
        Collection<GridNode> top = new ArrayList<>(grid.size());

        for (GridNode node : grid) {
            if (filter == null || filter.apply(node, ses)) {
                top.add(node);

                if (log.isDebugEnabled()) {
                    log.debug("Included node into topology: " + node);
                }
            }
        }

        return top;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNodeFilterTopologySpi.class, this);
    }
}
