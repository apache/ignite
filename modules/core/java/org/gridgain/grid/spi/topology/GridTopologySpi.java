// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.topology;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.topology.attributes.*;
import org.gridgain.grid.spi.topology.basic.*;
import org.gridgain.grid.spi.topology.nodefilter.*;
import java.util.*;

/**
 * Topology SPI allows developer to have a custom logic deciding what specific set of
 * grid nodes (topology) is available to GridGain in any given point of time. This SPI is
 * called every time before grid task gets mapped ({@link org.gridgain.grid.compute.GridComputeTask#map(List, Object)}).
 * <p>
 * Implementations can employ various strategies, e.g., some may be time based when certain nodes
 * are available only at certain time or dates, or topology can be based on average load of
 * the nodes, or it can be based on specifics of the task obtained from the task session
 * and ability to match them to grid nodes.
 * <p>
 * Note that in simple environments the topology is often the same as entire grid (sometimes
 * minus the local node). More complex topology management is required only when available
 * topology changes per task or per some other condition.
 * <p>
 * GridGain comes with following implementations:
 * <ul>
 *      <li>
 *          {@link  GridBasicTopologySpi} -
 *          based on configuration returns either all,
 *          only local, or only remote nodes. This one is a default implementation.
 *      </li>
 *      <li>
 *          {@link GridAttributesTopologySpi} -
 *          based on attributes set.
 *          Those nodes that have attributes with the same values will be included.
 *      </li>
 *      <li>
 *          {@link GridNodeFilterTopologySpi} -
 *          based on predicate node filter.
 *          Those nodes that pass predicate filter will be included.
 *      </li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link Grid#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridTopologySpi extends GridSpi, GridSpiJsonConfigurable {
    /**
     * This method is called by GridGain right before calling {@link org.gridgain.grid.compute.GridComputeTask#map(List, Object)}
     * to obtain a topology for the task's split.
     *
     * @param ses Current task's session. If implementation does not depend on task's
     *      information it may ignore it.
     * @param grid Full set of all grid nodes.
     * @return Topology to use for execution of the task represented by the
     *      session passed in.
     * @throws GridSpiException Thrown in case if topology cannot be obtained.
     */
    public Collection<? extends GridNode> getTopology(GridComputeTaskSession ses, Collection<? extends GridNode> grid)
        throws GridSpiException;
}
