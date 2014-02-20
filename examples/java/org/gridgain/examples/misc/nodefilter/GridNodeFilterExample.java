// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.nodefilter;

import org.gridgain.examples.misc.multispi.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.topology.nodefilter.*;

import java.io.*;
import java.util.*;

/**
 * Demonstrates use of node filter API.
 * <p>
 * Two different node instances are started in this example, and only one has
 * user attribute defined. Then we are using node filter API to receive only one node
 * with right user attribute.
 * <p>
 * For example of how to use node filter for filter-based
 * {@link GridNodeFilterTopologySpi}
 * refer to {@link GridMultipleTopologyExample} documentation.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridNodeFilterExample {
    /**
     * Execute {@code Node Filter} example on the grid.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        // Create custom configuration for another grid instance.
        GridConfiguration cfg = new GridConfiguration();

        // Map to store user attributes.
        Map<String, Serializable> attrs = new HashMap<>();

        // Add some attribute to have ability to find this node later.
        attrs.put("custom_attribute", "custom_value");

        // Put created attributes to configuration.
        cfg.setUserAttributes(attrs);

        // Define another grid name to have ability to start two grid instance in one JVM.
        cfg.setGridName("OneMoreGrid");

        // Start first and second grid instances
        try (Grid first = GridGain.start(); Grid second = GridGain.start(cfg)) {
            // Now, when we have two grid instance, we want to find only second one,
            // that has user attribute defined.

            Collection<GridNode> nodes = first.forPredicate(
                new GridJexlPredicate<GridNode>(
                    "node.attributes().get('custom_attribute') == 'custom_value'", "node")).nodes();

            // All available nodes.
            Collection<GridNode> allNodes = first.nodes();

            if (nodes.size() == 1) {
                System.out.println(">>>");
                System.out.println(">>> Finished executing Grid \"Node Filter\" example.");
                System.out.println(">>> Total number of found nodes is " + nodes.size() + " from " + allNodes.size()
                    + " available.");
                System.out.println(">>> We found only one node that has necessary attribute defined.");
                System.out.println(">>>");
            }
            else {
                throw new GridException("Found " + nodes.size() + " nodes by node filter, but only 1 was expected.");
            }
        }
    }
}
