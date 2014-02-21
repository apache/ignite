// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.multispi;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.springframework.context.support.*;

/**
 * Demonstrates a simple use of GridGain multiple topology SPI feature.
 * <p>
 * Two tasks {@link GridSegmentATask} and {@link GridSegmentBTask}
 * configured to be executed on certain nodes. {@link GridSegmentATask} should
 * be executed on nodes that have attribute {@code segment} with value {@code A}
 * and {@link GridSegmentBTask} should be executed on nodes that have attribute
 * {@code segment} with value {@code B}.
 * <p>
 * The node attributes are configured in {@code nodeA.xml} and {@code nodeB.xml}
 * files located in the same package. {@code NodeA} is configured with attribute
 * {@code segmentA}, and {@code NodeB} is configured with attribute {@code segmentB}.
 * <p>
 * The {@code master.xml} configuration represents the master node. It has two
 * Topology SPIs configured: one includes only nodes with {@code SegmentA} attribute
 * and the other includes only nodes with {@code SegmentB} attribute. The
 * {@link GridSegmentATask} and {@link GridSegmentBTask} tasks specify which Topology
 * SPI they should use via {@link org.gridgain.grid.compute.GridComputeTaskSpis} annotation. Once you run this example,
 * the master node with provided {@code master.xml} configuration will be started.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you have to start two remote grid instances.
 * One should be started with configuration {@code nodeA.xml} that can be found
 * in the same package where example is. Another node should be started
 * with {@code nodeB.xml} configuration file. Please use {@link GridNodeAStartup}
 * and {@link GridNodeBStartup} classes to start remote nodes.
 * <p>
 * You can also startup remote nodes from shell. Here is an example of
 * starting node with {@code nodeA.xml} configuration file.
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} /path_to_configuration/nodeA.xml</pre>
 * <p>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridMultipleTopologyExample {
    /**
     * Executes {@link GridSegmentATask} and {@link GridSegmentBTask} tasks on the grid.
     *
     * @param args Command line arguments, none required or used.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        AbstractApplicationContext ctx =
            new ClassPathXmlApplicationContext("org/gridgain/examples/misc/multispi/master.xml");

        // Get configuration from Spring.
        GridConfiguration cfg = ctx.getBean("grid.cfg", GridConfiguration.class);

        try (Grid g = GridGain.start(cfg)){
            if (g.nodes().size() == 3) {
                // Execute task on segment "A".
                GridComputeTaskFuture<Integer> futA = g.compute().execute(GridSegmentATask.class, null);

                // Execute task on segment "B".
                GridComputeTaskFuture<Integer> futB = g.compute().execute(GridSegmentBTask.class, null);

                // Wait for task completion.
                futA.get();
                futB.get();

                System.out.println(">>>");
                System.out.println(">>> Finished executing Grid \"Multiple Topology\" example with custom tasks.");
                System.out.println(">>> You should see print out of 'Executing job on node that is from segment A.'" +
                    "on node that has attribute \"segment=A\"");
                System.out.println(">>> and 'Executing job on node that is from segment B.' on node that has " +
                    "attribute \"segment=B\"");
                System.out.println(">>> Check all nodes for output (this node is not a part of the grid).");
                System.out.println(">>>");
            }
            else {
                System.err.println(">>>");
                System.err.println(">>> Incorrect topology. Ensure that you started two remote nodes using GridNodeAStartup " +
                    "and GridNodeBStartup.");
                System.err.println(">>>");
            }
        }
    }
}
