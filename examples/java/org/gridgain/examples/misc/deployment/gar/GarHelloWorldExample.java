// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.deployment.gar;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.deployment.uri.*;

/**
 * Demonstrates a simple use of GridGain grid with GAR file.
 * <p>
 * The main purpose of this example is to demonstrate how grid task can
 * be packaged into GAR file, how GAR ant task can be used and how
 * various resources (such as Spring XML and properties files) can be
 * accessed from within GAR deployment.
 * <p>
 * Message property key "HELLOWORLD.MSG" is passed as an argument for execution
 * of {@code GridGarHelloWorldTask} task which is loaded from
 * GAR file. {@code GridGarHelloWorldTask} gets {@code GridGarHelloWorldBean}
 * bean from {@code GAR/lib/depend.jar} via Spring. Message resource {@code example.properties} also
 * loaded from imported {@code GAR/lib/depend.jar}. As an outcome, two participating
 * nodes will print out a single word from "Hello World" string.
 * One node will print out "Hello, " and the other will printout
 * "World!". If there is only one node participating, then it will
 * print out both words.
 * <p>
 * <b>NOTE:</b> Both {@code GridGarHelloWorldTask} and {@code GridGarHelloWorldBean} classes
 * are placed outside of this project for example purpose. They are not part of this
 * Javadoc and assumed to be built into GAR file by running {@code build.xml} Ant script
 * supplied with this example.
 * <p>
 * Grid task {@code GridGarHelloWorldTask} handles actual splitting
 * into sub-jobs, remote execution, and result aggregation
 * (see {@link org.gridgain.grid.compute.GridComputeTask}).
 * <p>
 * Before running example, make the following steps.
 * <ol>
 * <li>
 *      Create GAR file ({@code helloworld.gar}) with Ant script.
 *      Go in folder {@code ${GRIDGAIN_HOME}/examples/gar/build} and run {@code ant}
 *      in command line.
 * </li>
 * <li>
 *      Copy {@code ${GRIDGAIN_HOME}/examples/gar/deploy/helloworld.gar} in folder
 *      {@code ${GRIDGAIN_HOME}/work/deployment/file/}
 * </li>
 * <li>
 *      You should run the following sample with Spring XML configuration file shipped
 *      with GridGain and located {@code ${GRIDGAIN_HOME}/examples/config/example-gar.xml}.
 *      You should pass a path to Spring XML configuration file as 1st command line
 *      argument into this example.
 *      Note, that {@code example-gar.xml} starts GridGain with {@link GridUriDeploymentSpi}
 *      which scans default folder {@code ${GRIDGAIN_HOME}/work/deployment/file/}
 *      for new GAR files. See {@link GridUriDeploymentSpi}.
 * </li>
 * </ol>
 *
 * @author @java.author
 * @version @java.version
 */
public final class GarHelloWorldExample {
    /**
     * Execute {@code HelloWorld} example on the grid.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try {
            try (Grid g = GridGain.start(args.length == 0 ? "examples/config/example-gar.xml" : args[0])) {
                // Execute Hello World task from GAR file.
                g.compute().execute("GridGarHelloWorldTask", "HELLOWORLD.MSG").get();

                System.out.println(">>>");
                System.out.println(">>> Finished executing Grid \"Hello World\" example with custom task.");
                System.out.println(">>> You should see print out of 'Hello' on one node and 'World' on another node.");
                System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
                System.out.println(">>>");
            }
        }
        finally {
            GridGain.stopAll(false);
        }
    }
}
