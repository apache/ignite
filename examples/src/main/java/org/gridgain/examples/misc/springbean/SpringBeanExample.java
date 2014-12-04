/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.springbean;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.springframework.context.support.*;

import java.util.concurrent.*;

/**
 * Demonstrates a simple use of GridGain grid configured with Spring.
 * <p>
 * String "Hello World." is printed out by Callable passed into
 * the executor service provided by Grid. This statement could be printed
 * out on any node in the grid.
 * <p>
 * The major point of this example is to show grid injection by Spring
 * framework. Grid bean is described in {@code spring-bean.xml} file and instantiated
 * by Spring context. Once application completed its execution Spring will
 * apply grid bean destructor and stop the grid.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public final class SpringBeanExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println();
        System.out.println(">>> Spring bean example started.");

        // Initialize Spring factory.
        ClassPathXmlApplicationContext ctx =
            new ClassPathXmlApplicationContext("org/gridgain/examples/misc/springbean/spring-bean.xml");

        try {
            // Get grid from Spring (note that local grid node is already started).
            Ignite g = (Ignite)ctx.getBean("mySpringBean");

            // Execute any method on the retrieved grid instance.
            ExecutorService exec = g.executorService();

            Future<String> res = exec.submit(new Callable<String>() {
                @Override public String call() throws Exception {
                    System.out.println("Hello world!");

                    return null;
                }
            });

            // Wait for callable completion.
            res.get();

            System.out.println(">>>");
            System.out.println(">>> Finished executing Grid \"Spring bean\" example.");
            System.out.println(">>> You should see printed out of 'Hello world' on one of the nodes.");
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
            System.out.println(">>>");
        }
        finally {
            // Stop local grid node.
            ctx.destroy();
        }
    }
}
