// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.misc.springbean;

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
 * framework. Grid bean is described in spring-bean.xml file and instantiated
 * by Spring context. Once application completed its execution Spring will
 * apply grid bean destructor and stop the grid.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you should (but don't have to) start remote grid instances.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh}</pre>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 * <p>
 * <h1 class="header">Spring Configuration</h1>
 * This example uses spring-bean.xml file which one can find in examples/config directory.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridSpringBeanExample {
    /**
     * Executes simple {@code HelloWorld} example on the grid (without splitting).
     *
     * @param args Command line arguments, none of them are used.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // Initialize Spring factory.
        ClassPathXmlApplicationContext ctx =
            new ClassPathXmlApplicationContext("org/gridgain/examples/advanced/misc/springbean/spring-bean.xml");

        try {
            // Get grid from Spring (note that local grid node is already started).
            GridProjection g = (GridProjection)ctx.getBean("mySpringBean");

            // Execute any method on the retrieved grid instance.
            ExecutorService exec = g.compute().executorService();

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
