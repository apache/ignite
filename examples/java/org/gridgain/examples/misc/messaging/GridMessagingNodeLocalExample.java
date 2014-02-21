// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.messaging;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.grid.resources.*;

import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.zip.*;

/**
 * Demonstrates various messaging APIs. This example implements a classic
 * streaming processing problem for continues processing of large data sets
 * on the cloud or grid.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you need to start at least one remote grid instance.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/example-default.xml</pre>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 *
 * @author @java.author
 * @version @java.version
 */
public class GridMessagingNodeLocalExample {
    /** UTF-8 charset. */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * This example demonstrates a bit contrived but useful example of how to
     * combine node locals and closure for powerful distributed processing pattern.
     * <p>
     * This example runs on two or more nodes. Here's what this is going to do:
     * <ol>
     * <li>
     *      Local node will pick up random number of message that it will later send
     *      to the remote node.
     * </li>
     * <li>
     *      Local node will configure the remote node to receive these messages and calculate
     *      their CRC32 value which will be stored in remote's node local storage.
     * </li>
     * <li>
     *      Local node then will send predetermined number of message and remote node
     *      will start receiving and processing them.
     * </li>
     * <li>
     *      Local node will then wait till all messages are eventually received by the
     *      remote node and successfully processed.
     * </li>
     * <li>
     *      Local node will finally retrieve the CRC32 value from the remote node.
     * </li>
     * </ol>
     * This example illustrates classic streaming processing concept and how it can be easily
     * implemented using GridGain.
     *
     * @param args Command line arguments (none required).
     * @throws GridException Thrown in case of any errors.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start(args.length == 0 ? "examples/config/example-default.xml" : args[0])) {
            if (g.nodes().size() < 2) {
                System.err.println("Two or more nodes are needed.");

                return;
            }

            // Pick first remote node.
            GridProjection rmt = g.forNode(g.forRemotes().nodes().iterator().next());

            // Number of messages to process.
            final int MSG_NUM = 1 + new Random().nextInt(100);

            // Configure listener on remote node.
            rmt.compute().run(new GridRunnable() {
                @GridInstanceResource
                private Grid g;

                // Method 'run' will be executed on remote node.
                @Override public void run() {
                    final CountDownLatch latch = new CountDownLatch(1);

                    // Store latch reference in node local storage.
                    g.nodeLocalMap().put("latch", latch);

                    g.message().localListen(null, new GridMessagingListenActor<String>() {
                        private final CRC32 crc32 = new CRC32();

                        private final AtomicInteger cnt = new AtomicInteger();

                        @Override protected void receive(UUID nodeId, String rcvMsg) throws Throwable {
                            System.out.println("Calculating for: " + rcvMsg);

                            crc32.update(rcvMsg.getBytes(UTF_8));

                            if (cnt.incrementAndGet() == MSG_NUM) {
                                stop();

                                // Store final CRC32 value in node local storage.
                                g.nodeLocalMap().put("crc32", crc32.getValue());

                                // Drop the latch.
                                latch.countDown();
                            }
                            else
                                skip();
                        }
                    });
                }
            }).get();

            // Send all messages.
            for (int i = 0; i < MSG_NUM; i++)
                rmt.message().send(null, "Message " + i);

            // Wait for all messages to be successfully processed
            // on the remote node.
            rmt.compute().run(new GridRunnable() {
                @GridInstanceResource
                private Grid g;

                @Override public void run() {
                    CountDownLatch latch = (CountDownLatch)g.nodeLocalMap().get("latch");

                    try {
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }).get();

            // Retrieve and print final CRC32 value from the remote node.
            // For example's sake we do it in a separate call (extra network trip).
            Long crc32 = rmt.compute().call(new Callable<Long>() {
                @GridInstanceResource
                private Grid g;

                @Override public Long call() throws Exception {
                    return (Long)g.nodeLocalMap().get("crc32");
                }
            }).get();

            System.out.println("CRC32: " + crc32);
        }
    }
}
