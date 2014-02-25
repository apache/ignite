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
import org.gridgain.grid.resources.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Example that demonstrates how to exchange messages between nodes. Use such
 * functionality for cases when you need to communicate to other nodes outside
 * of grid task. In such cases all your message classes must be in system
 * class path as they will not be peer-loaded.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridMessagingExample {
    /** An ordered message timeout. */
    private static final long MSG_TIMEOUT = 15000;

    /**
     * A custom messaging topic.
     * <p>
     * You can use topics to handle messages
     * in different listeners depending on
     * corresponding business logic.
     * <p>
     * You can use any object for topic,
     * including strings, integers, and enums.
     */
    private enum CustomTopic {
        /** Info message topic. */
        INFO,

        /** Alarm message topic. */
        ALARM,

        /** Topic for local-only messages. */
        LOCAL_ONLY,

        /** Bye messaging topic. */
        BYE,

        /** Bye reply messaging topic. */
        BYE_REPLY
    }

    /**
     * Custom message class.
     */
    private static class ExampleMessage {
        /** Message body. */
        private Object body;

        /**
         * @param body Message body.
         */
        ExampleMessage(Object body) {
            this.body = body;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ExampleMessage [body=" + body + "]";
        }
    }

    /**
     * Shows several use-cases for ordered and unordered messages.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        Grid grid = args.length > 0 ? GridGain.start(args[0]) : GridGain.start("examples/config/example-default.xml");

        try {
            // Run example with ordered messages.
            unorderedMessages(grid);

            // Run example with unordered messages.
            orderedMessages(grid);
        }
        finally {
            GridGain.stop(false);
        }
    }

    /**
     * Sends unordered messages to the nodes in grid and receives it via listener.
     *
     * @param grid Grid for performing example.
     * @throws Exception If example execution failed.
     */
    public static void unorderedMessages(Grid grid) throws Exception {
        System.out.println(">>> Starting Unordered Messages Example.");

        // Register listener for default topic for the whole grid.
        // Only messages that are not sent to a specific topic will
        // be received by this listener.
        grid.message().remoteListen(null, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received message [msg=" + msg + ", originatingNodeId=" + nodeId + ']');

                return !"stop".equals(msg); // Stop listening when we receive "stop" message.
            }
        }).get();

        // Register listener for INFO topic for the whole grid.
        grid.message().remoteListen(CustomTopic.INFO, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received INFO message [msg=" + msg + ", originatingNodeId=" + nodeId + ']');

                return false; // Unregister listener.
            }
        }).get();

        // Register listener for ALARM topic for the whole grid.
        grid.message().remoteListen(CustomTopic.ALARM, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received ALARM message [msg=" + msg + ", originatingNodeId=" + nodeId + ']');

                return false; // Unregister listener.
            }
        }).get();

        // Listen locally for LOCAL_ONLY messages.
        grid.message().localListen(CustomTopic.LOCAL_ONLY, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received LOCAL_ONLY message [msg=" + msg + ", originatingNodeId=" + nodeId +
                    "]. This message should be seen only on node, started within example.");

                return false; // Unregister listener.
            }
        });

        // Send messages to the grid (default topic is used).
        grid.message().send(null, "MSG-1");
        grid.message().send(null, "MSG-2");
        grid.message().send(null, "MSG-3");

        // Send INFO message to the grid.
        grid.message().send(CustomTopic.INFO, "This is an info message.");

        // Send ALARM message to the grid.
        grid.message().send(CustomTopic.ALARM, "Something nasty happened!");

        // Send LOCAL_ONLY message.
        grid.message().send(CustomTopic.LOCAL_ONLY, "Local message.");

        // Allow time for messages to be received.
        Thread.sleep(1000);

        grid.message().send(null, "stop"); // Unregister listener.

        // Allow time for messages to be received.
        Thread.sleep(1000);

        grid.message().send(null, "This message won't be received, because listener is already unregistered.");

        System.out.println(">>>");
        System.out.println(">>> Finished executing Grid Messaging Example.");
        System.out.println(">>> Check local node output.");
        System.out.println(">>>");
    }

    /**
     * Sends ordered messages to the nodes in grid and receives it via listener.
     *
     * @param grid Grid for performing example.
     * @throws Exception If example execution failed.
     */
    public static void orderedMessages(Grid grid) throws Exception {
        System.out.println(">>> Starting Ordered Messaging Example.");

        // Register listener for default topic for the whole grid.
        // Only messages that are not sent to a specific topic will
        // be received by this listener.
        grid.message().remoteListen(null, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received message [msg=" + msg + ", originatingNodeId=" + nodeId + ']');

                return !"stop".equals(msg); // Stop listening when we receive "stop" message.
            }
        }).get();

        // Register listener for INFO topic for the whole grid.
        grid.message().remoteListen(CustomTopic.INFO, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received INFO message [msg=" + msg + ", originatingNodeId=" + nodeId + ']');

                return false; // Unregister listener.
            }
        }).get();

        // Register listener for ALARM topic for the whole grid.
        grid.message().remoteListen(CustomTopic.ALARM, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received ALARM message [msg=" + msg + ", originatingNodeId=" + nodeId + ']');

                return false; // Unregister listener.
            }
        }).get();

        // Register listener for BYE topic for the whole grid.
        grid.message().remoteListen(CustomTopic.BYE, new GridBiPredicate<UUID, Object>() {
            @GridInstanceResource
            private Grid g;

            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received BYE message [msg=" + msg + ", originatingNodeId=" + nodeId +
                    "]. You should see all the previously sent messages in order they were sent. " +
                    "This message goes last.");

                // Reply to sender node.
                try {
                    g.forNodeIds(Collections.singleton(nodeId)).message().send(CustomTopic.BYE_REPLY, "See you!");
                }
                catch (GridException e) {
                    throw new GridRuntimeException(e);
                }

                return false; // Unregister listener.
            }
        }).get();

        // Listen locally for LOCAL_ONLY messages.
        grid.message().localListen(CustomTopic.LOCAL_ONLY, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received LOCAL_ONLY message [msg=" + msg + ", originatingNodeId=" + nodeId +
                    "]. This message should be seen only on node, started within example.");

                return false; // Unregister listener.
            }
        });

        final CountDownLatch finishLatch = new CountDownLatch(grid.nodes().size());

        // Listen locally for BYE_REPLY message. Once all grids have replied, we can finish.
        grid.message().localListen(CustomTopic.BYE_REPLY, new GridBiPredicate<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(">>> Received BYE_REPLY message [msg=" + msg + ", originatingNodeId=" + nodeId +
                    "]. This message should be seen only on local node as many times, as there are nodes in grid " +
                    "(each node replies to BYE from local node).");

                finishLatch.countDown();

                return true; // Always listen.
            }
        });

        // Send messages to the grid (default topic is used).
        grid.message().sendOrdered(null, new ExampleMessage("MSG-1"), MSG_TIMEOUT);
        grid.message().sendOrdered(null, new ExampleMessage("MSG-2"), MSG_TIMEOUT); // Delayed message.
        grid.message().sendOrdered(null, new ExampleMessage("MSG-3"), MSG_TIMEOUT);

        // Send INFO message to the grid.
        grid.message().sendOrdered(CustomTopic.INFO, "This is an info message.", MSG_TIMEOUT);

        // Send ALARM message to the grid.
        grid.message().sendOrdered(CustomTopic.ALARM, "Something nasty happened!", MSG_TIMEOUT);

        // Send LOCAL_ONLY message.
        grid.message().sendOrdered(CustomTopic.LOCAL_ONLY, "Local message.", MSG_TIMEOUT);

        grid.message().sendOrdered(null, "stop", MSG_TIMEOUT); // Unregister listener.

        grid.message().sendOrdered(null, "This message won't be received, because listener is already unregistered.",
            MSG_TIMEOUT);

        // Send BYE message to finish example.
        grid.message().sendOrdered(CustomTopic.BYE, "Bye!", MSG_TIMEOUT);

        finishLatch.await(20, TimeUnit.SECONDS);

        System.out.println(">>>");
        System.out.println(">>> Finished executing Ordered Messaging Example.");
        System.out.println(">>> Check local node output.");
        System.out.println(">>>");
    }
}
