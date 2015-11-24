package org.apache.ignite.examples.java8.messaging;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.examples.ExamplesUtils;

/**
 * Demonstrates simple message exchange between two nodes in a single JVM.
 * <p>
 * No remote nodes are needed to be started.
 */
public class MessagingInSingleJVMExample {

    private static final int MAX_PLAYS = 10;

    public static void main(String[] args) throws Exception {
        try (Ignite ignite1 = Ignition.start("examples/config/example-ignite.xml")) {
            try (Ignite ignite2 = Ignition.start("examples/config/example-ignite2.xml")) {

                if (!ExamplesUtils.checkMinTopologySize(ignite1.cluster(), 2))
                    return;

                if (!ExamplesUtils.checkMinTopologySize(ignite2.cluster(), 2))
                    return;

                System.out.println();
                System.out.println(">>> Messaging ping-pong in a single JVM example started.");

                ClusterGroup young = ignite1.cluster().forYoungest();
                ClusterGroup old = ignite2.cluster().forOldest();

                ignite2.message(old).localListen(null, (nodeId, rcvMsg) -> {
                    System.out.println("Received message [msg=" + rcvMsg + ", sender=" + nodeId + ']');
                    if ("PING".equals(rcvMsg)) {
                        ignite2.message(ignite2.cluster().forNodeId(nodeId)).send(null, "PONG");
                        return true; // Continue listening.
                    }
                    return false; // Unsubscribe.
                });

                final CountDownLatch cnt = new CountDownLatch(MAX_PLAYS);

                ignite1.message(young).localListen(null, (nodeId, rcvMsg) -> {
                    System.out.println("Received message [msg=" + rcvMsg + ", sender=" + nodeId + ']');
                    if (cnt.getCount() == 1) {
                        ignite1.message(ignite1.cluster().forNodeId(nodeId)).send(null, "STOP");
                        cnt.countDown();
                        return false; // Stop listening.
                    }
                    else if ("PONG".equals(rcvMsg))
                        ignite1.message(ignite1.cluster().forNodeId(nodeId)).send(null, "PING");
                    else
                        throw new IgniteException("Received unexpected message: " + rcvMsg);
                    cnt.countDown();
                    return true; // Continue listening.
                });

                // Serve!
                ignite1.message(old).send(null, "PING");

                // Wait til the game is over.
                try {
                    cnt.await();
                }
                catch (InterruptedException e) {
                    System.err.println("Hm... let us finish the game!\n" + e);
                }
            }
        }
    }
}