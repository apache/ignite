package org.apache.ignite.examples.datastructures;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * This example demonstrates cache based semaphore. <p> Remote nodes should always be started with special configuration
 * file which enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}. <p> Alternatively
 * you can run {@link ExampleNodeStartup} in another JVM which will start node with {@code
 * examples/config/example-ignite.xml} configuration.
 */
public class IgniteSemaphoreExample {
    /** Cache name. */
    private static final String CACHE_NAME = IgniteSemaphoreExample.class.getSimpleName();

    /** Number of items for each producer/consumer to produce/consume. */
    private static final int ITEM_COUNT = 100;

    /** Number of producers. */
    private static final int NUM_PRODUCERS = 10;

    /** Number of consumers. */
    private static final int NUM_CONSUMERS = 10;

    /** Synchronization semaphore name. */
    private static final String syncName = IgniteSemaphoreExample.class.getSimpleName();

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic semaphore example started.");

            // Initialize semaphore.
            IgniteSemaphore syncSemaphore = ignite.semaphore(syncName, 0, false, true);

            // Make name of semaphore.
            final String semaphoreName = UUID.randomUUID().toString();

            // Initialize semaphore.
            IgniteSemaphore semaphore = ignite.semaphore(semaphoreName, 0, false, true);

            // Start consumers on all cluster nodes.
            for (int i = 0; i < NUM_CONSUMERS; i++)
                ignite.compute().withAsync().run(new Consumer(semaphoreName));

            // Start producers on all cluster nodes.
            for (int i = 0; i < NUM_PRODUCERS; i++)
                ignite.compute().withAsync().run(new Producer(semaphoreName));

            System.out.println("Master node is waiting for all other nodes to finish...");

            // Wait for everyone to finish.
            syncSemaphore.acquire(NUM_CONSUMERS + NUM_PRODUCERS);
        }

        System.out.flush();
        System.out.println();
        System.out.println("Finished semaphore example...");
        System.out.println("Check all nodes for output (this node is also part of the cluster).");
    }

    /**
     * Closure which simply waits on the latch on all nodes.
     */
    private abstract static class SemaphoreExampleClosure implements IgniteRunnable {
        /** Semaphore name. */
        protected final String semaphoreName;

        /**
         * @param semaphoreName Semaphore name.
         */
        SemaphoreExampleClosure(String semaphoreName) {
            this.semaphoreName = semaphoreName;
        }
    }

    /**
     * Closure which simply signals the semaphore.
     */
    private static class Producer extends SemaphoreExampleClosure {

        /**
         * @param semaphoreName Semaphore name.
         */
        public Producer(String semaphoreName) {
            super(semaphoreName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteSemaphore semaphore = Ignition.ignite().semaphore(semaphoreName, 0, true, true);

            for (int i = 0; i < ITEM_COUNT; i++) {
                System.out.println("Producer [nodeId=" + Ignition.ignite().cluster().localNode().id() + "]. Available: " + semaphore.availablePermits());

                // Signals others that shared resource is available.
                semaphore.release();
            }

            System.out.println("Producer [nodeId=" + Ignition.ignite().cluster().localNode().id() + "] finished. ");

            // Gets the syncing semaphore
            IgniteSemaphore sync = Ignition.ignite().semaphore(syncName, 0, true, true);

            // Signals the master thread
            sync.release();
        }
    }

    /**
     * Closure which simply waits on semaphore.
     */
    private static class Consumer extends SemaphoreExampleClosure {

        /**
         * @param semaphoreName Semaphore name.
         */
        public Consumer(String semaphoreName) {
            super(semaphoreName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteSemaphore semaphore = Ignition.ignite().semaphore(semaphoreName, 0, true, true);

            for (int i = 0; i < ITEM_COUNT; i++) {
                // Block if no permits are available.
                semaphore.acquire();

                System.out.println("Consumer [nodeId=" + Ignition.ignite().cluster().localNode().id() + "]. Available: " + semaphore.availablePermits());
            }

            System.out.println("Consumer [nodeId=" + Ignition.ignite().cluster().localNode().id() + "] finished. ");

            // Gets the syncing semaphore
            IgniteSemaphore sync = Ignition.ignite().semaphore(syncName, 3, true, true);

            // Signals the master thread
            sync.release();
        }
    }
}

