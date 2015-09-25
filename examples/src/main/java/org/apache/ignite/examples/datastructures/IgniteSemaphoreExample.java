package org.apache.ignite.examples.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.examples.ExampleNodeStartup;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;

/**
 * This example demonstrates cache based semaphore.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 *
 * @author Vladisav Jelisavcic
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
            IgniteSemaphore syncSemaphore = ignite.semaphore(syncName,0,false,true);

            // Make name of semaphore.
            final String semaphoreName = UUID.randomUUID().toString();

            // Make name of mutex
            final String mutexName = UUID.randomUUID().toString();

            // Make shared resource
            final String resourceName = UUID.randomUUID().toString();
            IgniteCache<String, Queue<String>> cache = ignite.getOrCreateCache(CACHE_NAME);
            cache.put(resourceName, new LinkedList<>());

            // Initialize semaphore.
            IgniteSemaphore semaphore = ignite.semaphore(semaphoreName, 0, false, true);

            // Initialize mutex.
            IgniteSemaphore mutex = ignite.semaphore(mutexName,1,false,true);

            // Start consumers on all cluster nodes.
            for (int i = 0; i < NUM_CONSUMERS; i++)
                ignite.compute().withAsync().run(new Consumer(mutexName, semaphoreName, resourceName));

            // Start producers on all cluster nodes.
            for(int i = 0; i < NUM_PRODUCERS; i++)
                ignite.compute().withAsync().run(new Producer(mutexName, semaphoreName, resourceName));

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

        /** Mutex name. */
        protected final String mutexName;

        /** Resource name. */
        protected final String resourceName;

        /**
         * @param mutexName Mutex name.
         * @param semaphoreName Semaphore name.
         * @param resourceName Resource name.
         */
        SemaphoreExampleClosure(String mutexName, String semaphoreName, String resourceName) {
            this.semaphoreName = semaphoreName;
            this.mutexName = mutexName;
            this.resourceName = resourceName;
        }
    }

    /**
     * Closure which simply signals the semaphore.
     */
    private static class Producer extends SemaphoreExampleClosure {

        /**
         * @param mutexName Mutex name.
         * @param semaphoreName Semaphore name.
         * @param resourceName Resource name.
         */
        public Producer(String mutexName, String semaphoreName, String resourceName) {
            super(mutexName, semaphoreName, resourceName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteSemaphore semaphore = Ignition.ignite().semaphore(semaphoreName, 0, true, true);
            IgniteSemaphore mutex = Ignition.ignite().semaphore(mutexName,0,true,true);

            for(int i=0;i<ITEM_COUNT;i++) {
                mutex.acquire();

                Queue<String> queue = (Queue<String>) Ignition.ignite().cache(CACHE_NAME).get(resourceName);
                queue.add(Ignition.ignite().cluster().localNode().id().toString());
                Ignition.ignite().cache(CACHE_NAME).put(resourceName, queue);
                System.out.println("Producer [nodeId=" + Ignition.ignite().cluster().localNode().id() + "] produced data. Available: " + semaphore.availablePermits());

                mutex.release();

                semaphore.release();
            }

            System.out.println("Producer [nodeId=" + Ignition.ignite().cluster().localNode().id() + "] finished. ");
            IgniteSemaphore sync =  Ignition.ignite().semaphore(syncName, 0, true, true);
            sync.release();
        }
    }

    /**
     * Closure which simply waits on semaphore.
     */
    private static class Consumer extends SemaphoreExampleClosure {

        /**
         * @param mutexName Mutex name.
         * @param semaphoreName Semaphore name.
         * @param resourceName Resource name.
         */
        public Consumer(String mutexName, String semaphoreName, String resourceName) {
            super(mutexName, semaphoreName, resourceName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteSemaphore semaphore = Ignition.ignite().semaphore(semaphoreName, 0, true, true);
            IgniteSemaphore mutex = Ignition.ignite().semaphore(mutexName,0,true,true);

            for(int i=0;i<ITEM_COUNT;i++) {
                semaphore.acquire();

                mutex.acquire();

                Queue<String> queue = (Queue<String>) Ignition.ignite().cache(CACHE_NAME).get(resourceName);
                String data = queue.remove();
                Ignition.ignite().cache(CACHE_NAME).put(resourceName, queue);
                System.out.println("Consumer [nodeId=" + Ignition.ignite().cluster().localNode().id() + "] consumed data generated by producer [nodeId=" + data + "]");

                mutex.release();
            }

            System.out.println("Consumer [nodeId=" + Ignition.ignite().cluster().localNode().id() + "] finished. ");
            IgniteSemaphore sync =  Ignition.ignite().semaphore(syncName, 3, true, true);
            sync.release();
        }
    }
}

