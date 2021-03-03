/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.junit.jupiter.api.Test;

public class DataStructures {

    @Test
    void queue() {
        //tag::queue[]
        Ignite ignite = Ignition.start();

        IgniteQueue<String> queue = ignite.queue("queueName", // Queue name.
                0, // Queue capacity. 0 for an unbounded queue.
                new CollectionConfiguration() // Collection configuration.
        );

        //end::queue[]
        ignite.close();
    }

    @Test
    void set() {

        //tag::set[]
        Ignite ignite = Ignition.start();

        IgniteSet<String> set = ignite.set("setName", // Set name.
                new CollectionConfiguration() // Collection configuration.
        );
        //end::set[]
        ignite.close();
    }

    void colocatedQueue() {
        //tag::colocated-queue[]
        Ignite ignite = Ignition.start();

        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCollocated(true);

        // Create a colocated queue.
        IgniteQueue<String> queue = ignite.queue("queueName", 0, colCfg);
        //end::colocated-queue[]
        ignite.close();
    }

    @Test
    void colocatedSet() {
        //tag::colocated-set[]
        Ignite ignite = Ignition.start();

        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCollocated(true);

        // Create a colocated set.
        IgniteSet<String> set = ignite.set("setName", colCfg);
        //end::colocated-set[] 
        ignite.close();
    }

    @Test
    void atomicLong() {
        //tag::atomic-long[]

        Ignite ignite = Ignition.start();

        IgniteAtomicLong atomicLong = ignite.atomicLong("atomicName", // Atomic long name.
                0, // Initial value.
                true // Create if it does not exist.
        );

        // Increment atomic long on local node
        System.out.println("Incremented value: " + atomicLong.incrementAndGet());
        //end::atomic-long[]
        ignite.close();
    }

    @Test
    void atomicReference() {
        //tag::atomic-reference[]
        Ignite ignite = Ignition.start();

        // Create an AtomicReference
        IgniteAtomicReference<String> ref = ignite.atomicReference("refName", // Reference name.
                "someVal", // Initial value for atomic reference.
                true // Create if it does not exist.
        );

        // Compare and update the value
        ref.compareAndSet("WRONG EXPECTED VALUE", "someNewVal"); // Won't change.
        //end::atomic-reference[] 
        ignite.close();
    }

    @Test
    void countDownLatch() {
        //tag::count-down-latch[]
        Ignite ignite = Ignition.start();

        IgniteCountDownLatch latch = ignite.countDownLatch("latchName", // Latch name.
                10, // Initial count.
                false, // Auto remove, when counter has reached zero.
                true // Create if it does not exist.
        );
        //end::count-down-latch[]
        ignite.close();
    }

    @Test
    void syncOnLatch() {
        //tag::sync-on-latch[]

        Ignite ignite = Ignition.start();

        final IgniteCountDownLatch latch = ignite.countDownLatch("latchName", 10, false, true);

        // Execute jobs.
        for (int i = 0; i < 10; i++)
            // Execute a job on some remote cluster node.
            ignite.compute().run(() -> {
                int newCnt = latch.countDown();

                System.out.println("Counted down: newCnt=" + newCnt);
            });

        // Wait for all jobs to complete.
        latch.await();
        //end::sync-on-latch[]
        ignite.close();
    }

    @Test
    void atomicSequence() {
        //tag::atomic-sequence[]
        Ignite ignite = Ignition.start();

        //create an atomic sequence
        IgniteAtomicSequence seq = ignite.atomicSequence("seqName", // Sequence name.
                0, // Initial value for sequence.
                true // Create if it does not exist.
        );

        // Increment the atomic sequence.
        for (int i = 0; i < 20; i++) {
            long currentValue = seq.get();
            long newValue = seq.incrementAndGet();
        }
        //end::atomic-sequence[]
        ignite.close();
    }

    @Test
    void semaphore() {
        //tag::semaphore[]
        Ignite ignite = Ignition.start();

        IgniteSemaphore semaphore = ignite.semaphore("semName", // Distributed semaphore name.
                20, // Number of permits.
                true, // Release acquired permits if node, that owned them, left topology.
                true // Create if it doesn't exist.
        );
        //end::semaphore[] 
        ignite.close();
    }

    void useSemaphorr() {
        //tag::use-semaphore[]

        Ignite ignite = Ignition.start();

        IgniteSemaphore semaphore = ignite.semaphore("semName", // Distributed semaphore name.
                20, // Number of permits.
                true, // Release acquired permits if node, that owned them, left topology.
                true // Create if it doesn't exist.
        );

        // Acquires a permit, blocking until it's available.
        semaphore.acquire();

        try {
            // Semaphore permit is acquired. Execute a distributed task.
            ignite.compute().run(() -> {
                System.out.println("Executed on:" + ignite.cluster().localNode().id());

                // Additional logic.
            });
        } finally {
            // Releases a permit, returning it to the semaphore.
            semaphore.release();
        }

        //end::use-semaphore[]
    }
}
