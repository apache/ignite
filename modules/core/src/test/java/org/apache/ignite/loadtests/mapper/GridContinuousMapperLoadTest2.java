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

package org.apache.ignite.loadtests.mapper;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Continuous mapper load test.
 */
public class GridContinuousMapperLoadTest2 {
    /**
     * Main method.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final AtomicInteger jobIdGen = new AtomicInteger();
        final AtomicInteger sentJobs = new AtomicInteger();

        final LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>(10);

        /** Worker thread. */
        Thread t = new Thread("mapper-worker") {
            @Override public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted())
                        queue.put(jobIdGen.incrementAndGet());
                }
                catch (InterruptedException ignore) {
                    // No-op.
                }
            }
        };

        Ignite g = G.start("examples/config/example-cache.xml");

        try {
            int max = 20000;

            IgniteDataStreamer<Integer, TestObject> ldr = g.dataStreamer("replicated");

            for (int i = 0; i < max; i++)
                ldr.addData(i, new TestObject(i, "Test object: " + i));

            // Wait for loader to complete.
            ldr.close(false);

            X.println("Populated replicated cache.");

            t.start();

            while (sentJobs.get() < max) {
                int[] jobIds = new int[10];

                for (int i = 0; i < jobIds.length; i++)
                    jobIds[i] = queue.take();

                sentJobs.addAndGet(10);

                g.compute().execute(new GridContinuousMapperTask2(), jobIds);
            }
        }
        finally {
            t.interrupt();

            t.join();

            G.stopAll(false);
        }
    }
}