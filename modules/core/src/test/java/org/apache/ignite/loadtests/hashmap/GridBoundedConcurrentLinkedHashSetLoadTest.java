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

package org.apache.ignite.loadtests.hashmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.loadtests.util.GridCumulativeAverage;
import org.jsr166.LongAdder8;

import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.SINGLE_Q;

/**
 *
 */
public class GridBoundedConcurrentLinkedHashSetLoadTest {
    /** */
    public static final int UPDATE_INTERVAL_SEC = 5;

    /**
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        QueuePolicy qPlc = args.length > 0 ? QueuePolicy.valueOf(args[0]) : SINGLE_Q;
        int threadCnt = args.length > 1 ? Integer.valueOf(args[1]) : Runtime.getRuntime().availableProcessors();

        X.println("Queue policy: " + qPlc);
        X.println("Threads: " + threadCnt);

        ExecutorService pool = Executors.newFixedThreadPool(threadCnt);

        final Collection<IgniteUuid> set =
            new GridBoundedConcurrentLinkedHashSet<>(10240, 32, 0.75f, 128, qPlc);

        X.println("Set: " + set);

        final LongAdder8 execCnt = new LongAdder8();

        final AtomicBoolean finish = new AtomicBoolean();

        // Thread that measures and outputs performance statistics.
        Thread collector = new Thread(new Runnable() {
            @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
            @Override public void run() {
                GridCumulativeAverage avgTasksPerSec = new GridCumulativeAverage();

                try {
                    while (!finish.get()) {
                        Thread.sleep(UPDATE_INTERVAL_SEC * 1000);

                        long curTasksPerSec = execCnt.sumThenReset() / UPDATE_INTERVAL_SEC;

                        X.println(">>> Tasks/s: " + curTasksPerSec);

                        avgTasksPerSec.update(curTasksPerSec);
                    }
                }
                catch (InterruptedException ignored) {
                    X.println(">>> Interrupted.");

                    Thread.currentThread().interrupt();
                }
            }
        });

        collector.start();

        Collection<Callable<Object>> producers = new ArrayList<>(threadCnt);

        for (int i = 0; i < threadCnt; i++)
            producers.add(new Callable<Object>() {
                @SuppressWarnings({"unchecked", "InfiniteLoopStatement"})
                @Override public Object call() throws Exception {
                    UUID id = UUID.randomUUID();

                    try {
                    while (!finish.get()) {
                        set.add(IgniteUuid.fromUuid(id));

                        execCnt.increment();
                    }

                    return null;
                    }
                    catch (Throwable t) {
                        t.printStackTrace();

                        throw new Exception(t);
                    }
                    finally {
                        X.println("Thread finished.");
                    }
                }
            });

        pool.invokeAll(producers);
    }
}