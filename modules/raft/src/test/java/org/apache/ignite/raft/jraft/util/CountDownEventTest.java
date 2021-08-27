/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CountDownEventTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(CountDownEventTest.class);

    private ExecutorService executor;

    @AfterEach
    public void teardown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }

    @Test
    public void testAwait() throws Exception {
        CountDownEvent e = new CountDownEvent();
        e.incrementAndGet();
        e.incrementAndGet();
        AtomicLong cost = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(1);
        executor = Executors.newSingleThreadExecutor();
        Utils.runInThread(executor, new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    e.await();
                    cost.set(System.currentTimeMillis() - start);
                }
                catch (Exception e) {
                    LOG.error("Failed to wait", e);
                }
                latch.countDown();
            }
        });
        Thread.sleep(1000);
        e.countDown();
        Thread.sleep(1000);
        e.countDown();
        latch.await();
        assertEquals(2000, cost.get(), 50);
    }

    @Test
    public void testInterrupt() throws Exception {
        CountDownEvent e = new CountDownEvent();
        e.incrementAndGet();
        e.incrementAndGet();
        Thread thread = Thread.currentThread();
        executor = Executors.newSingleThreadExecutor();
        Utils.runInThread(executor, new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                    thread.interrupt();
                }
                catch (Exception e) {
                    LOG.error("Failed to wait", e);
                }
            }
        });
        assertThrows(InterruptedException.class, () -> e.await());
    }
}
