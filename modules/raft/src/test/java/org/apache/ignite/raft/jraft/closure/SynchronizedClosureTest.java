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
package org.apache.ignite.raft.jraft.closure;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.jraft.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SynchronizedClosureTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(SynchronizedClosureTest.class);

    private SynchronizedClosure done;

    @BeforeEach
    public void setup() {
        this.done = new SynchronizedClosure(1);
    }

    @Test
    public void testAwaitRun() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong cost = new AtomicLong(0);
        Thread t = new Thread(() -> {
            try {
                long start = System.currentTimeMillis();
                done.await();
                cost.set(System.currentTimeMillis() - start);
            }
            catch (InterruptedException e) {
                LOG.error("Thread was interrupted", e);
            }
            latch.countDown();
        });
        try {
            t.start();

            int n = 1000;
            Thread.sleep(n);
            this.done.run(Status.OK());
            latch.await();
            assertEquals(n, cost.get(), 50);
            assertTrue(this.done.getStatus().isOk());
        } finally {
            t.join();
        }
    }

    @Test
    public void testReset() throws Exception {
        testAwaitRun();
        this.done.await();
        assertTrue(true);
        this.done.reset();
        assertNull(this.done.getStatus());
        testAwaitRun();
        assertTrue(this.done.getStatus().isOk());
    }
}
