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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class ThreadIdTest implements ThreadId.OnError {
    private ThreadId id;
    private volatile int errorCode = -1;

    @Override
    public void onError(final ThreadId id, final Object data, final int errorCode) {
        assertSame(id, this.id);
        this.errorCode = errorCode;
        id.unlock();
    }

    @BeforeEach
    public void setup() {
        this.id = new ThreadId(this, this);
    }

    @Test
    public void testLockUnlock() throws Exception {
        assertSame(this, this.id.lock());

        CountDownLatch latch = new CountDownLatch(1);

        var t = new Thread(() -> {
            ThreadIdTest.this.id.lock();

            latch.countDown();
        });

        try {
            t.start();

            assertEquals(1, latch.getCount());

            this.id.unlock();

            TestUtils.waitForCondition(() -> latch.getCount() == 0, 10_000);
        } finally {
            t.join();
        }
    }

    @Test
    public void testSetError() throws Exception {
        this.id.setError(100);
        assertEquals(100, this.errorCode);
        this.id.lock();
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            ThreadIdTest.this.id.setError(99);
            latch.countDown();
        });
        try {
            t.start();
            latch.await();
            //just go into pending errors.
            assertEquals(100, this.errorCode);
            //invoke onError when unlock
            this.id.unlock();
            assertEquals(99, this.errorCode);
        } finally {
            t.join();
        }
    }

    @Test
    public void testUnlockAndDestroy() throws Exception {
        AtomicInteger lockSuccess = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(10);
        this.id.lock();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executor.execute(() -> {
                if (ThreadIdTest.this.id.lock() != null) {
                    lockSuccess.incrementAndGet();
                }
                latch.countDown();
            });
        }
        this.id.unlockAndDestroy();
        latch.await();
        assertEquals(0, lockSuccess.get());
        assertNull(this.id.lock());
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }
}
