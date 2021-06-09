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
package org.apache.ignite.raft.jraft.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.raft.jraft.util.NamedThreadFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MpscSingleThreadExecutorTest {
    private static final Logger LOG = LoggerFactory.getLogger(MpscSingleThreadExecutorTest.class);

    private static final ThreadFactory THREAD_FACTORY = new NamedThreadFactory("test", true);

    @Test
    public void testExecutorIsShutdownWithoutTask() {
        final MpscSingleThreadExecutor executor = new MpscSingleThreadExecutor(1024, THREAD_FACTORY);

        Assert.assertTrue(executor.shutdownGracefully());
        executeShouldFail(executor);
        executeShouldFail(executor);
        Assert.assertTrue(executor.isTerminated());
    }

    @Test
    public void testExecutorIsShutdownWithTask() throws InterruptedException {
        final MpscSingleThreadExecutor executor = new MpscSingleThreadExecutor(1024, THREAD_FACTORY);
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicLong ret = new AtomicLong(0);
        for (int i = 0; i < 10; i++) {
            executor.execute(() -> {
                try {
                    Thread.sleep(100);
                    ret.incrementAndGet();
                    latch.countDown();
                }
                catch (final InterruptedException e) {
                    LOG.info("Thread was interrupted", e);
                }
            });
        }
        Assert.assertTrue(executor.shutdownGracefully());
        executeShouldFail(executor);
        executeShouldFail(executor);
        Assert.assertTrue(executor.isTerminated());
        latch.await();
        Assert.assertEquals(10, ret.get());
    }

    @Test
    public void testExecutorShutdownHooksWithoutTask() {
        final MpscSingleThreadExecutor executor = new MpscSingleThreadExecutor(1024, THREAD_FACTORY);
        final AtomicBoolean hookCalled = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        executor.addShutdownHook(() -> {
            hookCalled.set(true);
            latch.countDown();
        });
        Assert.assertTrue(executor.shutdownGracefully());
        executeShouldFail(executor);
        executeShouldFail(executor);
        Assert.assertTrue(executor.isTerminated());
        Assert.assertTrue(hookCalled.get());
    }

    @Test
    public void testExecutorShutdownHooksWithTask() {
        final MpscSingleThreadExecutor executor = new MpscSingleThreadExecutor(1024, THREAD_FACTORY);
        final AtomicBoolean hookCalled = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(11);
        executor.addShutdownHook(() -> {
            hookCalled.set(true);
            latch.countDown();
        });
        final AtomicLong ret = new AtomicLong(0);
        for (int i = 0; i < 10; i++) {
            executor.execute(() -> {
                try {
                    Thread.sleep(100);
                    ret.incrementAndGet();
                    latch.countDown();
                }
                catch (final InterruptedException e) {
                    LOG.info("Thread was interrupted", e);
                }
            });
        }
        Assert.assertTrue(executor.shutdownGracefully());
        executeShouldFail(executor);
        executeShouldFail(executor);
        Assert.assertTrue(executor.isTerminated());
        Assert.assertTrue(hookCalled.get());
    }

    @Test
    public void testExecutorRejected() throws InterruptedException {
        // 2048 is the minimum of maxPendingTasks
        final int minMaxPendingTasks = 2048;
        final MpscSingleThreadExecutor executor = new MpscSingleThreadExecutor(minMaxPendingTasks, THREAD_FACTORY);
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);

        // add a block task
        executor.execute(() -> {
            try {
                latch1.await();
            }
            catch (final InterruptedException e) {
                LOG.info("Thread was interrupted", e);
            }
            latch2.countDown();
        });

        // wait until the work is blocked
        Thread.sleep(1000);

        // fill the task queue
        for (int i = 0; i < minMaxPendingTasks; i++) {
            executor.execute(() -> {
            });
        }

        executeShouldFail(executor);
        executeShouldFail(executor);
        executeShouldFail(executor);

        latch1.countDown();
        latch2.await();
        executor.shutdownGracefully();
    }

    private static void executeShouldFail(final Executor executor) {
        try {
            executor.execute(() -> {
                // Noop.
            });
            Assert.fail();
        }
        catch (final RejectedExecutionException expected) {
            // expected
        }
    }
}
