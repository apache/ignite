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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public abstract class PartitionsEvictManagerAbstractTest extends GridCommonAbstractTest {
    /** Failure. */
    protected AtomicBoolean failure = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);

        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

                // Do not invalidate a node context.
                return false;
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param node Node.
     * @param ms Milliseconds.
     */
    protected void awaitEvictionQueueIsEmpty(IgniteEx node, int ms) throws IgniteInterruptedCheckedException {
        PartitionsEvictManager.BucketQueue evictionQueue = node.context().cache().context().evict().evictionQueue;

        assertTrue(GridTestUtils.waitForCondition(evictionQueue::isEmpty, ms));
    }

    /**
     * @param node Node.
     * @param ms Milliseconds.
     */
    protected void awaitEvictionQueueForFilling(IgniteEx node, int ms) throws IgniteInterruptedCheckedException {
        PartitionsEvictManager.BucketQueue evictionQueue = node.context().cache().context().evict().evictionQueue;

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (Queue queue : evictionQueue.buckets)
                return ((InstrumentedEvictionQueue) queue).itemOffered;

            return false;
        }, ms));
    }

    /**
     * @param node Node.
     * @param interceptor Interceptor that will be invoked after task from eviction has polled.
     */
    protected void instrumentEvictionQueue(
        IgniteEx node,
        IgniteClosure<PartitionsEvictManager.AbstractEvictionTask,
            PartitionsEvictManager.AbstractEvictionTask> interceptor
    ) {
        PartitionsEvictManager.BucketQueue evictionQueue = node.context().cache().context().evict().evictionQueue;
        Queue[] buckets = evictionQueue.buckets;

        for (int i = 0; i < buckets.length; i++)
            buckets[i] = new InstrumentedEvictionQueue(interceptor);
    }

    /**
     *
     */
    protected T2<IgniteEx, CountDownLatch> makeNodeWithEvictLatch() throws Exception {
        IgniteEx node1 = startGrid(0);

        CountDownLatch latch = new CountDownLatch(1);

        instrumentEvictionQueue(node1, task -> {
            U.awaitQuiet(latch);

            return task;
        });

        node1.cluster().active(true);

        return new T2<>(node1, latch);
    }

    /**
     * @param nodeAndEvictLatch Node and evict latch.
     * @param r R.
     */
    protected void doActionDuringEviction(T2<IgniteEx, CountDownLatch> nodeAndEvictLatch, Runnable r) throws Exception {
        startGrid(1);

        awaitEvictionQueueForFilling(nodeAndEvictLatch.get1(), 100_000);

        nodeAndEvictLatch.get2().countDown();

        r.run();

        awaitEvictionQueueIsEmpty(nodeAndEvictLatch.get1(), 100_000);
    }

    /**
     * Queue that executes an interceptor during eviction task poll.
     */
    private static class InstrumentedEvictionQueue extends LinkedBlockingQueue {
        /** Interceptor. */
        private final IgniteClosure<PartitionsEvictManager.AbstractEvictionTask,
            PartitionsEvictManager.AbstractEvictionTask> interceptor;

        /** Empty indicator. */
        private volatile boolean itemOffered;

        /**
         * @param interceptor Interceptor.
         */
        private InstrumentedEvictionQueue(IgniteClosure<PartitionsEvictManager.AbstractEvictionTask,
            PartitionsEvictManager.AbstractEvictionTask> interceptor
        ) {
            this.interceptor = interceptor;
        }

        /** {@inheritDoc} */
        @Override public boolean offer(@NotNull Object o) {
            itemOffered = true;

            return super.offer(o);
        }

        /** {@inheritDoc} */
        @Override public Object poll() {
            Object obj = super.poll();

            if (obj instanceof PartitionsEvictManager.AbstractEvictionTask)
                return interceptor.apply((PartitionsEvictManager.AbstractEvictionTask) obj);

            return obj;
        }
    }
}
