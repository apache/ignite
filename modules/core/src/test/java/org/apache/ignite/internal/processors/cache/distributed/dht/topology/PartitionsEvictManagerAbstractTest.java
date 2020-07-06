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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class PartitionsEvictManagerAbstractTest extends GridCommonAbstractTest {
    /** Failure. */
    protected AtomicBoolean failure = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

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
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
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

        assertTrue(GridTestUtils.waitForCondition(() -> !evictionQueue.isEmpty(), ms));
    }

    /**
     * @param node Node.
     * @param latch Latch.
     * @param completeWithError Inner future throws exception.
     */
    protected void subscribeEvictionQueueAtLatch(IgniteEx node, CountDownLatch latch, boolean completeWithError) {
        PartitionsEvictManager.BucketQueue evictionQueue = node.context().cache().context().evict().evictionQueue;
        Queue[] buckets = evictionQueue.buckets;

        for (int i = 0; i < buckets.length; i++)
            buckets[i] = new WaitingQueue(latch, completeWithError);
    }

    /**
     *
     */
    protected T2<IgniteEx, CountDownLatch> makeNodeWithEvictLatch() throws Exception {
        return makeNodeWithEvictLatch(false);
    }

    /**
     *
     */
    protected T2<IgniteEx, CountDownLatch> makeNodeWithEvictLatch(boolean completeWithError) throws Exception {
        IgniteEx node1 = startGrid(0);

        node1.cluster().baselineAutoAdjustEnabled(false);

        CountDownLatch latch = new CountDownLatch(1);

        subscribeEvictionQueueAtLatch(node1, latch, completeWithError);

        node1.cluster().active(true);

        return new T2<>(node1, latch);
    }

    /**
     * @param nodeAndEvictLatch Node and evict latch.
     * @param r R.
     */
    protected void doActionDuringEviction(T2<IgniteEx, CountDownLatch> nodeAndEvictLatch, Runnable r) throws Exception {
        IgniteEx node2 = startGrid(1);

        awaitPartitionMapExchange();

        nodeAndEvictLatch.get1().cluster().setBaselineTopology(node2.cluster().topologyVersion());

        awaitEvictionQueueForFilling(nodeAndEvictLatch.get1(), 100_000);

        nodeAndEvictLatch.get2().countDown();

        r.run();

        awaitEvictionQueueIsEmpty(nodeAndEvictLatch.get1(), 100_000);
    }

    /**
     * Queue witch waits on the poll or breaks a PartitionEvictionTask.
     */
    private class WaitingQueue extends LinkedBlockingQueue {
        /** Latch. */
        private final CountDownLatch latch;

        /** Complete with error. */
        private final boolean completeWithError;

        /**
         * @param latch Latch.
         * @param completeWithError flag.
         */
        public WaitingQueue(CountDownLatch latch, boolean completeWithError) {
            this.latch = latch;
            this.completeWithError = completeWithError;
        }

        /** {@inheritDoc} */
        @Override public Object poll() {
            U.awaitQuiet(latch);

            Object obj = super.poll();

            // This code uses for failure handler testing into PartitionEvictionTask.
            if (obj != null && completeWithError) {
                try {
                    Field field = U.findField(PartitionsEvictManager.PartitionEvictionTask.class, "finishFut");

                    field.setAccessible(true);

                    Field modifiersField = Field.class.getDeclaredField("modifiers");
                    modifiersField.setAccessible(true);
                    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

                    field.set(obj, new GridFutureAdapter<Object>() {
                        @Override protected boolean onDone(
                            @Nullable Object res,
                            @Nullable Throwable err,
                            boolean cancel
                        ) {
                            if (err == null)
                                throw new RuntimeException("TEST");

                            return super.onDone(res, err, cancel);
                        }
                    });
                }
                catch (Exception e) {
                    fail();
                }
            }

            return obj;
        }
    }
}
