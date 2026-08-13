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

package org.apache.ignite.internal.managers.communication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteMXBeanImpl;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.thread.pool.IgniteThreadPoolExecutor;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/** Tests Communication SPI test messages. */
public class IgniteIoTestMessagesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        startClientGrid(3);
        startClientGrid(4);
    }

    /** */
    @Test
    public void testIoTestMessages() throws Exception {
        byte[] payload = new byte[1024];

        for (Ignite node : G.allGrids()) {
            IgniteKernal ignite = (IgniteKernal)node;
            IoTestHandler ioTest = ignite.context().io().ioTest();
            List<ClusterNode> rmts = new ArrayList<>(ignite.cluster().forRemotes().nodes());

            assertEquals(4, rmts.size());

            assertTimings(ioTest.sendIoTest(ignite.cluster().localNode(), payload, false).get());
            assertTimings(ioTest.sendIoTest(ignite.cluster().localNode(), payload, true).get());

            for (ClusterNode rmt : rmts) {
                assertTimings(ioTest.sendIoTest(rmt, payload, false).get());
                assertTimings(ioTest.sendIoTest(rmt, payload, true).get());
            }

            ioTest.sendIoTest(rmts, payload, false).get();
            ioTest.sendIoTest(rmts, payload, true).get();
        }
    }

    /** Verifies that a successful short run samples every target even when the first request exceeds the deadline. */
    @Test
    public void testRunSamplesEveryTargetDespiteShortDuration() throws Exception {
        IgniteKernal src = (IgniteKernal)grid(0);
        List<ClusterNode> targets = new ArrayList<>(src.cluster().forServers().forRemotes().nodes());
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(src);

        spi.closure((node, msg) -> {
            if (msg instanceof IgniteIoTestMessage)
                LockSupport.parkNanos(MILLISECONDS.toNanos(20));
        });

        try {
            IoTestResult result = src.context().io().ioTest().runIoTest(
                0,
                1,
                1,
                0,
                false,
                targets
            ).get(10, SECONDS);

            assertEquals(targets.size(), result.targets().size());
            assertTrue(result.targets().stream().allMatch(target -> target.samples() > 0));
        }
        finally {
            spi.closure(null);
        }
    }

    /** Verifies that a failed child cancels the other pending requests owned by the aggregate. */
    @Test
    public void testAggregateFailureRemovesOwnedRequests() throws Exception {
        IgniteKernal src = (IgniteKernal)grid(0);
        IgniteKernal target = (IgniteKernal)grid(1);
        ClusterNode departedNode = startGrid(5).localNode();

        stopGrid(5);

        assertTrue("Temporary node did not leave the source topology",
            GridTestUtils.waitForCondition(() -> src.cluster().node(departedNode.id()) == null, 10_000));

        IgniteThreadPoolExecutor sysPool = target.context().pools().getSystemExecutorService();
        CountDownLatch blockersStarted = new CountDownLatch(target.configuration().getSystemThreadPoolSize());
        CountDownLatch releaseBlockers = new CountDownLatch(1);

        try {
            for (int i = 0; i < target.configuration().getSystemThreadPoolSize(); i++) {
                sysPool.execute(() -> {
                    blockersStarted.countDown();

                    try {
                        releaseBlockers.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            assertTrue("Failed to occupy the target system pool", blockersStarted.await(10, SECONDS));

            IoTestHandler ioTest = src.context().io().ioTest();
            IgniteInternalFuture<Void> aggregate = ioTest.sendIoTest(
                List.of(target.localNode(), departedNode),
                new byte[32],
                false
            );
            Map<?, ?> pending = GridTestUtils.getFieldValue(ioTest, "ioTests");

            GridTestUtils.assertThrowsWithCause(() -> {
                aggregate.get();

                return null;
            }, IgniteCheckedException.class);

            assertTrue("Failed aggregate did not remove all per-node requests",
                GridTestUtils.waitForCondition(pending::isEmpty, 10_000));
        }
        finally {
            releaseBlockers.countDown();
        }
    }

    /** Verifies that a departed target fails the whole test. */
    @Test
    public void testRunFailsOnDepartedTarget() throws Exception {
        IgniteKernal src = (IgniteKernal)grid(0);
        ClusterNode healthyNode = grid(1).localNode();
        ClusterNode departedNode = startGrid(5).localNode();

        stopGrid(5);

        assertTrue("Temporary node did not leave the source topology",
            GridTestUtils.waitForCondition(() -> src.cluster().node(departedNode.id()) == null, 10_000));

        GridTestUtils.assertThrowsWithCause(() -> src.context().io().ioTest().runIoTest(
            0,
            500,
            1,
            0,
            false,
            List.of(departedNode, healthyNode)
        ).get(10, SECONDS), IgniteCheckedException.class);
    }

    /** Verifies that a transport SPI failure fails the whole test. */
    @Test
    public void testRunFailsOnSpiError() throws Exception {
        IgniteKernal src = (IgniteKernal)grid(0);
        ClusterNode failedNode = grid(1).localNode();
        ClusterNode healthyNode = grid(2).localNode();
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(src);

        spi.closure((node, msg) -> {
            if (node.id().equals(failedNode.id()) && msg instanceof IgniteIoTestMessage)
                throw new IgniteSpiException("Expected test failure.");
        });

        try {
            GridTestUtils.assertThrowsWithCause(() -> src.context().io().ioTest().runIoTest(
                0,
                500,
                1,
                0,
                false,
                List.of(failedNode, healthyNode)
            ).get(10, SECONDS), IgniteCheckedException.class);
        }
        finally {
            spi.closure(null);
        }
    }

    /** Verifies that marshal-to-unmarshal delivery time includes system-pool dispatch delay. */
    @Test
    public void testDeliveryDelayIncludesSystemPoolDispatch() throws Exception {
        IgniteKernal src = (IgniteKernal)grid(0);
        IgniteKernal target = (IgniteKernal)grid(1);
        IgniteThreadPoolExecutor sysPool = target.context().pools().getSystemExecutorService();
        CountDownLatch blockersStarted = new CountDownLatch(target.configuration().getSystemThreadPoolSize());
        CountDownLatch releaseBlockers = new CountDownLatch(1);
        IgniteInternalFuture<IgniteIoTestMessage> fut = null;

        try {
            for (int i = 0; i < target.configuration().getSystemThreadPoolSize(); i++) {
                sysPool.execute(() -> {
                    blockersStarted.countDown();

                    try {
                        releaseBlockers.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            assertTrue("Failed to occupy the target system pool", blockersStarted.await(10, SECONDS));

            fut = src.context().io().ioTest().sendIoTest(target.localNode(), new byte[32], false);
            long testId = GridTestUtils.getFieldValue(fut, "id");

            assertTrue("IO test message was not queued in the target system pool",
                GridTestUtils.waitForCondition(() -> sysPool.getQueue().stream()
                    .map(Object::toString)
                    .anyMatch(task -> task.contains(IgniteIoTestMessage.class.getSimpleName()) &&
                        task.contains("id=" + testId)), 10_000));
            assertFalse(fut.isDone());

            Thread.sleep(300);
        }
        finally {
            releaseBlockers.countDown();
        }

        assertNotNull(fut);

        IgniteIoTestMessage res = fut.get(10, SECONDS);

        assertTrue("Dispatch delay was not included: " + res.requestDeliveryTimeMillis(),
            res.requestDeliveryTimeMillis() >= 200);
    }

    /** Verifies that cancelling an aggregate request also removes all owned per-node futures. */
    @Test
    public void testAggregateCancellationRemovesOwnedRequests() throws Exception {
        IgniteKernal src = (IgniteKernal)grid(0);
        IgniteKernal target = (IgniteKernal)grid(1);
        IgniteThreadPoolExecutor sysPool = target.context().pools().getSystemExecutorService();
        CountDownLatch blockersStarted = new CountDownLatch(target.configuration().getSystemThreadPoolSize());
        CountDownLatch releaseBlockers = new CountDownLatch(1);

        try {
            for (int i = 0; i < target.configuration().getSystemThreadPoolSize(); i++) {
                sysPool.execute(() -> {
                    blockersStarted.countDown();

                    try {
                        releaseBlockers.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            assertTrue("Failed to occupy the target system pool", blockersStarted.await(10, SECONDS));

            IoTestHandler ioTest = src.context().io().ioTest();
            IgniteInternalFuture<Void> aggregate = ioTest.sendIoTest(List.of(target.localNode()), new byte[32], false);
            Map<?, ?> pending = GridTestUtils.getFieldValue(ioTest, "ioTests");

            assertTrue("Per-node request was not registered",
                GridTestUtils.waitForCondition(() -> !pending.isEmpty(), 10_000));
            assertTrue(aggregate.cancel());
            assertTrue("Cancelled per-node request was not removed",
                GridTestUtils.waitForCondition(pending::isEmpty, 10_000));
        }
        finally {
            releaseBlockers.countDown();
        }
    }

    /** Verifies parameter upper limits and ensures rejected calls do not block subsequent runs. */
    @SuppressWarnings("deprecation")
    @Test
    public void testIoTestParameterUpperLimits() throws Exception {
        IgniteKernal src = (IgniteKernal)grid(0);
        IoTestHandler ioTest = src.context().io().ioTest();
        List<ClusterNode> targets = new ArrayList<>(src.cluster().forServers().forRemotes().nodes());
        IgniteMXBeanImpl mxBean = new IgniteMXBeanImpl(src);

        List<Runnable> invalidRuns = List.of(
            () -> mxBean.runIoTest(
                0, 100, 65, MILLISECONDS.toNanos(100), 5, 0, false),
            () -> ioTest.runIoTest(
                3_600_001, 100, 1, 0, false, targets),
            () -> ioTest.runIoTest(
                0, 3_600_001, 1, 0, false, targets),
            () -> ioTest.runIoTest(
                0, 100, 1, 1024 * 1024 + 1, false, targets)
        );

        for (Runnable invalidRun : invalidRuns)
            GridTestUtils.assertThrowsWithCause(invalidRun, IllegalArgumentException.class);

        IoTestResult result = ioTest.runIoTest(
            0,
            100,
            8,
            0,
            false,
            targets
        ).get();

        assertEquals(8, result.threads());
        assertTrue(result.targets().stream().allMatch(target -> target.samples() > 0));
    }

    /** Verifies RTT and timestamps used for one-way delivery measurements. */
    private void assertTimings(IgniteIoTestMessage res) {
        assertTrue(res.roundTripNanos() > 0);
        assertTrue(res.reqSndTsMillis > 0);
        assertTrue(res.reqRcvTsMillis > 0);
        assertTrue(res.resSndTsMillis > 0);
        assertTrue(res.resRcvTsMillis > 0);
    }
}
