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

package org.apache.ignite.internal.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests {@link DistributedProcess} in case of coordinator node left.
 */
public class DistributedProcessCoordinatorLeftTest extends GridCommonAbstractTest {
    /** Timeout to wait latches. */
    public static final long TIMEOUT = 20_000L;

    /** Nodes count. */
    public static final int NODES_CNT = 3;

    /** Stop node index. */
    public static final int STOP_NODE_IDX = 0;

    /** Latch to send single message on node left. */
    private final CountDownLatch nodeLeftLatch = new CountDownLatch(NODES_CNT - 1);

    /** Latch to await sending single messages to a failed coordinator. */
    private final CountDownLatch msgSendLatch = new CountDownLatch(NODES_CNT - 1);

    /** Failure handler invocation flag. */
    private final AtomicBoolean failure = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalEventListeners(Collections.singletonMap(event -> {
            nodeLeftLatch.countDown();

            try {
                msgSendLatch.await();
            }
            catch (InterruptedException e) {
                fail("Unexpected interrupt.");
            }

            return false;
        }, new int[] {EVT_NODE_LEFT, EVT_NODE_FAILED}));

        cfg.setFailureHandler(new FailureHandler() {
            @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

                return false;
            }
        });

        return cfg;
    }

    /**
     * Tests that coordinator failing during sending single result not cause node failure and the process finishes.
     *
     * <ol>
     *  <li>Start new process of {@link DistributedProcess}.</li>
     *  <li>The coordinator fails.</li>
     *  <li>Nodes try to send a single message to the not-alive coordinator.</li>
     *  <li>{@link DistributedProcess} process a node left event and reinitialize a new coordinator.</li>
     *  <li>Process finishes.</li>
     * </ol>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorFailed() throws Exception {
        startGrids(NODES_CNT);

        CountDownLatch startLatch = new CountDownLatch(NODES_CNT);
        CountDownLatch finishLatch = new CountDownLatch(NODES_CNT - 1);

        HashMap<String, DistributedProcess<Integer, Integer>> processes = new HashMap<>();

        int processRes = 1;

        for (Ignite grid : G.allGrids()) {
            DistributedProcess<Integer, Integer> dp = new DistributedProcess<>(((IgniteEx)grid).context(), TEST_PROCESS,
                req -> {
                    IgniteInternalFuture<Integer> fut = runAsync(() -> {
                        try {
                            nodeLeftLatch.await();
                        }
                        catch (InterruptedException ignored) {
                            fail("Unexpected interrupt.");
                        }

                        return req;
                    });

                    // A single message will be sent before this latch released.
                    // It is guaranteed by the LIFO order of future listeners notifying.
                    if (!grid.name().equals(getTestIgniteInstanceName(STOP_NODE_IDX)))
                        fut.listen(f -> msgSendLatch.countDown());

                    startLatch.countDown();

                    return fut;
                },
                (uuid, res, err) -> {
                    if (res.values().size() == NODES_CNT - 1 && res.values().stream().allMatch(i -> i == processRes))
                        finishLatch.countDown();
                    else
                        fail("Unexpected process result [res=" + res + ", err=" + err + ']');
                }
            );

            processes.put(grid.name(), dp);
        }

        processes.get(grid(STOP_NODE_IDX).name()).start(UUID.randomUUID(), processRes);

        assertTrue(startLatch.await(TIMEOUT, MILLISECONDS));

        stopGrid(STOP_NODE_IDX);

        assertTrue(finishLatch.await(TIMEOUT, MILLISECONDS));

        assertFalse(failure.get());
    }
}
