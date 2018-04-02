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

package org.apache.ignite.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * {@link StopNodeOrHaltFailureHandler} tests.
 */
public class StopNodeOrHaltFailureHandlerTest extends GridCommonAbstractTest {
    /** Number of grids started for tests. */
    public static final int NODES_CNT = 3;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return igniteInstanceName.endsWith("2") ?
            new StopNodeOrHaltFailureHandler(false, 0) :
            new NoOpFailureHandler();
    }

    /**
     * Tests failed node's JVM is halted after triggering StopNodeOrHaltFailureHandler.
     */
    public void testJvmHalted() throws Exception {
        IgniteEx g = grid(0);
        IgniteEx rmt1 = grid(1);
        IgniteEx rmt2 = grid(2);

        assert isMultiJvmObject(rmt1);
        assert isMultiJvmObject(rmt2);

        assert g.cluster().nodes().size() == NODES_CNT;

        g.compute().broadcast(new CA() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public void apply() {
                ignite.countDownLatch("latch", NODES_CNT, false, true).countDown();

                ((IgniteEx)ignite).context().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, null));
            }
        });

        assertTrue(g.countDownLatch("latch", NODES_CNT, false, true).await(2000));

        Thread.sleep(1000);

        assert ((IgniteProcessProxy)rmt1).getProcess().getProcess().isAlive();
        assert !((IgniteProcessProxy)rmt2).getProcess().getProcess().isAlive();
    }
}
