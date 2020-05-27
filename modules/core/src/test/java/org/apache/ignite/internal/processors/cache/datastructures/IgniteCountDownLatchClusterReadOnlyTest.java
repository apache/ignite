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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.function.Consumer;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singleton;

/**
 * Tests methods of {@link IgniteCountDownLatch} behaviour if cluster in a {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class IgniteCountDownLatchClusterReadOnlyTest extends GridCommonAbstractTest {
    /** Latch name. */
    private static final String LATCH_NAME = "test-latch";

    /** Latch initial value. */
    private static final int LATCH_INITIAL_VALUE = 10;

    /** Latch. */
    private static IgniteCountDownLatch latch;

    /** Latch. */
    private static IgniteCountDownLatch latchForClose;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteEx crd = startGrids(2);

        crd.cluster().state(ClusterState.ACTIVE);

        latch = crd.countDownLatch(LATCH_NAME, LATCH_INITIAL_VALUE, false, true);
        latchForClose = crd.countDownLatch(LATCH_NAME + "2", 1, false, true);

        latchForClose.countDown();

        crd.cluster().state(ClusterState.ACTIVE_READ_ONLY);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        assertEquals(latch.name(), LATCH_INITIAL_VALUE, latch.count());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assertEquals(latch.name(), LATCH_INITIAL_VALUE, latch.count());

        super.afterTest();
    }

    /** */
    @Test
    public void testCountAllowed() {
        assertEquals(LATCH_INITIAL_VALUE, latch.count());
    }

    /** */
    @Test
    public void testGetInstanceWithoutCreateAllowed() {
        assertNotNull(grid(0).countDownLatch(LATCH_NAME, LATCH_INITIAL_VALUE, false, false));
    }

    /** */
    @Test
    public void testCloseDenied() {
        assertEquals(0, latchForClose.count());

        IgniteDataStructuresTestUtils.performAction(
            log,
            singleton(latchForClose),
            IgniteCountDownLatch::name,
            IgniteCountDownLatch::close
        );
    }

    /** */
    @Test
    public void testCountDown() {
        performAction(IgniteCountDownLatch::countDown);
    }

    /** */
    @Test
    public void testCountDown2() {
        performAction(l -> l.countDown(2));
    }

    /** */
    @Test
    public void testCountDownAll() {
        performAction(IgniteCountDownLatch::countDownAll);
    }

    /** */
    private void performAction(Consumer<IgniteCountDownLatch> action) {
        IgniteDataStructuresTestUtils.performAction(log, singleton(latch), IgniteCountDownLatch::name, action);
    }
}
