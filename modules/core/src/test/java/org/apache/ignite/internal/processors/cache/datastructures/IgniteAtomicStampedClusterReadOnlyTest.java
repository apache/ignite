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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructuresTestUtils.getAtomicConfigurations;

/**
 * Tests methods of {@link IgniteAtomicStamped} behaviour if cluster in a {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class IgniteAtomicStampedClusterReadOnlyTest extends GridCommonAbstractTest {
    /** Initial value. */
    private static final int INITIAL_VAL = 1;

    /** Initial stamp. */
    private static final int INITIAL_STAMP = 0;

    /** Ignite atomic longs. */
    private static List<IgniteAtomicStamped> atomicStamps = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        for (Map.Entry<String, AtomicConfiguration> e : getAtomicConfigurations().entrySet())
            atomicStamps.add(grid(0).atomicStamped(e.getKey(), e.getValue(), INITIAL_VAL, INITIAL_STAMP, true));

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        atomicStamps.forEach(l -> assertEquals(l.name(), INITIAL_VAL, l.get().get1()));
        atomicStamps.forEach(l -> assertEquals(l.name(), INITIAL_STAMP, l.get().get2()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        atomicStamps.forEach(l -> assertEquals(l.name(), INITIAL_VAL, l.get().get1()));
        atomicStamps.forEach(l -> assertEquals(l.name(), INITIAL_STAMP, l.get().get2()));

        super.afterTest();
    }

    /** */
    @Test
    public void testGetInstanceWithoutCreateAllowed() {
        for (Map.Entry<String, AtomicConfiguration> e : getAtomicConfigurations().entrySet())
            assertNotNull(grid(0).atomicStamped(e.getKey(), e.getValue(), INITIAL_VAL, INITIAL_STAMP, false));
    }

    /** */
    @Test
    public void testGetAllowed() {
        atomicStamps.forEach(l -> assertEquals(l.name(), INITIAL_VAL, l.get().get1()));
        atomicStamps.forEach(l -> assertEquals(l.name(), INITIAL_STAMP, l.get().get2()));
    }

    /** */
    @Test
    public void testGetStampAllowed() {
        atomicStamps.forEach(l -> assertEquals(l.name(), INITIAL_STAMP, l.stamp()));
    }

    /** */
    @Test
    public void testGetValueAllowed() {
        atomicStamps.forEach(l -> assertEquals(l.name(), INITIAL_VAL, l.value()));
    }

    /** */
    @Test
    public void testCloseDenied() {
        performAction(IgniteAtomicStamped::close);
    }

    /** */
    @Test
    public void testSetDenied() {
        performAction(s -> s.set(INITIAL_VAL + 1, INITIAL_STAMP + 1));
    }

    /** */
    @Test
    public void testCompareAndSetDenied() {
        performAction(s -> s.compareAndSet(INITIAL_VAL, INITIAL_VAL + 1,INITIAL_STAMP, INITIAL_STAMP + 1));
    }

    /** */
    void performAction(Consumer<IgniteAtomicStamped> action) {
        IgniteDataStructuresTestUtils.performAction(log, atomicStamps, IgniteAtomicStamped::name, action);
    }
}
