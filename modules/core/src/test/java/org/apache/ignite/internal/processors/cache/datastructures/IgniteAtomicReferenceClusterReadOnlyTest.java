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
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructuresTestUtils.getAtomicConfigurations;

/**
 * Tests methods of {@link IgniteAtomicReference} behaviour if cluster in a {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class IgniteAtomicReferenceClusterReadOnlyTest extends GridCommonAbstractTest {
    /** Initial value. */
    private static final int INITIAL_VAL = 0;

    /** Ignite atomic longs. */
    private static List<IgniteAtomicReference> atomicRefs = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        for (Map.Entry<String, AtomicConfiguration> e : getAtomicConfigurations().entrySet())
            atomicRefs.add(grid(0).atomicReference(e.getKey(), e.getValue(), INITIAL_VAL, true));

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

        atomicRefs.forEach(l -> assertEquals(l.name(), INITIAL_VAL, l.get()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        atomicRefs.forEach(l -> assertEquals(l.name(), INITIAL_VAL, l.get()));

        super.afterTest();
    }

    /** */
    @Test
    public void testGetInstanceWithoutCreateAllowed() {
        for (Map.Entry<String, AtomicConfiguration> e : getAtomicConfigurations().entrySet())
            assertNotNull(e.getKey(), grid(0).atomicReference(e.getKey(), e.getValue(), INITIAL_VAL, false));
    }

    /** */
    @Test
    public void testGetAllowed() {
        atomicRefs.forEach(l -> assertEquals(l.name(), INITIAL_VAL, l.get()));
    }

    /** */
    @Test
    public void testCloseDenied() {
        performAction(IgniteAtomicReference::close);
    }

    /** */
    @Test
    public void testSetDenied() {
        performAction(r -> r.set(1));
    }

    /** */
    @Test
    public void testCompareAndSetDenied() {
        performAction(r -> r.compareAndSet(INITIAL_VAL, 1));
    }

    /** */
    void performAction(Consumer<IgniteAtomicReference> action) {
        IgniteDataStructuresTestUtils.performAction(log, atomicRefs, IgniteAtomicReference::name, action);
    }
}
