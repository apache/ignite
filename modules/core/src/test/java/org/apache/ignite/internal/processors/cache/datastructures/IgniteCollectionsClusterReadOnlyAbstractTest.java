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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.datastructures.IgniteDataStructuresTestUtils.getCollectionConfigurations;

/**
 * Abstract class for test common methods of {@link IgniteQueue} and {@link IgniteSet} behaviour if cluster in a
 * {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public abstract class IgniteCollectionsClusterReadOnlyAbstractTest extends GridCommonAbstractTest {
    /** Node count. */
    private static final int NODE_CNT = 2;

    /** Element in a queue. */
    private static final int ELEM = 1;

    /** Collection size. */
    private static final int COLLECTION_SIZE = 2;

    /** Element not in a queue. */
    static final int UNKNOWN_ELEM = 777;

    /** Collection of Ignite collections (IgniteQueue or IgniteSet). */
    static Collection<? extends Collection> igniteCollections;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        startGrids(NODE_CNT).cluster().state(ClusterState.ACTIVE);

        igniteCollections = createAndFillCollections();

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        igniteCollections = null;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        commonChecks();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        commonChecks();

        super.afterTest();
    }

    /**
     * @param col Given collection.
     * @return Name of collection.
     */
    abstract String name(Collection col);

    /**
     * Creates distributed collection with given {@code name} and {@code cfg} configuration.
     *
     * @param name Collection's name.
     * @param cfg Collection's config.
     * @return Distributed collection.
     */
    abstract Collection createCollection(String name, CollectionConfiguration cfg);

    /** */
    @Test
    public void testClearDenied() {
        performAction(Collection::clear);
    }

    /** */
    @Test
    public void testRemoveDenied() {
        performAction(c -> c.remove(ELEM));
    }

    /** */
    @Test
    public void testRemoveAllDenied() {
        performAction(c -> c.removeAll(Arrays.asList(ELEM, name(c))));

        igniteCollections.forEach(c -> assertFalse(name(c), c.contains(UNKNOWN_ELEM)));

        performAction(c -> c.removeAll(Arrays.asList(UNKNOWN_ELEM, name(c))));
    }

    /** */
    @Test
    public void testRetainAllDenied() {
        igniteCollections.forEach(c -> assertFalse(name(c), c.contains(UNKNOWN_ELEM)));

        performAction(c -> c.retainAll(Arrays.asList(UNKNOWN_ELEM, name(c))));

        // All elements in a collection is a collection in retainAll. So nothing to remove.
        igniteCollections.forEach(c -> c.retainAll(Arrays.asList(ELEM, name(c))));
    }

    /** */
    @Test
    public void testSizeAllowed() {
        igniteCollections.forEach(c -> assertEquals(name(c), COLLECTION_SIZE, c.size()));
    }

    /** */
    @Test
    public void testIsEmptyAllowed() {
        igniteCollections.forEach(c -> assertFalse(name(c), c.isEmpty()));
    }

    /** */
    @Test
    public void testAddDenied() {
        performAction(queue -> queue.add("tmp"));
    }

    /** */
    @Test
    public void testAddAllDenied() {
        performAction(queue -> queue.addAll(Arrays.asList("tmp", "tmp2")));
    }

    /** */
    @Test
    public void testContainsAllowed() {
        for (Collection col : igniteCollections) {
            assertTrue(name(col), col.contains(ELEM));
            assertFalse(name(col), col.contains(UNKNOWN_ELEM));
        }
    }

    /** */
    @Test
    public void testContainsAllAllowed() {
        for (Collection col : igniteCollections) {
            assertTrue(name(col), col.containsAll(Arrays.asList(ELEM, name(col))));
            assertFalse(name(col), col.containsAll(Arrays.asList(UNKNOWN_ELEM, name(col))));
        }
    }

    private void commonChecks() {
        igniteCollections.forEach(c -> assertEquals(name(c), COLLECTION_SIZE, c.size()));
        igniteCollections.forEach(c -> assertTrue(name(c), c.containsAll(Arrays.asList(ELEM, name(c)))));

        G.allGrids().forEach(n -> assertEquals(ClusterState.ACTIVE_READ_ONLY, n.cluster().state()));
    }

    /**
     * Performs given {@code action} for all presented collections and check that {@link IgniteClusterReadOnlyException}
     * had been thrown.
     *
     * @param action Action with a collection.
     * @see IgniteDataStructuresTestUtils#performAction(IgniteLogger, Collection, Function, Consumer)
     */
    void performAction(Consumer<? super Collection> action) {
        IgniteDataStructuresTestUtils.performAction(log, igniteCollections, this::name, action);
    }

    /** */
    private Collection<Collection> createAndFillCollections() {
        List<Collection> collections = new ArrayList<>();

        for (Map.Entry<String, CollectionConfiguration> e : getCollectionConfigurations().entrySet()) {
            Collection col = createCollection(e.getKey(), e.getValue());

            assertTrue(col.add(name(col)));
            assertTrue(col.add(ELEM));

            assertEquals(name(col), COLLECTION_SIZE, col.size());

            collections.add(col);
        }

        return collections;
    }
}
