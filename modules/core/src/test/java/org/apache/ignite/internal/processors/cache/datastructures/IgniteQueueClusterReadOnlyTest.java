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

import java.util.Collection;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.junit.Test;

/**
 * Tests specific methods of {@link IgniteQueue} behaviour if cluster in a {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class IgniteQueueClusterReadOnlyTest extends IgniteCollectionsClusterReadOnlyAbstractTest {
    /** */
    @Test
    public void testOfferDenied() {
        performAction(c -> cast(c).offer("tmp"));
    }

    /** */
    @Test
    public void testPollDenied() {
        performAction(c -> cast(c).poll());
    }

    /** */
    @Test
    public void testPeekAllowed() {
        igniteCollections.forEach(c -> assertEquals(name(c), name(c), cast(c).peek()));
    }

    /** */
    @Test
    public void testPutDenied() {
        performAction(c -> cast(c).put("tmp"));
    }

    /** */
    @Test
    public void testTakeDenied() {
        performAction(c -> cast(c).take());
    }

    /** */
    @Test
    public void testCloseDenied() {
        performAction(c -> cast(c).close());
    }

    @Override public void testRemoveDenied() {
        super.testRemoveDenied();

        igniteCollections.forEach(c -> assertFalse(name(c), c.contains(UNKNOWN_ELEM)));

        // Unknown element doesn't exist in a collection. So exception won't be thrown.
        igniteCollections.forEach(c -> assertFalse(name(c), c.remove(UNKNOWN_ELEM)));
    }

    /** {@inheritDoc} */
    @Override String name(Collection col) {
        assertTrue(col + "", col instanceof IgniteQueue);

        return ((IgniteQueue)col).name();
    }

    /** {@inheritDoc} */
    @Override Collection createCollection(String name, CollectionConfiguration cfg) {
        return grid(0).queue(name, 0, cfg);
    }

    /** */
    private IgniteQueue cast(Collection col) {
        return (IgniteQueue)col;
    }
}
