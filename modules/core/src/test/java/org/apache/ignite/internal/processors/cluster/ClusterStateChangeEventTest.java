/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cluster;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.ClusterStateChangeEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.max;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_STATE_CHANGED;

/**
 * Checks that server and client nodes receives {@link EventType#EVT_CLUSTER_STATE_CHANGED} events.
 */
public class ClusterStateChangeEventTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME))
            .setClusterStateOnStart(INACTIVE)
            .setIncludeEventTypes(EventType.EVTS_ALL);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        startGrids(2);

        startClientGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    @Test
    /** */
    public void test() throws Exception {
        IgniteEx crd = grid(0);

        crd.cluster().state(ACTIVE);

        Map<Integer, Integer> data = IntStream.range(0, 1000).boxed().collect(Collectors.toMap(i -> i, i -> i));

        crd.cache(DEFAULT_CACHE_NAME).putAll(data);

        for (Ignite node : G.allGrids())
            assertEquals(node.name(), ACTIVE, node.cluster().state());

        // ACTIVE -> READ_ONLY
        changeStateAndCheckEvents(ACTIVE_READ_ONLY);
        // READ_ONLY -> ACTIVE
        changeStateAndCheckEvents(ACTIVE);
        // ACTIVE -> INACTIVE
        changeStateAndCheckEvents(INACTIVE);
        // INACTIVE -> READ_ONLY
        changeStateAndCheckEvents(ACTIVE_READ_ONLY);
        // READ_ONLY -> INACTIVE
        changeStateAndCheckEvents(INACTIVE);
        // INACTIVE -> ACTIVE
        changeStateAndCheckEvents(ACTIVE);
    }

    /** */
    private void changeStateAndCheckEvents(ClusterState state) throws IgniteCheckedException {
        IgniteEx crd = grid(0);

        ClusterState prevState = crd.cluster().state();
        Collection<BaselineNode> blt = crd.cluster().currentBaselineTopology();

        assertNotSame(prevState, state);

        Map<Ignite, IgniteFuture<Event>> evtFuts = new HashMap<>();

        for (Ignite node : G.allGrids()) {
            Event event = max(node.events().localQuery(F.alwaysTrue(), EVT_CLUSTER_STATE_CHANGED), comparingLong(Event::localOrder));

            log.info("Event with highest local id for node: " + node.name() + " is: " + event);

            evtFuts.put(node, waitForLocalEvent(node.events(), e -> e.localOrder() > event.localOrder(), EVT_CLUSTER_STATE_CHANGED));
        }

        crd.cluster().state(state);

        for (Ignite node : evtFuts.keySet()) {
            assertEquals(node.name(), state, node.cluster().state());

            Event e = evtFuts.get(node).get(1000L);

            assertNotNull(node.name(), e);

            assertTrue(node.name() + " " + e, e instanceof ClusterStateChangeEvent);

            ClusterStateChangeEvent changeEvent = (ClusterStateChangeEvent)e;

            assertEquals(prevState, changeEvent.previousState());
            assertEquals(state, changeEvent.state());

            if (blt == null)
                assertNull(node.name(), changeEvent.baselineNodes());
            else {
                assertNotNull(changeEvent.baselineNodes());

                Set<Object> bltIds = blt.stream().map(BaselineNode::consistentId).collect(toSet());
                Set<Object> evtBltIds = changeEvent.baselineNodes().stream().map(BaselineNode::consistentId).collect(toSet());

                assertEqualsCollections(bltIds, evtBltIds);
            }
        }
    }
}
