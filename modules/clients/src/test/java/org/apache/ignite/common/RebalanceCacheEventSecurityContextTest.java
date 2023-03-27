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

package org.apache.ignite.common;

import java.util.Collection;
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/** Tests that cache rebalance events contains security data that belongs to rebalance initiator. */
public class RebalanceCacheEventSecurityContextTest extends AbstractEventSecurityContextTest {
    /** {@inheritDoc} */
    @Override protected int[] eventTypes() {
        return new int[] {EVT_CACHE_REBALANCE_OBJECT_LOADED, EVT_CACHE_REBALANCE_OBJECT_UNLOADED};
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setUserAttributes(singletonMap(IDX_ATTR, getTestIgniteInstanceIndex(igniteInstanceName)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testNodeJoinedCacheRebalanceEvents() throws Exception {
        IgniteEx crd = startGridAllowAll(getTestIgniteInstanceName(0));

        IgniteCache<Object, Object> cache = crd.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new GridCacheModuloAffinityFunction(2, 0)));

        cache.put(0, 0);
        cache.put(1, 1);

        String joiningNodeLogin = getTestIgniteInstanceName(1);

        IgniteEx joiningNode = startGridAllowAll(joiningNodeLogin);

        awaitPartitionMapExchange(true, true, null);

        checkCacheRebalanceEvent(crd, EVT_CACHE_REBALANCE_OBJECT_UNLOADED, 1, joiningNodeLogin);
        checkCacheRebalanceEvent(joiningNode, EVT_CACHE_REBALANCE_OBJECT_LOADED, 1, joiningNodeLogin);
    }

    /** */
    @Test
    public void testNodeLeftCacheRebalanceEvents() throws Exception {
        IgniteEx crd = startGridAllowAll(getTestIgniteInstanceName(0));

        String leavingNodeLogin = getTestIgniteInstanceName(1);

        startGridAllowAll(leavingNodeLogin);

        IgniteEx srv = startGridAllowAll(getTestIgniteInstanceName(2));

        int backups = 1;

        IgniteCache<Object, Object> cache = crd.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new GridCacheModuloAffinityFunction(3, backups))
            .setBackups(backups));

        awaitPartitionMapExchange(true, true, null);

        cache.put(0, 0);
        cache.put(1, 1);
        cache.put(2, 2);

        stopGrid(leavingNodeLogin);

        awaitPartitionMapExchange(true, true, null);

        checkCacheRebalanceEvent(crd, EVT_CACHE_REBALANCE_OBJECT_LOADED, 1, leavingNodeLogin);
        checkCacheRebalanceEvent(srv, EVT_CACHE_REBALANCE_OBJECT_LOADED, 0, leavingNodeLogin);
    }

    /** */
    private void checkCacheRebalanceEvent(IgniteEx node, int evtType, int key, String initiatorLogin) throws Exception {
        Collection<Event> nodeEvts = LISTENED_EVTS.get(node.localNode());

        CacheEvent rebalanceEvt = (CacheEvent)nodeEvts.stream().findFirst().orElseThrow(NoSuchElementException::new);

        assertEquals(evtType, rebalanceEvt.type());
        assertEquals(key, (int)rebalanceEvt.<Integer>key());
        assertEquals(initiatorLogin, node.context().security().authenticatedSubject(rebalanceEvt.subjectId()).login());
    }
}
