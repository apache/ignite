/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
@RunWith(JUnit4.class)
public class ContinuousQueryReassignmentTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebug() {
        return true;
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryNotCalledOnReassignment() throws Exception {
        testContinuousQueryNotCalledOnReassignment(false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testLocalContinuousQueryNotCalledOnReassignment() throws Exception {
        testContinuousQueryNotCalledOnReassignment(true);
    }

    /**
     * @param loc If {@code true}, then local continuous query will be tested.
     * @throws Exception If failed.
     */
    private void testContinuousQueryNotCalledOnReassignment(boolean loc) throws Exception {
        Ignite lsnrNode = startGrid(1);
        Ignite victim = startGrid(2);

        awaitPartitionMapExchange();

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>("cache");
        cacheCfg.setBackups(1);
        IgniteCache<Integer, String> cache = lsnrNode.getOrCreateCache(cacheCfg);

        AtomicInteger updCntr = new AtomicInteger();

        listenToUpdates(cache, loc, updCntr, null);

        // Subscribe on all nodes to receive all updates.
        if (loc)
            listenToUpdates(victim.cache("cache"), true, updCntr, null);

        int updates = 1000;

        for (int i = 0; i < updates; i++)
            cache.put(i, Integer.toString(i));

        assertTrue(
            "Failed to wait for continuous query updates. Exp: " + updates + "; actual: " + updCntr.get(),
            waitForCondition(() -> updCntr.get() == updates, 10000));

        victim.close();

        assertFalse("Continuous query is called on reassignment.",
            waitForCondition(() -> updCntr.get() > updates, 2000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryWithRemoteFilterNotCalledOnReassignment() throws Exception {
        Ignite lsnrNode = startGrid(1);
        Ignite victim = startGrid(2);

        awaitPartitionMapExchange();

        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>("cache");
        cacheCfg.setBackups(1);
        IgniteCache<Integer, String> cache = lsnrNode.getOrCreateCache(cacheCfg);

        AtomicInteger updCntr = new AtomicInteger();

        CacheEntryEventSerializableFilter<Integer, String> filter = (e) -> e.getKey() % 2 == 0;

        listenToUpdates(cache, false, updCntr, filter);

        int updates = 1000;

        for (int i = 0; i < updates; i++)
            cache.put(i, Integer.toString(i));

        assertTrue(
            "Failed to wait for continuous query updates. Exp: " + updates + "; actual: " + updCntr.get(),
            waitForCondition(() -> updCntr.get() == updates / 2, 10000));

        victim.close();

        assertFalse("Continuous query is called on reassignment.",
            waitForCondition(() -> updCntr.get() > updates / 2, 2000));
    }

    /**
     * Register a continuous query, that counts updates on the provided cache.
     *
     * @param cache Cache.
     * @param loc If {@code true}, then local continuous query will be registered.
     * @param updCntr Update counter.
     * @param rmtFilter Remote filter.
     */
    private void listenToUpdates(IgniteCache<Integer, String> cache, boolean loc, AtomicInteger updCntr,
        CacheEntryEventSerializableFilter<Integer, String> rmtFilter) {

        ContinuousQuery<Integer, String> cq = new ContinuousQuery<>();
        cq.setLocal(loc);
        cq.setLocalListener((evts) -> {
            for (CacheEntryEvent e : evts)
                updCntr.incrementAndGet();
        });
        if (rmtFilter != null)
            cq.setRemoteFilterFactory(FactoryBuilder.factoryOf(rmtFilter));

        cache.query(cq);
    }
}
