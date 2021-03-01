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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Tests a scenario when a partition is attempted for eviction while being reserved in group.
 */
@WithSystemProperty(key = "IGNITE_PRELOAD_RESEND_TIMEOUT", value = "0") // Force fast partition state message exchange.
public class EvictionWhilePartitionGroupIsReservedTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS = 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(1).setAffinity(new RendezvousAffinityFunction(false, PARTS)).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testGroupReservation() throws Exception {
        testGroupReservation(false, false);
    }

    /** */
    @Test
    public void testGroupReservation_2() throws Exception {
        testGroupReservation(true, false);
    }

    /** */
    @Test
    public void testGroupReservationStartClient() throws Exception {
        testGroupReservation(false, true);
    }

    /** */
    @Test
    public void testGroupReservationStartClient_2() throws Exception {
        testGroupReservation(true, true);
    }

    /**
     * @param clientBefore {@code True} to start a client before acquiring a group reservation.
     * @param clientAfter {@code True} to start a client before releasing a group reservation, otherwise start a server.
     */
    private void testGroupReservation(boolean clientBefore, boolean clientAfter) throws Exception {
        final int cnt = 3;

        IgniteEx crd = startGrids(cnt);

        awaitPartitionMapExchange(true, true, null);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i);

        IgniteEx node = grid(0);

        List<Integer> evicting = evictingPartitionsAfterJoin(node, node.cache(DEFAULT_CACHE_NAME), PARTS);

        if (clientBefore)
            startClientGrid("client");

        int[] p0 = node.affinity(DEFAULT_CACHE_NAME).primaryPartitions(node.localNode());
        int[] b0 = node.affinity(DEFAULT_CACHE_NAME).backupPartitions(node.localNode());

        Set<Integer> reserved = IntStream.concat(IntStream.of(p0), IntStream.of(b0)).boxed().collect(Collectors.toSet());

        assertEquals(p0.length + b0.length, reserved.size());

        GridCacheContext<Object, Object> ctx = node.cachex(DEFAULT_CACHE_NAME).context();
        GridDhtPartitionTopology top = ctx.topology();

        GridDhtPartitionsReservation grpR = new GridDhtPartitionsReservation(top.readyTopologyVersion(), ctx, "TEST");

        Collection<GridDhtLocalPartition> resrv = F.view(top.localPartitions(), p -> reserved.contains(p.id()));
        resrv.forEach(GridDhtLocalPartition::reserve);

        assertTrue(grpR.register(resrv));
        resrv.forEach(GridDhtLocalPartition::release);

        assertTrue(grpR.reserve());

        if (!clientAfter)
            startGrid(cnt);
        else
            startClientGrid(cnt);

        for (Integer p : reserved) {
            GridDhtLocalPartition locPart = top.localPartition(p);

            assertEquals(locPart.toString(), OWNING, locPart.state());
        }

        if (!clientAfter) {
            // Make sure partitions are attempted to evict before calling "release".
            for (Integer p : evicting) {
                assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        GridDhtLocalPartition locPart = top.localPartition(p);

                        return U.field(locPart, "delayedRenting");
                    }
                }, 5_000));
            }
        }

        grpR.release();

        // Necessary to guaranatee a call to rent().
        assertTrue(GridTestUtils.waitForCondition(() ->
            top.readyTopologyVersion().equals(top.lastTopologyChangeVersion()), 5_000));

        assertEquals(clientAfter, grpR.reserve());

        awaitPartitionMapExchange(true, true, null);

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }
}
