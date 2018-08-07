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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;

/**
 * Test correctness of forced PME and rebalancing selected subset of partitions on some nodes.
 */
public class PartitionRebalanceRequestTest extends GridCommonAbstractTest {
    /** Partitions count. */
    private final int PARTS_CNT = 32;

    /** */
    private final Random rnd = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setCheckpointFrequency(10 * 60 * 1000)
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                                .setMaxSize(512 * 1024 * 1024)
                                .setPersistenceEnabled(true)
                );

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public void testPartitionRebalanceRequest() throws Exception {
        IgniteEx  ig = (IgniteEx)startGrids(2);

        ig.cluster().active(true);

        try(IgniteDataStreamer streamer = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 1000; i++)
                streamer.addData(i, i << 3);
        }

        int grpId = ig.cachex(DEFAULT_CACHE_NAME).context().groupId();

        ConcurrentMap<Integer, Set<UUID>> finRes = new ConcurrentHashMap<>();

        CountDownLatch finRebLatch = new CountDownLatch(2);

        Map<Integer, Set<UUID>> payload = new HashMap<>();

        for (int i = 0 ; i < PARTS_CNT; i++) {
            if (i < PARTS_CNT / 2)
                payload.put(i, Collections.singleton(grid(i % 2).localNode().id()));
            else
                payload.put(i, new HashSet<>(Arrays.asList(grid(0).localNode().id(),
                        grid(1).localNode().id())));
        }

        for (Ignite g: G.allGrids()) {
            g.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    CacheRebalancingEvent rebEvt = (CacheRebalancingEvent)evt;

                    log.info("Received " + rebEvt);

                    if(rebEvt.cacheName().equals(DEFAULT_CACHE_NAME)) {
                        if (rebEvt.type() == EVT_CACHE_REBALANCE_PART_LOADED)
                            finRes.compute(rebEvt.partition(), (id, set) -> {
                                if (set == null)
                                    return Collections.singleton(g.cluster().localNode().id());
                                else {
                                    set.add(g.cluster().localNode().id());

                                    return set;
                                }
                            });
                        else
                            finRebLatch.countDown();
                    }

                    return true;
                }
            }, EVT_CACHE_REBALANCE_PART_LOADED,  EVT_CACHE_REBALANCE_STOPPED);

        }

        log.info("Payload to rebalance: " + payload);

        Map<UUID, Map<Integer, Long>> updCtrs = new HashMap<>();

        for (Ignite g: G.allGrids()) {
            CacheGroupContext grp = ((IgniteEx) g).context().cache().cacheGroup(grpId);

            Map<Integer, Long> nodeUpdCtr = new HashMap<>();

            for (GridDhtLocalPartition p : grp.topology().localPartitions())
                nodeUpdCtr.put(p.id(), p.updateCounter());

            updCtrs.put(g.cluster().localNode().id(), nodeUpdCtr);
        }

        ig.context().discovery().sendCustomEvent(createMessage(grpId, payload));

        finRebLatch.await();

        for (Ignite g: G.allGrids()) {
            CacheGroupContext grp = ((IgniteEx) g).context().cache().cacheGroup(grpId);

            Map<Integer, Long> nodeUpdCtr = updCtrs.get(g.cluster().localNode().id());

            for (GridDhtLocalPartition p : grp.topology().localPartitions())
                assertEquals("Update counters for partition should not change", nodeUpdCtr.get(p.id()), Long.valueOf(p.updateCounter()));
        }

        log.info("Real rebalance result: " + finRes);

        for (Map.Entry<Integer, Set<UUID>> parts: payload.entrySet()) {
            Set<UUID> nodes = parts.getValue();

            if (nodes.size() == 2)
                assertTrue("Request for partition with all owners should be ignored", !finRes.containsKey(parts.getKey()));
            else if (nodes.size() == 1)
                assertTrue("Request for partition with all owners should be ignored", finRes.containsKey(parts.getKey()));
        }
    }

    /**
     * @param gId Group id.
     * @param payload Payload partition and nodes ids.
     */
    private PartitionRebalanceRequestMessage createMessage(Integer gId, Map<Integer, Set<UUID>> payload) {
        Map<Integer, Map<Integer, Set<UUID>>> data = new HashMap<>();

        data.put(gId, payload);

        return new PartitionRebalanceRequestMessage(data);
    }
}
