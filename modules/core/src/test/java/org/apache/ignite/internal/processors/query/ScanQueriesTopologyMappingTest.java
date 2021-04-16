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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/** */
public class ScanQueriesTopologyMappingTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setClientMode(client);
    }

    /** */
    @Test
    public void testPartitionedQueryWithRebalance() throws Exception {
        checkQueryWithRebalance(CacheMode.PARTITIONED);
    }

    /** */
    @Test
    public void testReplicatedQueryWithRebalance() throws Exception {
        checkQueryWithRebalance(CacheMode.REPLICATED);
    }

    /** */
    @Test
    public void testPartitionedQueryWithNodeFilter() throws Exception {
        checkQueryWithNodeFilter(CacheMode.PARTITIONED);
    }

    /** */
    @Test
    public void testReplicatedQueryWithNodeFilter() throws Exception {
        checkQueryWithNodeFilter(CacheMode.REPLICATED);
    }

    /** */
    @Test
    public void testLocalCacheQueryMapping() throws Exception {
        IgniteEx ign0 = startGrid(0);

        IgniteCache<Object, Object> cache = ign0.createCache(new CacheConfiguration<>(GridAbstractTest.DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.LOCAL));

        cache.put(1, 2);

        startGrid(1);

        ScanQuery<Object, Object> qry = new ScanQuery<>();

        {
            List<Cache.Entry<Object, Object>> res0 = grid(0).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

            assertEquals(1, res0.size());
            assertEquals(1, res0.get(0).getKey());
            assertEquals(2, res0.get(0).getValue());
        }

        {
            List<Cache.Entry<Object, Object>> res1 = grid(1).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

            assertTrue(res1.isEmpty());
        }
    }

    /** */
    private void checkQueryWithRebalance(CacheMode cacheMode) throws Exception {
        IgniteEx ign0 = startGrid(0);

        IgniteCache<Object, Object> cache0 = ign0.createCache(new CacheConfiguration<>(GridAbstractTest.DEFAULT_CACHE_NAME)
                .setCacheMode(cacheMode));

        cache0.put(1, 2);

        blockRebalanceSupplyMessages(ign0, DEFAULT_CACHE_NAME, getTestIgniteInstanceName(1));

        startGrid(1);

        client = true;

        startGrid(10);

        int part = ign0.affinity(DEFAULT_CACHE_NAME).partition(1);

        for (int i = 0; i < 100; i++) {
            for (Ignite ign : G.allGrids()) {
                IgniteCache<Object, Object> cache = ign.cache(DEFAULT_CACHE_NAME);

                //check scan query
                List<Cache.Entry<Object, Object>> res = cache.query(new ScanQuery<>()).getAll();

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());

                res = new ArrayList<>();

                //check iterator
                for (Cache.Entry<Object, Object> entry : cache) {
                    res.add(entry);
                }

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());

                // check scan query by partition
                res = cache.query(new ScanQuery<>(part)).getAll();

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());
            }

            ScanQuery<Object, Object> qry = new ScanQuery<>().setLocal(true);

            {
                List<Cache.Entry<Object, Object>> res0 = grid(0).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

                assertEquals(1, res0.size());
                assertEquals(1, res0.get(0).getKey());
                assertEquals(2, res0.get(0).getValue());
            }

            {
                List<Cache.Entry<Object, Object>> res1 = grid(1).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

                assertTrue(res1.isEmpty());
            }

            {
                List<Cache.Entry<Object, Object>> res1 = grid(10).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

                assertTrue(res1.isEmpty());
            }
        }
    }

    /** */
    private void checkQueryWithNodeFilter(CacheMode cacheMode) throws Exception {
        IgniteEx ign0 = startGrid(0);
        String name0 = ign0.name();

        IgniteCache<Object, Object> cache0 = ign0.createCache(new CacheConfiguration<>(GridAbstractTest.DEFAULT_CACHE_NAME)
                .setCacheMode(cacheMode)
                .setNodeFilter(node -> name0.equals(node.attribute(ATTR_IGNITE_INSTANCE_NAME))));

        cache0.put(1, 2);

        startGrid(1);

        client = true;

        startGrid(10);

        int part = ign0.affinity(DEFAULT_CACHE_NAME).partition(1);

        for (int i = 0; i < 100; i++) {
            for (Ignite ign : G.allGrids()) {
                IgniteCache<Object, Object> cache = ign.cache(GridAbstractTest.DEFAULT_CACHE_NAME);

                List<Cache.Entry<Object, Object>> res = cache.query(new ScanQuery<>()).getAll();

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());

                res = new ArrayList<>();

                for (Cache.Entry<Object, Object> entry : cache) {
                    res.add(entry);
                }

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());

                res = cache.query(new ScanQuery<>(part)).getAll();

                assertEquals(1, res.size());
                assertEquals(1, res.get(0).getKey());
                assertEquals(2, res.get(0).getValue());
            }

            ScanQuery<Object, Object> qry = new ScanQuery<>().setLocal(true);

            {
                List<Cache.Entry<Object, Object>> res0 = grid(0).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

                assertEquals(1, res0.size());
                assertEquals(1, res0.get(0).getKey());
                assertEquals(2, res0.get(0).getValue());
            }

            {
                List<Cache.Entry<Object, Object>> res1 = grid(1).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

                assertTrue(res1.isEmpty());
            }

            {
                List<Cache.Entry<Object, Object>> res1 = grid(10).cache(GridAbstractTest.DEFAULT_CACHE_NAME).query(qry).getAll();

                assertTrue(res1.isEmpty());
            }
        }
    }

    /** */
    private void blockRebalanceSupplyMessages(IgniteEx sndNode, String cacheName, String dstNodeName) {
        int grpId = sndNode.cachex(cacheName).context().groupId();

        TestRecordingCommunicationSpi comm0 = (TestRecordingCommunicationSpi)sndNode.configuration().getCommunicationSpi();
        comm0.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                String dstName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

                if (dstNodeName.equals(dstName) && msg instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)msg;
                    return msg0.groupId() == grpId;
                }

                return false;
            }
        });
    }
}
