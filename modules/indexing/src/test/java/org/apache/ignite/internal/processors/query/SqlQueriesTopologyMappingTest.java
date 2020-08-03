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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/** */
public class SqlQueriesTopologyMappingTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
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

        IgniteCache<Object, Object> cache = ign0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.LOCAL)
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Integer.class));

        cache.put(1, 2);

        startGrid(1);

        SqlFieldsQuery qry = new SqlFieldsQuery("select * from Integer");

        {
            List<List<?>> res0 = grid(0).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

            assertEquals(1, res0.size());
            assertEqualsCollections(Arrays.asList(1, 2), res0.get(0));
        }

        {
            List<List<?>> res1 = grid(1).cache(DEFAULT_CACHE_NAME).query(qry).getAll();

            assertTrue(res1.isEmpty());
        }
    }

    /** */
    private void checkQueryWithRebalance(CacheMode cacheMode) throws Exception {
        IgniteEx ign0 = startGrid(0);

        IgniteCache<Object, Object> cache = ign0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(cacheMode)
            .setIndexedTypes(Integer.class, Integer.class));

        cache.put(1, 2);

        blockRebalanceSupplyMessages(ign0, DEFAULT_CACHE_NAME, getTestIgniteInstanceName(1));

        startGrid(1);
        startClientGrid(10);

        for (Ignite ign : G.allGrids()) {
            List<List<?>> res = ign.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("select * from Integer")).getAll();

            assertEquals(1, res.size());
            assertEqualsCollections(Arrays.asList(1, 2), res.get(0));
        }
    }

    /** */
    private void checkQueryWithNodeFilter(CacheMode cacheMode) throws Exception {
        IgniteEx ign0 = startGrid(0);
        String name0 = ign0.name();

        IgniteCache<Object, Object> cache = ign0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(cacheMode)
            .setNodeFilter(node -> name0.equals(node.attribute(ATTR_IGNITE_INSTANCE_NAME)))
            .setIndexedTypes(Integer.class, Integer.class));

        cache.put(1, 2);

        startGrid(1);
        startClientGrid(10);

        for (Ignite ign : G.allGrids()) {
            List<List<?>> res = ign.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("select * from Integer")).getAll();

            assertEquals(1, res.size());
            assertEqualsCollections(Arrays.asList(1, 2), res.get(0));
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
