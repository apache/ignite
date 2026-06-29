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

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class DynamicCacheStartExchangeTest extends GridCommonAbstractTest {
    /** */
    private static final int SRV_NODES = 3;

    /** */
    private static final int FIRST_CLIENT_PORT = 10800;

    /** */
    private static final int DYNAMIC_GROUPS = 3;

    /** */
    private static final int CACHES_PER_GROUP = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        return cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setPort(FIRST_CLIENT_PORT + idx)
            .setPortRange(0));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        super.afterTest();
    }

    /** */
    @Test
    public void testThinClientDynamicCacheStartCreatesCacheGroups() throws Exception {
        startGrids(SRV_NODES);

        try (IgniteClient client = Ignition.startClient(clientConfiguration())) {
            for (int grpIdx = 0; grpIdx < DYNAMIC_GROUPS; grpIdx++) {
                for (int cacheIdx = 0; cacheIdx < CACHES_PER_GROUP; cacheIdx++) {
                    ClientCache<Integer, Integer> cache = prepareCache(client, grpIdx, cacheIdx);

                    int key = grpIdx * 100 + cacheIdx;

                    cache.put(key, key);

                    assertEquals(Integer.valueOf(key), cache.get(key));
                }
            }
        }

        for (int grpIdx = 0; grpIdx < DYNAMIC_GROUPS; grpIdx++) {
            String grpName = groupName(grpIdx);

            int grpId = CU.cacheId(grpName);

            for (int i = 0; i < SRV_NODES; i++)
                assertNotNull(grid(i).context().cache().cacheGroup(grpId));

            for (int cacheIdx = 0; cacheIdx < CACHES_PER_GROUP; cacheIdx++) {
                String cacheName = cacheName(grpIdx, cacheIdx);

                for (int i = 0; i < SRV_NODES; i++)
                    assertNotNull(grid(i).cache(cacheName));
            }
        }
    }

    /** */
    private ClientConfiguration clientConfiguration() {
        return new ClientConfiguration().setAddresses(clientAddresses());
    }

    /** */
    private String[] clientAddresses() {
        String[] addrs = new String[SRV_NODES];

        for (int i = 0; i < SRV_NODES; i++)
            addrs[i] = "127.0.0.1:" + (FIRST_CLIENT_PORT + i);

        return addrs;
    }

    /** */
    private ClientCache<Integer, Integer> prepareCache(IgniteClient client, int grpIdx, int cacheIdx) {
        return client.getOrCreateCache(new ClientCacheConfiguration()
            .setName(cacheName(grpIdx, cacheIdx))
            .setGroupName(groupName(grpIdx))
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(2));
    }

    /** */
    private String cacheName(int grpIdx, int cacheIdx) {
        return "thin-dynamic-cache-" + grpIdx + '-' + cacheIdx;
    }

    /** */
    private String groupName(int grpIdx) {
        return "thin-dynamic-group-" + grpIdx;
    }
}
