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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class CacheAffinityCoordinatorInitTest extends GridCommonAbstractTest {
    /** */
    private static final int SRV_NODES = 3;

    /** */
    private static final int STATIC_GROUPS = 3;

    /** */
    private static final int CACHES_PER_GROUP = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        List<CacheConfiguration<?, ?>> caches = new ArrayList<>();

        for (int grpIdx = 0; grpIdx < STATIC_GROUPS; grpIdx++) {
            for (int cacheIdx = 0; cacheIdx < CACHES_PER_GROUP; cacheIdx++) {
                caches.add(new CacheConfiguration<>(cacheName(grpIdx, cacheIdx))
                    .setGroupName(groupName(grpIdx))
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setBackups(1));
            }
        }

        cfg.setCacheConfiguration(caches.toArray(new CacheConfiguration[0]));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        super.afterTest();
    }

    /** */
    @Test
    public void testStaticCacheGroupsOnCoordinatorStart() throws Exception {
        startGrids(SRV_NODES);

        awaitPartitionMapExchange();

        for (int i = 0; i < SRV_NODES; i++) {
            IgniteEx node = grid(i);

            for (int grpIdx = 0; grpIdx < STATIC_GROUPS; grpIdx++) {
                String grpName = groupName(grpIdx);

                int grpId = CU.cacheId(grpName);

                assertNotNull(
                    "Cache group context was not created [node=" + i + ", grp=" + grpName + ']',
                    node.context().cache().cacheGroup(grpId)
                );

                for (int cacheIdx = 0; cacheIdx < CACHES_PER_GROUP; cacheIdx++) {
                    String cacheName = cacheName(grpIdx, cacheIdx);

                    assertNotNull(
                        "Cache was not started [node=" + i + ", cache=" + cacheName + ']',
                        node.cache(cacheName)
                    );
                }
            }
        }
    }

    /** */
    private String cacheName(int grpIdx, int cacheIdx) {
        return "static-cache-" + grpIdx + '-' + cacheIdx;
    }

    /** */
    private String groupName(int grpIdx) {
        return "static-group-" + grpIdx;
    }
}
