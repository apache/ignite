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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Utility class for ClusterState* tests.
 */
public class ClusterStateTestUtils {
    /** */
    public static final int ENTRY_CNT = 50;

    /**
     * @param cacheName Cache name.
     * @return Partitioned cache configuration with 1 backup.
     */
    public static CacheConfiguration partitionedCache(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Replicated cache configuration.
     */
    public static CacheConfiguration replicatedCache(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /** */
    public static void putSomeDataAndCheck(IgniteLogger log, List<IgniteEx> nodes, String... cacheNames) {
        assertFalse(F.isEmpty(nodes));
        assertFalse(F.isEmpty(cacheNames));

        IgniteEx crd = nodes.get(0);

        for (String cacheName : cacheNames) {
            switch (crd.cluster().state()) {
                case INACTIVE:
                    assertNotNull(assertThrows(
                        log,
                        () -> crd.cache(cacheName),
                        IgniteException.class,
                        "Can not perform the operation because the cluster is inactive. Note, that the cluster is considered inactive by default if Ignite Persistent Store is used to let all the nodes join the cluster. To activate the cluster call Ignite.active(true)."
                    ));

                    break;

                case ACTIVE:
                    for (int k = 0; k < ENTRY_CNT; k++)
                        crd.cache(cacheName).put(k, k);

                    for (Ignite node : nodes) {
                        for (int k = 0; k < ENTRY_CNT; k++)
                            assertEquals(k, node.cache(cacheName).get(k));
                    }

                    break;

                case ACTIVE_READ_ONLY:
                    assertNotNull(assertThrowsWithCause(
                        () -> crd.cache(cacheName).put(0, 0),
                        IgniteClusterReadOnlyException.class
                    ));

                    for (Ignite node : nodes)
                        assertNull(node.cache(cacheName).get(0));

                    break;
            }
        }
    }

    /** */
    private ClusterStateTestUtils() {
    }
}
