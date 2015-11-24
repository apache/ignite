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

package org.apache.ignite.internal;

import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.affinity.GridCacheAffinityImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests usage of affinity in case when cache doesn't exist.
 */
public class GridAffinityNoCacheSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String EXPECTED_MSG = "Failed to find a cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityProxyNoCache() throws Exception {
        IgniteEx ignite = grid(0);

        final Affinity<Object> affinity = ignite.affinity("noCache");

        assertFalse("Affinity proxy instance expected", affinity instanceof GridCacheAffinityImpl);

        final Object key = new Object();
        final ClusterNode n = ignite.cluster().localNode();

        assertAffinityMethodsException(affinity, key, n);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityImplCacheDeleted() throws Exception {
        IgniteEx grid = grid(0);

        final String cacheName = "cacheToBeDeleted";

        grid(1).getOrCreateCache(cacheName);

        Affinity<Object> affinity = grid.affinity(cacheName);

        assertTrue(affinity instanceof GridCacheAffinityImpl);

        final Object key = new Object();
        final ClusterNode n = grid.cluster().localNode();

        grid.cache(cacheName).destroy();

        assertAffinityMethodsException(affinity, key, n);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityImplCacheObject() throws Exception {
        // TODO see affinityKey().
    }

    /**
     * @param affinity Affinity.
     * @param key Key.
     * @param n Node.
     */
    private void assertAffinityMethodsException(final Affinity<Object> affinity, final Object key,
        final ClusterNode n) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.affinityKey(key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.allPartitions(n);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.backupPartitions(n);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.isBackup(n, key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.isPrimary(n, key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.isPrimaryOrBackup(n, key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapKeysToNodes(Collections.singleton(key));
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapKeyToPrimaryAndBackups(key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapPartitionsToNodes(Collections.singleton(0));
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapPartitionToNode(0);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapPartitionToPrimaryAndBackups(0);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.mapKeyToNode(key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.partition(key);
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.partitions();
            }
        }, IgniteException.class, EXPECTED_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return affinity.primaryPartitions(n);
            }
        }, IgniteException.class, EXPECTED_MSG);
    }
}
