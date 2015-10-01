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
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests usage of affinity in case when cache doesn't exist.
 */
public class GridAffinityNoCacheSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testCornerCases() throws Exception {
        Ignite ignite = startGrid();

        Affinity<Object> affinity = ignite.affinity("noCache");

        Object key = new Object();
        ClusterNode node = ignite.cluster().localNode();

        assertEquals(0, affinity.partitions());
        assertEquals(-1, affinity.partition(key));
        assertEquals(null, affinity.affinityKey(key));
        assertEquals(U.emptyIntArray(), affinity.allPartitions(node));
        assertEquals(U.emptyIntArray(), affinity.backupPartitions(node));
        assertEquals(U.emptyIntArray(), affinity.primaryPartitions(node));
        assertEquals(false, affinity.isBackup(node, key));
        assertEquals(false, affinity.isPrimary(node, key));
        assertEquals(false, affinity.isPrimaryOrBackup(node, key));

        assertEquals(null, affinity.mapKeyToNode(key));
        assertEquals(Collections.emptyList(), affinity.mapKeyToPrimaryAndBackups(key));
        assertEquals(Collections.emptyMap(), affinity.mapKeysToNodes(Collections.singleton(key)));
        assertEquals(null, affinity.mapPartitionToNode(0));
        assertEquals(Collections.emptyList(), affinity.mapPartitionToPrimaryAndBackups(0));
        assertEquals(Collections.emptyMap(), affinity.mapPartitionsToNodes(Collections.singleton(0)));
    }
}
