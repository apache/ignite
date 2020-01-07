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

import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteSystemCacheOnClientTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSystemCacheOnClientNode() throws Exception {
        startGrid(0);
        startClientGrid(1);

        final IgniteKernal ignite = (IgniteKernal)ignite(1);

        assertTrue(ignite.configuration().isClientMode());

        GridCacheAdapter utilityCache = ((IgniteKernal)ignite(0)).internalCache(CU.UTILITY_CACHE_NAME);

        Collection<ClusterNode> affNodes = utilityCache.affinity().mapKeyToPrimaryAndBackups(1);

        assertEquals(1, affNodes.size());
        assertTrue(affNodes.contains(ignite(0).cluster().localNode()));
    }
}
