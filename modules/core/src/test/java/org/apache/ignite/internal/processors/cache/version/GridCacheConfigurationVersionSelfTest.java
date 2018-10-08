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
package org.apache.ignite.internal.processors.cache.version;

import java.util.Comparator;
import org.apache.ignite.internal.IgniteEx;

import static org.apache.ignite.internal.processors.cache.version.GridCacheConfigurationChangeAction.DESTROY;
import static org.apache.ignite.internal.processors.cache.version.GridCacheConfigurationChangeAction.START;

public class GridCacheConfigurationVersionSelfTest extends GridCacheConfigurationVersionAbstractSelfTest {
    /** Cache name. */
    private static final String CACHE_NAME = DEFAULT_CACHE_NAME + "-test";

    public void testRestartNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.getOrCreateCache(CACHE_NAME);

        checkCacheVersion(ignite, CACHE_NAME, 1, START);

        stopAllGrids();

        ignite = startGrid(0);

        ignite.cluster().active(true);

        checkCacheVersion(ignite, CACHE_NAME, 1, START);

    }

    public void testSignleNode() throws Exception {
        testSameVersionOnNodes(1, 0, 0, false, null);
    }

    public void testTwoNodes0() throws Exception {
        testSameVersionOnNodes(2, 0, 0, false, null);
    }

    public void testTwoNodes1() throws Exception {
        testSameVersionOnNodes(2, 1, 0, false, null);
    }

    public void testTwoNodesWithStopSecond1() throws Exception {
        testSameVersionOnNodes(2, 0, 1, false, null);
    }

    public void testTwoNodesWithStopSecond2() throws Exception {
        testSameVersionOnNodes(2, 0, 2, false, null);
    }

    public void testTwoNodesWithStopSecond1RestartNatural() throws Exception {
        testSameVersionOnNodes(2, 0, 1, true, Comparator.naturalOrder());
    }

    public void testTwoNodesWithStopSecond1RestartReverse() throws Exception {
        testSameVersionOnNodes(2, 0, 1, true, Comparator.reverseOrder());
    }

    public void testTwoNodesWithStopSecond2RestartNatural() throws Exception {
        testSameVersionOnNodes(2, 0, 2, true, Comparator.naturalOrder());
    }

    public void testTwoNodesWithStopSecond2RestartReverse() throws Exception {
        testSameVersionOnNodes(2, 0, 2, true, Comparator.reverseOrder());
    }

    @Override protected int performActionsOnCache(
        int firstNodeId,
        int lastNodeId,
        int version,
        IgniteEx ignite
    ) throws Exception {
        ignite.getOrCreateCache(CACHE_NAME);

        version++;

        for (int i = firstNodeId; i < lastNodeId; i++)
            checkCacheVersion(grid(i), CACHE_NAME, version, START);

        ignite.cache(CACHE_NAME).destroy();

        Thread.sleep(1000L);

        version++;

        for (int i = firstNodeId; i < lastNodeId; i++)
            checkCacheVersion(grid(i), CACHE_NAME, version, DESTROY);

        return version;
    }

}