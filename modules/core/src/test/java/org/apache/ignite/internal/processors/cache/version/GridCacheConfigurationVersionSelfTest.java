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

import org.apache.ignite.internal.IgniteEx;

import static org.apache.ignite.internal.processors.cache.version.GridCacheConfigurationChangeAction.DESTROY;
import static org.apache.ignite.internal.processors.cache.version.GridCacheConfigurationChangeAction.START;

/**
 *
 */
public class GridCacheConfigurationVersionSelfTest extends GridCacheConfigurationVersionAbstractSelfTest {
    /** Cache name. */
    private static final String CACHE_NAME = DEFAULT_CACHE_NAME + "-test";

    /** */
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

    /** {@inheritDoc} */
    @Override protected int performActionsOnCache(
        int firstNodeId,
        int lastNodeId,
        int ver,
        IgniteEx ignite
    ) throws Exception {
        ignite.getOrCreateCache(CACHE_NAME);

        awaitCacheVersion(firstNodeId, lastNodeId, CACHE_NAME, ++ver, 5000L);

        for (int i = firstNodeId; i < lastNodeId; i++)
            checkCacheVersion(grid(i), CACHE_NAME, ver, START);

        ignite.cache(CACHE_NAME).destroy();

        awaitCacheVersion(firstNodeId, lastNodeId, CACHE_NAME, ++ver, 5000L);

        for (int i = firstNodeId; i < lastNodeId; i++)
            checkCacheVersion(grid(i), CACHE_NAME, ver, DESTROY);

        return ver;
    }

}