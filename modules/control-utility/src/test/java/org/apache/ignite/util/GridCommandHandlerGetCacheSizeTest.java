/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import org.apache.ignite.Ignite;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;

/**
 * Test for get cache size
 */
public class GridCommandHandlerGetCacheSizeTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    public static final int GRID_CNT = 2;

    /**
     * Default cache size in createAndFillCache
     */
    public static final int CACHE_SIZE = 10000;

    /**
     * Test new answer from cache list command
     */
    @Test
    public void testValidateGridCommandHandlerGetCacheSizeTest() throws Exception {
        prepareGridForTest();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", "."));

        String out = testOut.toString();

        assertContains(log, out, "cacheSize=" + CACHE_SIZE);

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", ".", "--groups"));

        out = testOut.toString();

        assertContains(log, out, "cacheSize=" + (CACHE_SIZE + CACHE_SIZE));
        assertContains(log, out, "cacheSize=" + CACHE_SIZE);
    }

    /**
     * Create and fill nodes.
     *
     * @throws Exception - exception
     */
    private Ignite prepareGridForTest() throws Exception {
        Ignite ignite = startGrids(GRID_CNT);

        ignite.cluster().active(true);

        Ignite client = startGrid(CLIENT_NODE_NAME_PREFIX);

        String cacheGrpName1 = GROUP_NAME;
        String cacheGrpName2 = GROUP_NAME + "0";

        createAndFillCache(client, CACHE_NAME, cacheGrpName1);
        createAndFillCache(client, CACHE_NAME + "0", cacheGrpName1);
        createAndFillCache(client, CACHE_NAME + "1", cacheGrpName2);

        return ignite;
    }

    /**
     * Stop all grids and clean heap memory
     *
     * @throws Exception - exception
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
        cleanPersistenceDir();
    }

}
