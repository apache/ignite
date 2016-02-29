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

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.spi.swapspace.noop.NoopSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Check creation of cache with swap space enabled on grids with and without swap space spi
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal")
public class GridCacheSwapSpaceSpiConsistencySelfTest extends GridCommonAbstractTest {
    /** */
    protected static final String GRID_WITHOUT_SWAP_SPACE = "grid-without-swap-space";

    /** */
    protected static final String GRID_WITH_SWAP_SPACE = "grid-with-swap-space";

    /** */
    protected static final String GRID_CLIENT = "grid-client";

    /** */
    protected static final String CACHE_NAME = "TestCache";

    /**
     *
     */
    public GridCacheSwapSpaceSpiConsistencySelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.startsWith(GRID_WITHOUT_SWAP_SPACE))
            cfg.setSwapSpaceSpi(new NoopSwapSpaceSpi());

        if (gridName.startsWith(GRID_WITH_SWAP_SPACE))
            cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        if (GRID_CLIENT.equals(gridName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void testInconsistentCacheCreationFromClient() throws Exception {
        testInconsistentCacheCreation(true);
    }

    /**
     *
     */
    public void testInconsistentCacheCreationFromServer() throws Exception {
        testInconsistentCacheCreation(false);
    }

    /**
     * It should be impossible to create cache with swap enabled on grid without swap.
     */
    public void testInconsistentCacheCreation(boolean fromClient) throws Exception {
        startGrid(GRID_WITHOUT_SWAP_SPACE);

        final Ignite ignite = startGrid(fromClient ? GRID_CLIENT : GRID_WITHOUT_SWAP_SPACE + "2");

        final CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

        cfg.setSwapEnabled(true);
        cfg.setName(CACHE_NAME);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return ignite.createCache(cfg);
            }
        }, CacheException.class, "Failed to start cache " + CACHE_NAME + " with swap enabled:");
    }

    /**
     *
     */
    public void testConsistentCacheCreationFromClient() throws Exception {
        testConsistentCacheCreation(true);
    }

    /**
     *
     */
    public void testConsistentCacheCreationFromServer() throws Exception {
        testConsistentCacheCreation(false);
    }

    /**
     * It should ok to create cache with swap enabled on grid with swap.
     */
    public void testConsistentCacheCreation(boolean fromClient) throws Exception {
        startGrid(GRID_WITH_SWAP_SPACE);

        final Ignite ignite = startGrid(fromClient ? GRID_CLIENT : GRID_WITH_SWAP_SPACE + "2");

        final CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

        cfg.setSwapEnabled(true);
        cfg.setName(CACHE_NAME);

        IgniteCache<Integer, String> cache = ignite.createCache(cfg);

        cache.put(1, "one");

        assert cache.get(1).equals("one");
    }
}
