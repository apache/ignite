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

package org.apache.ignite.spi.swapspace;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.spi.swapspace.noop.NoopSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Check that all server nodes in grid have configured the same swap space spi. Check that client nodes could have any
 * swap space spi.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
public class GridSwapSpaceSpiConsistencySelfTest extends GridCommonAbstractTest {
    /** */
    protected static final String GRID_WITHOUT_SWAP_SPACE = "grid-without-swap-space";

    /** */
    protected static final String GRID_WITH_SWAP_SPACE = "grid-with-swap-space";

    /** */
    protected static final String GRID_CLIENT_WITHOUT_SWAP_SPACE = "grid-client-without-swap-space";

    /** */
    protected static final String GRID_CLIENT_WITH_SWAP_SPACE = "grid-client-with-swap-space";

    /** */
    protected static final String CACHE_NAME = "TestCache";

    /**
     *
     */
    public GridSwapSpaceSpiConsistencySelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (GRID_WITHOUT_SWAP_SPACE.equals(gridName))
            cfg.setSwapSpaceSpi(new NoopSwapSpaceSpi());

        if (GRID_WITH_SWAP_SPACE.equals(gridName))
            cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        if (GRID_CLIENT_WITHOUT_SWAP_SPACE.equals(gridName)) {
            cfg.setClientMode(true);
            cfg.setSwapSpaceSpi(new NoopSwapSpaceSpi());
        }

        if (GRID_CLIENT_WITH_SWAP_SPACE.equals(gridName)) {
            cfg.setClientMode(true);
            cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Node with swap enabled should not start after node without swap
     */
    public void testServerNodeIncompatibleSwapSpaceSpi1() throws Exception {
        startGrid(GRID_WITHOUT_SWAP_SPACE);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(GRID_WITH_SWAP_SPACE);
            }
        }, IgniteCheckedException.class, "Failed to initialize SPI context");
    }

    /**
     * Node without swap should not start after node with swap enabled
     */
    public void testServerNodeIncompatibleSwapSpaceSpi2() throws Exception {
        startGrid(GRID_WITH_SWAP_SPACE);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(GRID_WITHOUT_SWAP_SPACE);
            }
        }, IgniteCheckedException.class, "Failed to initialize SPI context");
    }

    /**
     * Client nodes should join to grid with any swap policy
     */
    public void testClientNodeAnySwapSpaceSpi() throws Exception {
        startGrid(GRID_WITHOUT_SWAP_SPACE);

        Ignite client1 = startGrid(GRID_CLIENT_WITH_SWAP_SPACE);

        Ignite client2 = startGrid(GRID_CLIENT_WITHOUT_SWAP_SPACE);

        IgniteCache<Integer, String> cache1 = client1.createCache("TestCache");

        cache1.put(1, "one");

        IgniteCache<Integer, String> cache2 = client2.getOrCreateCache("TestCache");

        assert cache2.get(1).equals("one");
    }
}