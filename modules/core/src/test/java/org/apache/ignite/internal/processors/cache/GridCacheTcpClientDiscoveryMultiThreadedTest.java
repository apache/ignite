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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests TcpClientDiscovery SPI with multiple client nodes that interact with a cache concurrently.
 */
public class GridCacheTcpClientDiscoveryMultiThreadedTest extends GridCacheAbstractSelfTest {
    /** Server nodes count. */
    private final static int SERVER_NODES_COUNT = 3;

    /** Client nodes count. */
    private final static int CLIENT_NODES_COUNT = 5;

    /** Grids counter. */
    private static AtomicInteger gridsCounter;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return SERVER_NODES_COUNT + CLIENT_NODES_COUNT;
    }
    
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        gridsCounter = new AtomicInteger();
        
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName);
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        return super.cacheConfiguration(gridName);
    }
}
