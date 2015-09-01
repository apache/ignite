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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base class for various tests for byte array values.
 */
public abstract class GridCacheAbstractByteArrayValuesSelfTest extends GridCommonAbstractTest {
    /** Regular cache name. */
    protected static final String CACHE_REGULAR = "cache";

    /** Offheap cache name. */
    protected static final String CACHE_OFFHEAP = "cache_offheap";

    /** Offheap tiered cache name. */
    protected static final String CACHE_OFFHEAP_TIERED = "cache_offheap_tiered";

    /** Key 1. */
    protected static final Integer KEY_1 = 1;

    /** Key 2. */
    protected static final Integer KEY_2 = 2;

    /** Use special key for swap test, otherwise entry with readers is not evicted. */
    protected static final Integer SWAP_TEST_KEY = 3;

    /** Shared IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Wrap provided values into byte array.
     *
     * @param vals Values.
     * @return Byte array.
     */
    protected byte[] wrap(int... vals) {
        byte[] res = new byte[vals.length];

        for (int i = 0; i < vals.length; i++)
            res[i] = (byte)vals[i];

        return res;
    }
}