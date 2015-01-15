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

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Load-All self test.
 */
public class GridCacheLocalLoadAllSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridCacheLocalLoadAllSelfTest() {
        super(true);
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testCacheGetAll() throws Exception {
        Ignite ignite = grid();

        assert ignite != null;

        ignite.cache("test-cache").getAll(Collections.singleton(1));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cache = defaultCacheConfiguration();

        cache.setName("test-cache");
        cache.setCacheMode(LOCAL);
        cache.setStore(new TestStore());

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @SuppressWarnings({"TypeParameterExtendsFinalClass"})
        @Override public void loadAll(Collection<? extends Integer> keys,
            IgniteBiInClosure<Integer, Integer> c) {
            assert keys != null;

            c.apply(1, 1);
            c.apply(2, 2);
            c.apply(3, 3);
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) {
            // No-op.

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(Integer key, Integer val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(Integer key) {
            // No-op.
        }
    }
}
