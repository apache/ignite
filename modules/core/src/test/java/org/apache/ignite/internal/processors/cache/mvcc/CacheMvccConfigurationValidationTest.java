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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class CacheMvccConfigurationValidationTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMvccEnabled(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccModeMismatchForGroup1() throws Exception {
        final Ignite node = startGrid(0);

        node.createCache(new CacheConfiguration("cache1").setGroupName("grp1").setAtomicityMode(ATOMIC));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL));

                return null;
            }
        }, CacheException.class, null);

        node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccModeMismatchForGroup2() throws Exception {
        final Ignite node = startGrid(0);

        node.createCache(new CacheConfiguration("cache1").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(ATOMIC));

                return null;
            }
        }, CacheException.class, null);

        node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL));
    }
}
