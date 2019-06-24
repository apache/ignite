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

package org.apache.ignite.internal.processors.igfs;

import java.util.concurrent.Callable;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for igfs with one node in client mode.
 */
public class IgfsOneClientNodeTest extends GridCommonAbstractTest {
    /** Regular cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));

        cfg.setClientMode(true);

        cfg.setDiscoverySpi(new TcpDiscoverySpi()
            .setForceServerMode(true)
            .setIpFinder(new TcpDiscoveryVmIpFinder(true)));

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName("igfs");
        igfsCfg.setMetaCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));
        igfsCfg.setDataCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        cfg.setFileSystemConfiguration(igfsCfg);

        return cfg;
    }

    /** */
    protected CacheConfiguration cacheConfiguration(@NotNull String cacheName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        cacheCfg.setCacheMode(PARTITIONED);

        cacheCfg.setBackups(0);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartIgfs() throws Exception {
        final IgfsImpl igfs = (IgfsImpl) grid(0).fileSystem("igfs");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsAbstractSelfTest.create(igfs, new IgfsPath[]{new IgfsPath("/dir")}, null);
                return null;
            }
        }, IgfsException.class, "Failed to execute operation because there are no IGFS metadata nodes.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsPath FILE = new IgfsPath(new IgfsPath("/dir"), "file");

                igfs.delete(FILE, false);

                return null;
            }
        }, IgfsException.class, "Failed to execute operation because there are no IGFS metadata nodes.");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsPath FILE = new IgfsPath(new IgfsPath("/dir"), "file");

                igfs.append(FILE, true);

                return null;
            }
        }, IgfsException.class, "Failed to execute operation because there are no IGFS metadata nodes.");
    }
}
