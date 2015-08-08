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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;

import java.io.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.igfs.IgfsMode.*;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;

/**
 *
 */
public class IgfsStartCacheTest extends IgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * @param igfs If {@code true} created IGFS configuration.
     * @param idx Node index.
     * @return Configuration
     */
    private IgniteConfiguration config(boolean igfs, int idx) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        if (igfs) {
            FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

            igfsCfg.setDataCacheName("dataCache");
            igfsCfg.setMetaCacheName("metaCache");
            igfsCfg.setName("igfs");
            igfsCfg.setDefaultMode(PRIMARY);
            igfsCfg.setFragmentizerEnabled(false);

            CacheConfiguration dataCacheCfg = new CacheConfiguration();

            dataCacheCfg.setName("dataCache");
            dataCacheCfg.setCacheMode(PARTITIONED);
            dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
            dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
            dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(1));

            CacheConfiguration metaCacheCfg = new CacheConfiguration();

            metaCacheCfg.setName("metaCache");
            metaCacheCfg.setCacheMode(REPLICATED);
            metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
            dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
            cfg.setFileSystemConfiguration(igfsCfg);
        }

        cfg.setGridName("node-" + idx);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheStart() throws Exception {
        Ignite g0 = G.start(config(true, 0));

        checkIgfsCaches(g0);

        Ignite g1 = G.start(config(false, 1));

        checkIgfsCaches(g1);

        IgniteFileSystem igfs = g0.fileSystem("igfs");

        igfs.mkdirs(new IgfsPath("/test"));

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(igfs.create(
            new IgfsPath("/test/test.file"), true)))) {

            for (int i = 0; i < 1000; i++)
                bw.write("test-" + i);
        }
    }

    /**
     * @param ignite Ignite.
     */
    private void checkIgfsCaches(final Ignite ignite) {
        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.cache("dataCache");

                return null;
            }
        }, IllegalStateException.class, null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.cache("metaCache");

                return null;
            }
        }, IllegalStateException.class, null);

        checkCache(((IgniteKernal)ignite).internalCache("dataCache"));
        checkCache(((IgniteKernal)ignite).internalCache("metaCache"));
    }

    /**
     * @param cache Cache.
     */
    private void checkCache(GridCacheAdapter cache) {
        assertNotNull(cache);
        assertFalse(cache.context().userCache());
        assertFalse(cache.context().systemTx());
        assertEquals(SYSTEM_POOL, cache.context().ioPolicy());
    }
}
