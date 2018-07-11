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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_TMP_FILENAME;

/**
 * Tests that ignite can start when caches' configurations with same name in different groups stored.
 */
public class IgnitePdsCacheConfigurationFileConsistencyCheckTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int CACHES = 4;

    /** */
    private static final int NODES = 4;

    /** */
    private static final String ODD_GROUP_NAME = "group-odd";

    /** */
    private static final String EVEN_GROUP_NAME = "group-even";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg.setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(IP_FINDER))
                .setDataStorageConfiguration(new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                                .setMaxSize(200 * 1024 * 1024)
                                .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * Tests that ignite can start when caches' configurations with same name in different groups stored.
     *
     * @throws Exception If fails.
     */
    public void testStartDuplicatedCacheConfigurations() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(NODES);

        ig0.cluster().active(true);

        startCaches(ig0);

        DynamicCacheDescriptor desc = ig0.context().cache().cacheDescriptor(cacheName(3));

        storeInvalidCacheData(desc);

        stopAllGrids();

        startGrids(NODES);

        desc = ig0.context().cache().cacheDescriptor(cacheName(3));

        assertEquals("expected that group of " + cacheName(3) + " is " + EVEN_GROUP_NAME, EVEN_GROUP_NAME,
                desc.groupDescriptor().groupName());

    }

    /**
     * Check that cache_data.dat.tmp files are deleted after node restarts.
     *
     * @throws Exception If failed.
     */
    public void testTmpCacheConfigurationsDelete() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(NODES);

        ig0.cluster().active(true);

        startCaches(ig0);

        DynamicCacheDescriptor desc = ig0.context().cache().cacheDescriptor(cacheName(3));

        storeTmpCacheData(desc);

        stopAllGrids();

        startGrids(NODES);

        for (int i = 0; i < NODES; i++) {
            IgniteEx ig = grid(i);

            GridCacheSharedContext sharedCtx = ig.context().cache().context();

            FilePageStoreManager pageStore = (FilePageStoreManager) sharedCtx.pageStore();

            File[] tmpFile = pageStore.cacheWorkDir(true, ODD_GROUP_NAME).listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.endsWith(CACHE_DATA_TMP_FILENAME);
                }
            });

            assertNotNull(tmpFile);

            assertEquals(0, tmpFile.length);
        }
    }

    /**
     * Check that exception contains proper filename when trying to read corrupted cache configuration file.
     *
     * @throws Exception If failed.
     */
    public void testCorruptedCacheConfigurationsValidation() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(NODES);

        ig0.cluster().active(true);

        startCaches(ig0);

        DynamicCacheDescriptor desc = ig0.context().cache().cacheDescriptor(cacheName(2));

        corruptCacheData(desc);

        stopAllGrids();

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrids(NODES), IgniteCheckedException.class,
                desc.cacheName() + CACHE_DATA_FILENAME);
    }

    /**
     * Store cache descriptor to PDS with invalid group name.
     *
     * @param cacheDescr Cache descr.
     * @throws IgniteCheckedException If fails.
     */
    private void storeInvalidCacheData(DynamicCacheDescriptor cacheDescr) throws IgniteCheckedException {
        for (int i = 0; i < NODES; i++) {
            IgniteEx ig = grid(i);

            GridCacheSharedContext sharedCtx = ig.context().cache().context();

            FilePageStoreManager pageStore = (FilePageStoreManager) sharedCtx.pageStore();

            StoredCacheData corrData = cacheDescr.toStoredData();

            corrData.config().setGroupName(ODD_GROUP_NAME);

            pageStore.storeCacheData(corrData, true);
        }
    }

    /**
     * Store temp cache descriptor to PDS.
     *
     * @param cacheDescr Cache descr.
     * @throws IgniteCheckedException If fails.
     */
    private void storeTmpCacheData(DynamicCacheDescriptor cacheDescr) throws Exception {
        Marshaller marshaller = new JdkMarshaller();

        for (int i = 0; i < NODES; i++) {
            IgniteEx ig = grid(i);

            GridCacheSharedContext sharedCtx = ig.context().cache().context();

            FilePageStoreManager pageStore = (FilePageStoreManager) sharedCtx.pageStore();

            StoredCacheData data = cacheDescr.toStoredData();

            data.config().setGroupName(ODD_GROUP_NAME);

            File tmp = new File(pageStore.cacheWorkDir(true, ODD_GROUP_NAME), data.config().getName() + CACHE_DATA_TMP_FILENAME);

            try (OutputStream stream = new BufferedOutputStream(new FileOutputStream(tmp))) {
                marshaller.marshal(data, stream);
            }
        }
    }

    /**
     * Store temp cache descriptor to PDS.
     *
     * @param cacheDescr Cache descr.
     * @throws IgniteCheckedException If fails.
     */
    private void corruptCacheData(DynamicCacheDescriptor cacheDescr) throws Exception {
        for (int i = 0; i < NODES; i++) {
            IgniteEx ig = grid(i);

            GridCacheSharedContext sharedCtx = ig.context().cache().context();

            FilePageStoreManager pageStore = (FilePageStoreManager) sharedCtx.pageStore();

            StoredCacheData data = cacheDescr.toStoredData();

            data.config().setGroupName(ODD_GROUP_NAME);

            File config = new File(pageStore.cacheWorkDir(true, ODD_GROUP_NAME), data.config().getName() + CACHE_DATA_FILENAME);

            try (DataOutputStream os = new DataOutputStream(new FileOutputStream(config))) {
                os.writeLong(-1L);
            }

        }
    }

    /**
     * @param ignite Ignite instance.
     */
    private void startCaches(Ignite ignite) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++) {
            ccfg.add(new CacheConfiguration<>(cacheName(i))
                    .setGroupName(i % 2 == 0 ? ODD_GROUP_NAME : EVEN_GROUP_NAME)
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction(false, 32)));
        }

        ignite.createCaches(ccfg);
    }

    /**
     * Generate cache name from idx.
     *
     * @param idx Index.
     */
    private String cacheName(int idx) {
        return "cache" + idx;
    }
}
