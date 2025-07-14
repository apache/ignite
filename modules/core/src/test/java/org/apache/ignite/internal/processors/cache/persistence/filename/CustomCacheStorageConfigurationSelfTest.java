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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.client.Config.SERVER;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 *
 */
public class CustomCacheStorageConfigurationSelfTest extends GridCommonAbstractTest {
    /** */
    private File myPath;

    /** */
    private File myPath2;

    /** */
    private File myPath3;


    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        myPath = new File(U.defaultWorkDirectory(), "my_path");
        myPath2 = new File(U.defaultWorkDirectory(), "my_path2");
        myPath3 = new File(U.defaultWorkDirectory(), "my_path3");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
        U.delete(myPath);
        U.delete(myPath2);
        U.delete(myPath3);
    }

    /** */
    @Test
    public void testDuplicatesStoragePathThrows() {
        assertThrows(
            log,
            () -> startGrid(new IgniteConfiguration().setDataStorageConfiguration(new DataStorageConfiguration()
                .setStoragePath(myPath.getAbsolutePath())
                .setExtraStoragePaths(myPath.getAbsolutePath()))),
            IgniteCheckedException.class,
            "DataStorageConfiguration contains duplicates"
        );

        assertThrows(
            log,
            () -> startGrid(new IgniteConfiguration().setDataStorageConfiguration(new DataStorageConfiguration()
                .setStoragePath(myPath.getAbsolutePath())
                .setExtraStoragePaths(myPath2.getAbsolutePath(), myPath.getAbsolutePath()))),
            IgniteCheckedException.class,
            "DataStorageConfiguration contains duplicates"
        );

        assertThrows(
            log,
            () -> startGrid(new IgniteConfiguration().setDataStorageConfiguration(new DataStorageConfiguration()
                .setStoragePath(myPath.getAbsolutePath())
                .setExtraStoragePaths(myPath2.getAbsolutePath(), myPath2.getAbsolutePath()))),
            IgniteCheckedException.class,
            "DataStorageConfiguration contains duplicates"
        );
    }

    /** */
    @Test
    public void testCacheUnknownStoragePathThrows() throws Exception {
        ConsumerX<IgniteEx> check = srv -> {
            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache").setStoragePaths("other")),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache").setIndexPath("other")),
                IgniteCheckedException.class
            );
        };

        try (IgniteEx srv = startGrid(new IgniteConfiguration().setDataStorageConfiguration(new DataStorageConfiguration()
                .setStoragePath(myPath.getAbsolutePath())
                .setExtraStoragePaths(myPath2.getAbsolutePath())
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))))) {
            srv.cluster().state(ClusterState.ACTIVE);
            check.accept(srv);
        }

        try (IgniteEx srv = startGrid(new IgniteConfiguration().setDataStorageConfiguration(new DataStorageConfiguration()
            .setExtraStoragePaths(myPath.getAbsolutePath(), myPath2.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))))) {
            srv.cluster().state(ClusterState.ACTIVE);

            check.accept(srv);
        }
    }

    /** */
    @Test
    public void testDifferentStoragePathForGroupThrows() throws Exception {
        ConsumerX<IgniteEx> check = srv -> {
            srv.createCache(new CacheConfiguration<>("my-cache")
                .setGroupName("grp")
                .setStoragePaths(myPath.getAbsolutePath()));

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache2")
                    .setGroupName("grp")
                    .setStoragePaths(myPath3.getAbsolutePath())),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache2")
                    .setGroupName("grp")
                    .setStoragePaths(myPath2.getAbsolutePath(), myPath3.getAbsolutePath())),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache2").setGroupName("grp")),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache2").setGroupName("grp")
                    .setStoragePaths(myPath3.getAbsolutePath())
                    .setIndexPath(myPath.getAbsolutePath())),
                IgniteCheckedException.class
            );

            srv.createCache(new CacheConfiguration<>("my-cache2")
                .setGroupName("grp-2"));

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache3")
                    .setGroupName("grp-2")
                    .setStoragePaths(myPath.getAbsolutePath())),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache3")
                    .setGroupName("grp-2")
                    .setStoragePaths(myPath.getAbsolutePath(), myPath2.getAbsolutePath())),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache3")
                    .setGroupName("grp-2")
                    .setIndexPath(myPath.getAbsolutePath())),
                IgniteCheckedException.class
            );

            srv.createCache(new CacheConfiguration<>("my-cache3")
                .setStoragePaths(myPath2.getAbsolutePath(), myPath3.getAbsolutePath())
                .setGroupName("grp-3"));

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache4")
                    .setGroupName("grp-3")
                    .setStoragePaths(myPath.getAbsolutePath(), myPath2.getAbsolutePath())),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache4")
                    .setGroupName("grp-3")
                    .setStoragePaths(myPath3.getAbsolutePath(), myPath2.getAbsolutePath())),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache4")
                    .setGroupName("grp-3")
                    .setStoragePaths(myPath2.getAbsolutePath(), myPath3.getAbsolutePath())
                    .setIndexPath(myPath.getAbsolutePath())),
                IgniteCheckedException.class
            );
        };

        try (IgniteEx srv = startGrid(new IgniteConfiguration().setDataStorageConfiguration(new DataStorageConfiguration()
            .setStoragePath(myPath.getAbsolutePath())
            .setExtraStoragePaths(myPath2.getAbsolutePath(), myPath3.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))))) {
            srv.cluster().state(ClusterState.ACTIVE);

            check.accept(srv);
        }

        try (IgniteEx srv = startGrid(new IgniteConfiguration().setDataStorageConfiguration(new DataStorageConfiguration()
            .setExtraStoragePaths(myPath.getAbsolutePath(), myPath2.getAbsolutePath(), myPath3.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))))) {
            srv.cluster().state(ClusterState.ACTIVE);

            check.accept(srv);
        }
    }

    /** */
    @Test
    public void testCreateCaches() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setStoragePath(myPath.getAbsolutePath())
            .setExtraStoragePaths(myPath2.getAbsolutePath(), myPath3.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        try (IgniteEx srv = startGrid(new IgniteConfiguration().setDataStorageConfiguration(dsCfg))) {
            srv.cluster().state(ClusterState.ACTIVE);

            srv.createCache(new CacheConfiguration<>("my-cache")
                .setGroupName("grp").setStoragePaths(myPath3.getAbsolutePath()));

            srv.createCache(new CacheConfiguration<>("my-cache2")
                .setGroupName("grp").setStoragePaths(myPath3.getAbsolutePath()));

            srv.createCache(new CacheConfiguration<>("my-cache3")
                .setGroupName("grp2").setStoragePaths(myPath3.getAbsolutePath())
                .setIndexPath(myPath.getAbsolutePath()));

            srv.createCache(new CacheConfiguration<>("my-cache4")
                .setGroupName("grp2").setStoragePaths(myPath3.getAbsolutePath())
                .setIndexPath(myPath.getAbsolutePath()));
        }
    }

    /** */
    @Test
    public void testClientNodeJoin() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setStoragePath(myPath.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setStoragePaths(myPath.getAbsolutePath())
            .setIndexPath(myPath.getAbsolutePath());

        try (IgniteEx srv = startGrid(getConfiguration("srv")
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(ccfg)
        )) {
            srv.cluster().state(ClusterState.ACTIVE);

            try (IgniteEx cliNode = startGrid(getConfiguration("client")
                .setClientMode(true)
                .setCacheConfiguration(ccfg)
            )) {
                assertTrue(cliNode.cacheNames().contains(DEFAULT_CACHE_NAME));
            }
        }
    }

    /** */
    @Test
    public void testDifferentStoragesInConfigAndStoredCacheData() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setStoragePath(myPath.getAbsolutePath())
            .setExtraStoragePaths(myPath2.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        int partCnt = 10;

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            // Start with two storages.
            .setStoragePaths(myPath.getAbsolutePath(), myPath2.getAbsolutePath())
            .setAffinity(new RendezvousAffinityFunction().setPartitions(partCnt));

        try (IgniteEx srv = startGrid(getConfiguration("srv")
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(ccfg))) {
            srv.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> c = srv.cache(DEFAULT_CACHE_NAME);

            IntStream.range(0, 100).forEach(i -> c.put(i, i));
        }

        // Set one storage in IgniteConfiguration.
        ccfg.setStoragePaths(myPath2.getAbsolutePath());

        try (IgniteEx srv = startGrid(getConfiguration("srv")
            .setDataStorageConfiguration(dsCfg)
            .setCacheConfiguration(ccfg))) {
            srv.cluster().state(ClusterState.ACTIVE);

            assertEquals(2, srv.cachex(DEFAULT_CACHE_NAME).configuration().getStoragePaths().length);

            IgniteCache<Integer, Integer> c = srv.cache(DEFAULT_CACHE_NAME);

            IntStream.range(0, 100).forEach(i -> assertEquals((Integer)i, c.get(i)));
            IntStream.range(0, 100).forEach(i -> c.put(i, i * 2));

            NodeFileTree ft = srv.context().pdsFolderResolver().fileTree();
            Set<Integer> parts = new HashSet<>();

            for (File cs : ft.cacheStorages(srv.cachex(DEFAULT_CACHE_NAME).configuration())) {
                for (File partFile : cs.listFiles(NodeFileTree::binFile))
                    assertTrue(parts.add(NodeFileTree.partId(partFile)));
            }

            // Extra file for index partition.
            assertEquals(partCnt + 1, parts.size());
            assertTrue(parts.contains(INDEX_PARTITION));
            assertTrue(IntStream.range(0, partCnt).boxed().allMatch(parts::contains));
        }
    }

    /** */
    @Test
    public void testThinClientCreation() throws Exception {
        IgniteConfiguration cfg = getConfiguration("srv")
            .setDataStorageConfiguration(new DataStorageConfiguration()
            .setStoragePath(myPath.getAbsolutePath())
            .setExtraStoragePaths(myPath2.getAbsolutePath(), myPath3.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        try (IgniteEx srv = startGrid(cfg)) {
            srv.cluster().state(ClusterState.ACTIVE);

            try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(SERVER))) {
                Consumer<ClientCacheConfiguration> check = initCfg -> {
                    ClientCache<Integer, Integer> c = cli.createCache(initCfg);

                    CacheConfiguration<?, ?> srvCfg = srv.cachex(initCfg.getName()).configuration();
                    ClientCacheConfiguration cliCfg = cli.cache(initCfg.getName()).getConfiguration();

                    assertEquals(0, F.compareArrays(initCfg.getStoragePaths(), srvCfg.getStoragePaths()));
                    assertEquals(0, F.compareArrays(initCfg.getStoragePaths(), cliCfg.getStoragePaths()));

                    assertEquals(initCfg.getIndexPath(), srvCfg.getIndexPath());
                    assertEquals(initCfg.getIndexPath(), cliCfg.getIndexPath());

                    IntStream.range(0, 100).forEach(i -> c.put(i, i));
                    IntStream.range(0, 100).forEach(i -> assertEquals((Integer)i, c.get(i)));
                };

                check.accept(new ClientCacheConfiguration()
                    .setName("c0"));

                check.accept(new ClientCacheConfiguration()
                    .setName("c1")
                    .setStoragePaths(myPath.getAbsolutePath()));

                check.accept(new ClientCacheConfiguration()
                    .setName("c2")
                    .setStoragePaths(myPath.getAbsolutePath(), myPath2.getAbsolutePath()));

                check.accept(new ClientCacheConfiguration()
                    .setName("c3")
                    .setIndexPath(myPath.getAbsolutePath()));

                check.accept(new ClientCacheConfiguration()
                    .setName("c4")
                    .setStoragePaths(myPath.getAbsolutePath())
                    .setIndexPath(myPath2.getAbsolutePath()));

                check.accept(new ClientCacheConfiguration()
                    .setName("c5")
                    .setStoragePaths(myPath.getAbsolutePath(), myPath2.getAbsolutePath())
                    .setIndexPath(myPath3.getAbsolutePath()));
            }
        }
    }
}
