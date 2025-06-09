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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
    public void testDuplicatesStoragePathThrows() throws Exception {
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
        try (IgniteEx srv = startGrid(new IgniteConfiguration().setDataStorageConfiguration(new DataStorageConfiguration()
                .setStoragePath(myPath.getAbsolutePath())
                .setExtraStoragePaths(myPath2.getAbsolutePath())
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))))) {
            srv.cluster().state(ClusterState.ACTIVE);

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache").setStoragePath("other")),
                IgniteCheckedException.class
            );
        }
    }

    /** */
    @Test
    public void testDifferentStoragePathForGroupThrows() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setStoragePath(myPath.getAbsolutePath())
            .setExtraStoragePaths(myPath2.getAbsolutePath(), myPath3.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        try (IgniteEx srv = startGrid(new IgniteConfiguration().setDataStorageConfiguration(dsCfg))) {
            srv.cluster().state(ClusterState.ACTIVE);

            srv.createCache(new CacheConfiguration<>("my-cache")
                    .setGroupName("grp")
                    .setStoragePath(myPath.getAbsolutePath()));

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache2")
                    .setGroupName("grp")
                    .setStoragePath(myPath3.getAbsolutePath())),
                IgniteCheckedException.class
            );

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache2").setGroupName("grp")),
                IgniteCheckedException.class
            );

            srv.createCache(new CacheConfiguration<>("my-cache2")
                .setGroupName("grp-2"));

            assertThrowsWithCause(
                () -> srv.createCache(new CacheConfiguration<>("my-cache3")
                    .setGroupName("grp-2")
                    .setStoragePath(myPath.getAbsolutePath())),
                IgniteCheckedException.class
            );
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
                .setGroupName("grp").setStoragePath(myPath3.getAbsolutePath()));

            srv.createCache(new CacheConfiguration<>("my-cache2")
                .setGroupName("grp").setStoragePath(myPath3.getAbsolutePath()));
        }
    }

    /** */
    @Test
    public void testClientNodeJoin() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setStoragePath(myPath.getAbsolutePath())
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setStoragePath(myPath.getAbsolutePath());

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
}
