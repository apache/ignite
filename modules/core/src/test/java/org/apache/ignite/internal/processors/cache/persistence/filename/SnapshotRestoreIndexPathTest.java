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
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.filename.AbstractDataRegionRelativeStoragePathTest.consId;

/** Check snapshot restore when {@link CacheConfiguration#getIndexPath()} equals to some of {@link CacheConfiguration#getStoragePaths()}. */
public class SnapshotRestoreIndexPathTest extends GridCommonAbstractTest {
    /** */
    private boolean defaultStoragePath;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setStoragePath(path("path1"))
            .setExtraStoragePaths(path("path2"), path("path3"), path("path4"));

        dsCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true);

        return cfg
            .setConsistentId(consId(cfg))
            .setDataStorageConfiguration(dsCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.delete(new File(U.defaultWorkDirectory()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.delete(new File(U.defaultWorkDirectory()));
    }

    /** */
    @Test
    public void testSameIndexAndExtraStorage() throws Exception {
        defaultStoragePath = false;
        doTest();
    }

    /** */
    @Test
    public void testSameIndexAndDefaultStorage() throws Exception {
        defaultStoragePath = true;
        doTest();
    }

    /** */
    private void doTest() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        if (defaultStoragePath) {
            ccfg.setIndexPath(path("path1"));
        }
        else {
            ccfg.setStoragePaths(path("path1"), path("path2"), path("path3"), path("path4"))
                .setIndexPath(path("path2"));
        }

        IgniteCache<Integer, Integer> c = srv.createCache(ccfg);

        IntStream.range(0, 100).forEach(i -> c.put(i, i));

        srv.snapshot().createSnapshot("snapshot").get();

        stopAllGrids();

        cleanPersistenceDir(true);
        U.delete(new File(path("path1")));
        U.delete(new File(path("path2"), "db"));
        U.delete(new File(path("path3"), "db"));
        U.delete(new File(path("path4"), "db"));

        srv = startGrid(0);

        srv.cluster().state(ClusterState.ACTIVE);

        srv.snapshot().restoreSnapshot("snapshot", null).get();

        IgniteCache<Integer, Integer> c2 = srv.cache(DEFAULT_CACHE_NAME);

        IntStream.range(0, 100).boxed().forEach(i -> assertEquals(i, c2.get(i)));
    }

    /** */
    public String path(String folder) throws IgniteCheckedException {
        return new File(new File(U.defaultWorkDirectory()), folder).getAbsolutePath();
    }
}
