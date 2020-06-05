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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

public class IgnitePdsDefragmentationTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 1;

    /** */
    public static final int ADDED_KEYS_COUNT = 500;

    /** */
    public static final int REMOVED_KEYS_COUNT = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration().
            setInitialSize(100L * 1024 * 1024).
            setMaxSize(200L * 1024 * 1024).
            setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        return cfg;
    }

    /** */
    @Test
    public void testDefragmentedPartitionCreated() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        stopGrid(0);

        String defrag = "DEFRAGMENTATION";

        System.setProperty(defrag, "true");

        try {
            startGrid(0);
        }
        finally {
            System.clearProperty(defrag);
        }

        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        AtomicReference<File> cachePartFile = new AtomicReference<>();
        AtomicReference<File> defragCachePartFile = new AtomicReference<>();

        Files.walkFileTree(workDir.toPath(), new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                if (path.toString().contains(DEFAULT_CACHE_NAME)) {
                    if (path.toFile().getName().contains("part-dfrg-"))
                        cachePartFile.set(path.toFile());
                    else if (path.toFile().getName().contains("part-"))
                        defragCachePartFile.set(path.toFile());
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });

        assertNotNull(cachePartFile.get());
        assertNotNull(defragCachePartFile.get());
    }

    /** */
    private void fillCache(IgniteCache<Integer,Integer> cache) {
        Map kvs = new HashMap(ADDED_KEYS_COUNT);

        for (int i = 0; i < ADDED_KEYS_COUNT; i++) {
            byte[] val = new byte[8192];
            new Random().nextBytes(val);

            kvs.put(i, val);
        }

        cache.putAll(kvs);

        for (int i = 0; i < REMOVED_KEYS_COUNT; i++)
            cache.remove(new Random().nextInt(ADDED_KEYS_COUNT));
    }
}
