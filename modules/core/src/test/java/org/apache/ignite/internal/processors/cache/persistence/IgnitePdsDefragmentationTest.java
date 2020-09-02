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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.DEFRAGMENTATION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/** */
public class IgnitePdsDefragmentationTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_2_NAME = "cache2";

    /** */
    public static final int PARTS = 5;

    /** */
    public static final int ADDED_KEYS_COUNT = 10;

    /** */
    private static final String GRP_NAME = "group";

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

    private static class PolicyFactory implements Factory<ExpiryPolicy> {
        @Override public ExpiryPolicy create() {
            return new ExpiryPolicy() {
                @Override public Duration getExpiryForCreation() {
                    return new Duration(TimeUnit.MILLISECONDS, 13000);
                }

                /** {@inheritDoc} */
                @Override public Duration getExpiryForAccess() {
                    return new Duration(TimeUnit.MILLISECONDS, 13000);
                }

                /** {@inheritDoc} */
                @Override public Duration getExpiryForUpdate() {
                    return new Duration(TimeUnit.MILLISECONDS, 13000);
                }
            };
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);

        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setInitialSize(100L * 1024 * 1024)
                .setMaxSize(1024L * 1024 * 1024)
                .setPersistenceEnabled(true)
        );

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<?, ?> cache1Cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setGroupName(GRP_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS));

        CacheConfiguration<?, ?> cache2Cfg = new CacheConfiguration<>(CACHE_2_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setGroupName(GRP_NAME)
            .setExpiryPolicyFactory(new PolicyFactory())
            .setAffinity(new RendezvousAffinityFunction(false, PARTS));

        cfg.setCacheConfiguration(cache1Cfg, cache2Cfg);

        return cfg;
    }

    /** */
    @Test
    public void testEssentials() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        ig.context().cache().context().database()
            .forceCheckpoint("test")
            .futureFor(CheckpointState.FINISHED)
            .get();

        stopGrid(0);

        File dbWorkDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);
        File nodeWorkDir = new File(dbWorkDir, U.maskForFileName(ig.name()));
        File workDir = new File(nodeWorkDir, FilePageStoreManager.CACHE_GRP_DIR_PREFIX + GRP_NAME);

        long[] oldPartLen = partitionSizes(workDir);

        long oldIdxFileLen = new File(workDir, FilePageStoreManager.INDEX_FILE_NAME).length();

        System.setProperty(DEFRAGMENTATION, "true");

        try {
            startGrid(0);
        }
        finally {
            System.clearProperty(DEFRAGMENTATION);
        }

        awaitPartitionMapExchange();

        long[] newPartLen = partitionSizes(workDir);

        for (int p = 0; p < PARTS; p++)
            assertTrue(newPartLen[p] < oldPartLen[p]);

        long newIdxFileLen = new File(workDir, FilePageStoreManager.INDEX_FILE_NAME).length();

        assertTrue(newIdxFileLen <= oldIdxFileLen);

        File completionMarkerFile = DefragmentationFileUtils.defragmentationCompletionMarkerFile(workDir);
        assertTrue(completionMarkerFile.exists());

        stopGrid(0);

        IgniteCache<Object, Object> cache = startGrid(0).cache(DEFAULT_CACHE_NAME);

        assertFalse(completionMarkerFile.exists());

        for (int k = 0; k < ADDED_KEYS_COUNT; k++)
            cache.get(k);
    }

    /** */
    public long[] partitionSizes(File workDir) {
        return IntStream.range(0, PARTS)
            .mapToObj(p -> new File(workDir, String.format(FilePageStoreManager.PART_FILE_TEMPLATE, p)))
            .mapToLong(File::length)
            .toArray();
    }

    /** */
    @Test
    public void testDefragmentedPartitionCreated() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        fillCache(ig.getOrCreateCache(CACHE_2_NAME));

        stopGrid(0);

        System.setProperty(DEFRAGMENTATION, "true");

        try {
            startGrid(0);
        }
        finally {
            System.clearProperty(DEFRAGMENTATION);
        }

        awaitPartitionMapExchange();

        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        AtomicReference<File> cachePartFile = new AtomicReference<>();
        AtomicReference<File> defragCachePartFile = new AtomicReference<>();

        Files.walkFileTree(workDir.toPath(), new FileVisitor<Path>() {
            @Override public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                if (path.toString().contains("cacheGroup-group")) {
                    File file = path.toFile();

                    if (file.getName().contains("part-dfrg-"))
                        cachePartFile.set(file);
                    else if (file.getName().contains("part-"))
                        defragCachePartFile.set(file);
                }

                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });

        assertNull(cachePartFile.get());
        assertNotNull(defragCachePartFile.get());
    }

    /** */
    private void fillCache(IgniteCache<Integer, Object> cache) {
        for (int i = 0; i < ADDED_KEYS_COUNT; i++) {
            byte[] val = new byte[8192];
            new Random().nextBytes(val);

            cache.put(i, val);
        }

        for (int i = 0; i < ADDED_KEYS_COUNT / 2; i++)
            cache.remove(i * 2);
    }
}
