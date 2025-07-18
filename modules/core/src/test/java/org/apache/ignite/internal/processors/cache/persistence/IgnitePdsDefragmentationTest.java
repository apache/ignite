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
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStoreCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.filename.CacheFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters.toStore;

/** */
public class IgnitePdsDefragmentationTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_2_NAME = "cache2";

    /** */
    public static final int PARTS = 5;

    /** */
    public static final int ADDED_KEYS_COUNT = 1500;

    /** */
    protected static final String GRP_NAME = "group";

    /** Defragmentation pool size. If < 1, default value is used. */
    private int defragPoolSize;

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
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** */
    protected static class PolicyFactory implements Factory<ExpiryPolicy> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
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

        if (defragPoolSize > 0)
            dsCfg.setDefragmentationThreadPoolSize(defragPoolSize);

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

    /**
     * Tests basic derfragmentation.
     *
     * @throws Exception If failed.
     * @see #checkSuccessfulDefragmentation()
     */
    @Test
    public void testSuccessfulDefragmentation() throws Exception {
        checkSuccessfulDefragmentation();
    }

    /**
     * Tests basic derfragmentation with one thread.
     *
     * @throws Exception If failed.
     * @see #checkSuccessfulDefragmentation()
     */
    @Test
    public void testSuccessfulDefragmentationOneThread() throws Exception {
        defragPoolSize = 1;

        checkSuccessfulDefragmentation();
    }

    /**
     * Basic test scenario. Does following steps:
     *  - Start node;
     *  - Fill cache;
     *  - Remove part of data;
     *  - Stop node;
     *  - Start node in defragmentation mode;
     *  - Stop node;
     *  - Start node;
     *  - Check that partitions became smaller;
     *  - Check that cache is accessible and works just fine.
     *
     * @throws Exception If failed.
     */
    private void checkSuccessfulDefragmentation() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        forceCheckpoint(ig);

        createMaintenanceRecord();

        NodeFileTree ft = ig.context().pdsFolderResolver().fileTree();

        CacheConfiguration<?, ?> ccfg = ig.cachex(DEFAULT_CACHE_NAME).configuration();

        stopGrid(0);

        CacheFileTree cft = ft.cacheTree(ccfg);

        long[] oldPartLen = partitionSizes(ft, ccfg);

        long oldIdxFileLen = ft.partitionFile(ccfg, INDEX_PARTITION).length();

        startGrid(0);

        waitForDefragmentation(0);

        assertEquals(ClusterState.INACTIVE, grid(0).context().state().clusterState().state());

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                grid(0).cluster().state(ClusterState.ACTIVE);

                return null;
            },
            IgniteCheckedException.class,
            "Failed to activate cluster (node is in maintenance mode)"
        );

        long[] newPartLen = partitionSizes(ft, ccfg);

        for (int p = 0; p < PARTS; p++)
            assertTrue(newPartLen[p] < oldPartLen[p]);

        long newIdxFileLen = ft.partitionFile(ccfg, INDEX_PARTITION).length();

        assertTrue(newIdxFileLen <= oldIdxFileLen);

        File completionMarkerFile = cft.defragmentationCompletionMarkerFile();
        assertTrue(completionMarkerFile.exists());

        stopGrid(0);

        IgniteEx ig0 = startGrid(0);

        ig0.cluster().state(ClusterState.ACTIVE);

        assertFalse(completionMarkerFile.exists());

        validateCache(grid(0).cache(DEFAULT_CACHE_NAME));

        validateLeftovers(cft);
    }

    /** */
    protected long[] partitionSizes(CacheGroupContext grp) {
        final int grpId = grp.groupId();

        return IntStream.concat(
            IntStream.of(INDEX_PARTITION),
            IntStream.range(0, grp.shared().affinity().affinity(grpId).partitions())
        ).mapToLong(p -> {
            try {
                final FilePageStore store = (FilePageStore)((PageStoreCollection)grp.shared().pageStore()).getStore(grpId, p);

                return new File(store.getFileAbsolutePath()).length();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }).toArray();
    }

    /**
     * Force checkpoint and wait for it so all partitions will be in their final state after restart if no more data is
     * uploaded.
     *
     * @param ig Ignite node.
     * @throws IgniteCheckedException If checkpoint failed for some reason.
     */
    private void forceCheckpoint(IgniteEx ig) throws IgniteCheckedException {
        ig.context().cache().context().database()
            .forceCheckpoint("testDefrag")
            .futureFor(CheckpointState.FINISHED)
            .get();
    }

    /** */
    protected void waitForDefragmentation(int idx) throws IgniteCheckedException {
        IgniteEx ig = grid(idx);

        ((GridCacheDatabaseSharedManager)ig.context().cache().context().database())
            .defragmentationManager()
            .completionFuture()
            .get();
    }

    /** */
    protected void createMaintenanceRecord(String... cacheNames) throws IgniteCheckedException {
        IgniteEx grid = grid(0);

        MaintenanceRegistry mntcReg = grid.context().maintenanceRegistry();

        final List<String> caches = new ArrayList<>();

        caches.add(DEFAULT_CACHE_NAME);

        if (cacheNames != null && cacheNames.length != 0)
            caches.addAll(Arrays.asList(cacheNames));

        mntcReg.registerMaintenanceTask(toStore(caches));
    }

    /**
     * Returns array that contains sizes of partition files in gived working directories. Assumes that partitions
     * {@code 0} to {@code PARTS - 1} exist in that dir.
     *
     * @param ft Node file tree.
     * @param ccfg Cache configuration to check
     * @return The array.
     */
    protected long[] partitionSizes(NodeFileTree ft, CacheConfiguration<?, ?> ccfg) {
        return IntStream.range(0, PARTS)
            .mapToObj(p -> ft.partitionFile(ccfg, p))
            .mapToLong(File::length)
            .toArray();
    }

    /**
     * Checks that plain node start after failed defragmentation will finish batch renaming.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverRestartWithoutDefragmentation() throws Exception {
        testFailover(cft -> {
            try {
                NodeFileTree ft = GridTestUtils.getFieldValue(cft, "ft");

                File mntcRecFile = ft.maintenanceFile();

                assertTrue(mntcRecFile.exists());

                U.delete(mntcRecFile);

                startGrid(0);

                validateLeftovers(cft);
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }
            finally {
                createMaintenanceRecord();

                stopGrid(0);
            }
        });
    }

    /**
     * Checks that second start in defragmentation mode will finish defragmentation if no completion marker was found.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverOnLastStage() throws Exception {
        testFailover(cft -> {});
    }

    /**
     * Checks that second start in defragmentation mode will finish defragmentation if index was not defragmented.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverIncompletedIndex() throws Exception {
        testFailover(cft -> move(
            cft.defragmentedIndexFile(),
            cft.defragmentedIndexTmpFile()
        ));
    }

    /**
     * Checks that second start in defragmentation mode will finish defragmentation if partition was not defragmented.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverIncompletedPartition1() throws Exception {
        testFailover(cft -> {
            cft.defragmentedIndexFile().delete();

            move(
                cft.defragmentedPartFile(PARTS - 1),
                cft.defragmentedPartTmpFile(PARTS - 1)
            );
        });
    }

    /**
     * Checks that second start in defragmentation mode will finish defragmentation if no mapping was found for partition.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailoverIncompletedPartition2() throws Exception {
        testFailover(cft -> {
            cft.defragmentedIndexFile().delete();

            cft.defragmentedPartMappingFile(PARTS - 1).delete();
        });
    }

    /** */
    private void move(File from, File to) throws IgniteCheckedException {
        try {
            Files.move(from.toPath(), to.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** */
    private void testFailover(IgniteThrowableConsumer<CacheFileTree> c) throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        forceCheckpoint(ig);

        createMaintenanceRecord();

        CacheFileTree cft = ig.context().pdsFolderResolver().fileTree().cacheTree(ig.cachex(DEFAULT_CACHE_NAME).configuration());

        stopGrid(0);

        //Defragmentation should fail when node starts.
        startAndAwaitNodeFail(cft);

        c.accept(cft);

        startGrid(0); // Fails here VERY rarely. WTF?

        waitForDefragmentation(0);

        stopGrid(0);

        // Everything must be completed.
        startGrid(0).cluster().state(ClusterState.ACTIVE);

        validateCache(grid(0).cache(DEFAULT_CACHE_NAME));

        validateLeftovers(cft);
    }

    /**
     * @throws IgniteInterruptedCheckedException If fail.
     */
    private void startAndAwaitNodeFail(CacheFileTree cft) throws IgniteInterruptedCheckedException {
        String errMsg = "Failed to create defragmentation completion marker.";

        AtomicBoolean errOccurred = new AtomicBoolean();

        UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
            DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

            FileIOFactory delegate = dsCfg.getFileIOFactory();
            File marker = cft.defragmentationCompletionMarkerFile();

            dsCfg.setFileIOFactory((file, modes) -> {
                if (file.equals(marker)) {
                    errOccurred.set(true);

                    throw new IOException(errMsg);
                }

                return delegate.create(file, modes);
            });

            return cfg;
        };

        AtomicBoolean nodeStopped = new AtomicBoolean();
        IgnitionListener nodeStopListener = (name, state) -> {
            if (name.equals(getTestIgniteInstanceName(0)) && state == IgniteState.STOPPED_ON_FAILURE)
                nodeStopped.set(true);
        };

        Ignition.addListener(nodeStopListener);
        try {
            try {
                startGrid(0, cfgOp);
            }
            catch (Exception ignore) {
                // No-op.
            }

            // Failed node can leave interrupted status of the thread that needs to be cleared,
            // otherwise following "wait" wouldn't work.
            // This call can't be moved inside of "catch" block because interruption can actually be silent.
            Thread.interrupted();

            assertTrue(GridTestUtils.waitForCondition(errOccurred::get, 3_000L));
            assertTrue(GridTestUtils.waitForCondition(nodeStopped::get, 3_000L));
        }
        finally {
            Ignition.removeListener(nodeStopListener);
        }
    }

    /** */
    public void validateLeftovers(CacheFileTree cft) {
        assertFalse(cft.defragmentedIndexFile().exists());

        for (int p = 0; p < PARTS; p++) {
            assertFalse(cft.defragmentedPartMappingFile(p).exists());

            assertFalse(cft.defragmentedPartFile(p).exists());
        }
    }

    /** */
    @Test
    public void testDefragmentedPartitionCreated() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        fillCache(ig.cache(DEFAULT_CACHE_NAME));

        fillCache(ig.getOrCreateCache(CACHE_2_NAME));

        createMaintenanceRecord();

        NodeFileTree ft = ig.context().pdsFolderResolver().fileTree();

        String grpDirName = ft.defaultCacheStorage(ig.cachex(DEFAULT_CACHE_NAME).configuration()).getName();

        stopGrid(0);

        startGrid(0);

        waitForDefragmentation(0);

        AtomicReference<File> cachePartFile = new AtomicReference<>();
        AtomicReference<File> defragCachePartFile = new AtomicReference<>();

        for (Path s : ft.allStorages().map(File::toPath).collect(Collectors.toList())) {
            Files.walkFileTree(s, new FileVisitor<Path>() {
                @Override public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                    if (path.toString().contains(grpDirName)) {
                        File file = path.toFile();

                        if (file.getName().contains("part-dfrg-"))
                            cachePartFile.set(file);
                        else if (NodeFileTree.partitionFile(file))
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
        }

        assertNull(cachePartFile.get()); //TODO Fails.
        assertNotNull(defragCachePartFile.get());
    }

    /**
     * Fill cache using integer keys.
     *
     * @param cache
     */
    protected void fillCache(IgniteCache<Integer, Object> cache) {
        fillCache(Function.identity(), cache);
    }

    /** */
    protected <T> void fillCache(Function<Integer, T> keyMapper, IgniteCache<T, Object> cache) {
        try (IgniteDataStreamer<T, Object> ds = grid(0).dataStreamer(cache.getName())) {
            for (int i = 0; i < ADDED_KEYS_COUNT; i++) {
                byte[] val = new byte[8192];
                new Random().nextBytes(val);

                ds.addData(keyMapper.apply(i), val);
            }
        }

        try (IgniteDataStreamer<T, Object> ds = grid(0).dataStreamer(cache.getName())) {
            ds.allowOverwrite(true);

            for (int i = 0; i <= ADDED_KEYS_COUNT / 2; i++)
                ds.removeData(keyMapper.apply(i * 2));
        }
    }

    /** */
    public void validateCache(IgniteCache<Object, Object> cache) {
        for (int k = 0; k < ADDED_KEYS_COUNT; k++) {
            Object val = cache.get(k);

            if (k % 2 == 0)
                assertNull(val);
            else
                assertNotNull(val);
        }
    }

    /**
     * Start node, wait for defragmentation and validate that sizes of caches are less than those before the defragmentation.
     * @param gridId Idx of ignite grid.
     * @param groups Cache groups to check.
     * @throws Exception If failed.
     */
    protected void defragmentAndValidateSizesDecreasedAfterDefragmentation(int gridId, CacheGroupContext... groups) throws Exception {
        for (CacheGroupContext grp : groups) {
            final long[] oldPartLen = partitionSizes(grp);

            startGrid(0);

            waitForDefragmentation(0);

            stopGrid(0);

            final long[] newPartLen = partitionSizes(grp);

            boolean atLeastOneSmaller = false;

            for (int p = 0; p < oldPartLen.length; p++) {
                assertTrue(newPartLen[p] <= oldPartLen[p]);

                if (newPartLen[p] < oldPartLen[p])
                    atLeastOneSmaller = true;
            }

            assertTrue(atLeastOneSmaller);
        }
    }
}
