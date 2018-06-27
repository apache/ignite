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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.WALDisableContext;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.Files.walkFileTree;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CP_FILE_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.FILE_TMP_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.META_STORAGE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_TEMP_NAME_PATTERN;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/***
 *
 */
public class IgniteCrashDuringDisableWALTest extends GridCommonAbstractTest {
    /** */
    public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setAutoActivationEnabled(false);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        for (CrashPoint crashPoint : CrashPoint.values()) {
            testCrashWithDisableWAL(crashPoint);

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /**
     * @param crashPoint Crash point.
     * @throws Exception If failed.
     */
    private void testCrashWithDisableWAL(CrashPoint crashPoint) throws Exception {
        log.info("Start test crash " + crashPoint);

        IgniteEx ig0 = startGrid(0);

        GridCacheSharedContext<Object, Object> sharedContext = ig0.context().cache().context();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)sharedContext.database();

        WALDisableContext walDisableContext = new WALDisableContext(dbMgr, sharedContext.wal(), sharedContext.pageStore()) {
            @Override protected void writeMetaStoreDisableWALFlag() throws IgniteCheckedException {
                if (crashPoint == CrashPoint.BEFORE_WRITE_KEY_TO_META_STORE)
                    failNode(crashPoint);

                super.writeMetaStoreDisableWALFlag();

                if (crashPoint == CrashPoint.AFTER_WRITE_KEY_TO_META_STORE)
                    failNode(crashPoint);
            }

            @Override protected void removeMetaStoreDisableWALFlag() throws IgniteCheckedException {
                if (crashPoint == CrashPoint.AFTER_CHECKPOINT_AFTER_ENABLE_WAL)
                    failNode(crashPoint);

                super.removeMetaStoreDisableWALFlag();

                if (crashPoint == CrashPoint.AFTER_REMOVE_KEY_TO_META_STORE)
                    failNode(crashPoint);
            }

            @Override protected void disableWAL(boolean disable) throws IgniteCheckedException {
                if (disable) {
                    if (crashPoint == CrashPoint.AFTER_CHECKPOINT_BEFORE_DISABLE_WAL)
                        failNode(crashPoint);

                    super.disableWAL(disable);

                    if (crashPoint == CrashPoint.AFTER_DISABLE_WAL)
                        failNode(crashPoint);

                }
                else {
                    super.disableWAL(disable);

                    if (crashPoint == CrashPoint.AFTER_ENABLE_WAL)
                        failNode(crashPoint);
                }
            }
        };

        setFieldValue(dbMgr, "walDisableContext", walDisableContext);

        ig0.context().internalSubscriptionProcessor().registerMetastorageListener(walDisableContext);

        ig0.cluster().active(true);

        try (IgniteDataStreamer<Integer, Integer> st = ig0.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < 10_000; i++)
                st.addData(i, -i);
        }

        boolean fail = false;

        try (WALIterator it = sharedContext.wal().replay(null)) {
            dbMgr.applyUpdatesOnRecovery(it, (tup) -> true, (entry) -> true, new HashMap<>());
        }
        catch (IgniteCheckedException e) {
            if (crashPoint.needCleanUp)
                fail = true;
        }

        Assert.assertEquals(crashPoint.needCleanUp, fail);

        Ignite ig1 = startGrid(0);

        String msg = crashPoint.toString();

        if (crashPoint.needCleanUp) {
            PdsFoldersResolver foldersResolver = ((IgniteEx)ig1).context().pdsFolderResolver();

            File root = foldersResolver.resolveFolders().persistentStoreRootPath();

            walkFileTree(root.toPath(), new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    String name = path.toFile().getName();

                    String filePath = path.toString();

                    if (path.toFile().getParentFile().getName().equals(META_STORAGE_NAME))
                        return CONTINUE;

                    if (WAL_NAME_PATTERN.matcher(name).matches() || WAL_TEMP_NAME_PATTERN.matcher(name).matches())
                        return CONTINUE;

                    boolean failed = false;

                    if (name.endsWith(FILE_TMP_SUFFIX))
                        failed = true;

                    if (CP_FILE_NAME_PATTERN.matcher(name).matches())
                        failed = true;

                    if (name.startsWith(PART_FILE_PREFIX))
                        failed = true;

                    if (name.startsWith(INDEX_FILE_NAME))
                        failed = true;

                    if (failed)
                        fail(msg + " " + filePath);

                    return CONTINUE;
                }
            });
        }
    }

    /**
     * @param crashPoint Crash point.
     * @throws IgniteCheckedException Always throws exception.
     */
    private void failNode(CrashPoint crashPoint) throws IgniteCheckedException {
        stopGrid(0, true);

        throw new IgniteCheckedException(crashPoint.toString());
    }

    /**
     * Crash point.
     */
    private enum CrashPoint {
        BEFORE_WRITE_KEY_TO_META_STORE(false),
        AFTER_WRITE_KEY_TO_META_STORE(true),
        AFTER_CHECKPOINT_BEFORE_DISABLE_WAL(true),
        AFTER_DISABLE_WAL(true),
        AFTER_ENABLE_WAL(true),
        AFTER_CHECKPOINT_AFTER_ENABLE_WAL(true),
        AFTER_REMOVE_KEY_TO_META_STORE(false);

        /** Clean up flag. */
        private final boolean needCleanUp;

        CrashPoint(boolean up) {
            needCleanUp = up;
        }
    }
}
