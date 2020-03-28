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
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.WalStateManager.WALDisableContext;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.Files.walkFileTree;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CP_FILE_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.META_STORAGE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_TEMP_NAME_PATTERN;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/***
 *
 */
@RunWith(Parameterized.class)
public class IgniteNodeStoppedDuringDisableWALTest extends GridCommonAbstractTest {
    /** Crash point. */
    private NodeStopPoint nodeStopPoint;

    /**
     * Default constructor to avoid BeforeFirstAndAfterLastTestRule.
     */
    private IgniteNodeStoppedDuringDisableWALTest() {
    }

    /**
     * @param nodeStopPoint Crash point.
     */
    public IgniteNodeStoppedDuringDisableWALTest(NodeStopPoint nodeStopPoint) {
        this.nodeStopPoint = nodeStopPoint;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setAutoActivationEnabled(false);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * Test checks that after WAL is globally disabled and node is stopped, persistent store is cleaned properly after node restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-12040",
            MvccFeatureChecker.forcedMvcc() && nodeStopPoint == NodeStopPoint.AFTER_DISABLE_WAL);

        testStopNodeWithDisableWAL(nodeStopPoint);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param nodeStopPoint Stop point.
     * @throws Exception If failed.
     */
    private void testStopNodeWithDisableWAL(NodeStopPoint nodeStopPoint) throws Exception {
        log.info("Start test crash " + nodeStopPoint);

        IgniteEx ig0 = startGrid(0);

        GridCacheSharedContext<Object, Object> sharedContext = ig0.context().cache().context();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)sharedContext.database();
        IgniteWriteAheadLogManager WALmgr = sharedContext.wal();

        WALDisableContext walDisableContext = new WALDisableContext(dbMgr, sharedContext.pageStore(), log) {
            @Override protected void writeMetaStoreDisableWALFlag() throws IgniteCheckedException {
                if (nodeStopPoint == NodeStopPoint.BEFORE_WRITE_KEY_TO_META_STORE)
                    failNode(nodeStopPoint);

                super.writeMetaStoreDisableWALFlag();

                if (nodeStopPoint == NodeStopPoint.AFTER_WRITE_KEY_TO_META_STORE)
                    failNode(nodeStopPoint);
            }

            @Override protected void removeMetaStoreDisableWALFlag() throws IgniteCheckedException {
                if (nodeStopPoint == NodeStopPoint.AFTER_CHECKPOINT_AFTER_ENABLE_WAL)
                    failNode(nodeStopPoint);

                super.removeMetaStoreDisableWALFlag();

                if (nodeStopPoint == NodeStopPoint.AFTER_REMOVE_KEY_TO_META_STORE)
                    failNode(nodeStopPoint);
            }

            @Override protected void disableWAL(boolean disable) throws IgniteCheckedException {
                if (disable) {
                    if (nodeStopPoint == NodeStopPoint.AFTER_CHECKPOINT_BEFORE_DISABLE_WAL)
                        failNode(nodeStopPoint);

                    super.disableWAL(disable);

                    if (nodeStopPoint == NodeStopPoint.AFTER_DISABLE_WAL)
                        failNode(nodeStopPoint);

                }
                else {
                    super.disableWAL(disable);

                    if (nodeStopPoint == NodeStopPoint.AFTER_ENABLE_WAL)
                        failNode(nodeStopPoint);
                }
            }
        };

        setFieldValue(sharedContext.walState(), "walDisableContext", walDisableContext);

        setFieldValue(WALmgr, "walDisableContext", walDisableContext);

        ig0.context().internalSubscriptionProcessor().registerMetastorageListener(walDisableContext);

        ig0.cluster().active(true);

        try (IgniteDataStreamer<Integer, Integer> st = ig0.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < GridTestUtils.SF.apply(10_000); i++)
                st.addData(i, -i);
        }

        boolean fail = false;

        try (WALIterator it = sharedContext.wal().replay(null)) {
            dbMgr.applyUpdatesOnRecovery(it, (ptr, rec) -> true, (entry) -> true);
        }
        catch (IgniteCheckedException e) {
            if (nodeStopPoint.needCleanUp)
                fail = true;
        }

        Assert.assertEquals(nodeStopPoint.needCleanUp, fail);

        Ignite ig1 = startGrid(0);

        String msg = nodeStopPoint.toString();

        int pageSize = ig1.configuration().getDataStorageConfiguration().getPageSize();

        if (nodeStopPoint.needCleanUp) {
            PdsFoldersResolver foldersResolver = ((IgniteEx)ig1).context().pdsFolderResolver();

            File root = foldersResolver.resolveFolders().persistentStoreRootPath();

            walkFileTree(root.toPath(), new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    String name = path.toFile().getName();

                    String filePath = path.toString();

                    String parentDirName = path.toFile().getParentFile().getName();

                    if (parentDirName.equals(META_STORAGE_NAME) || parentDirName.equals(TxLog.TX_LOG_CACHE_NAME))
                        return CONTINUE;

                    if (WAL_NAME_PATTERN.matcher(name).matches() || WAL_TEMP_NAME_PATTERN.matcher(name).matches())
                        return CONTINUE;

                    boolean failed = false;

                    if (name.endsWith(FilePageStoreManager.TMP_SUFFIX))
                        failed = true;

                    if (CP_FILE_NAME_PATTERN.matcher(name).matches())
                        failed = true;

                    if (name.startsWith(PART_FILE_PREFIX) && path.toFile().length() > pageSize)
                        failed = true;

                    if (name.startsWith(INDEX_FILE_NAME) && path.toFile().length() > pageSize)
                        failed = true;

                    if (failed)
                        fail(msg + " " + filePath + " " + path.toFile().length());

                    return CONTINUE;
                }
            });
        }
    }

    /**
     * @param nodeStopPoint Stop point.
     * @throws IgniteCheckedException Always throws exception.
     */
    private void failNode(NodeStopPoint nodeStopPoint) throws IgniteCheckedException {
        stopGrid(0, true);

        throw new IgniteCheckedException(nodeStopPoint.toString());
    }

    /**
     * @return Node stop point.
     */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> providedTestData() {
        return Arrays.stream(NodeStopPoint.values()).map(it -> new Object[] {it}).collect(Collectors.toList());
    }

    /**
     * Crash point.
     */
    private enum NodeStopPoint {
        BEFORE_WRITE_KEY_TO_META_STORE(false),
        AFTER_WRITE_KEY_TO_META_STORE(true),
        AFTER_CHECKPOINT_BEFORE_DISABLE_WAL(true),
        AFTER_DISABLE_WAL(true),
        AFTER_ENABLE_WAL(true),
        AFTER_CHECKPOINT_AFTER_ENABLE_WAL(true),
        AFTER_REMOVE_KEY_TO_META_STORE(false);

        /** Clean up flag. */
        private final boolean needCleanUp;

        NodeStopPoint(boolean up) {
            needCleanUp = up;
        }
    }
}
