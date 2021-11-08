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

package org.apache.ignite.internal.processors.diagnostic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.corruptedPagesFile;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.walDirs;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Class for testing diagnostics.
 */
public class DiagnosticProcessorTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER, false));
    }

    /**
     * Checks the correctness of the {@link DiagnosticProcessor#corruptedPagesFile}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedPagesFile() throws Exception {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), getName());

        try {
            int grpId = 10;
            long[] pageIds = {20, 40};

            File f = corruptedPagesFile(tmpDir.toPath(), new RandomAccessFileIOFactory(), grpId, pageIds);

            assertTrue(f.exists());
            assertTrue(f.isFile());
            assertTrue(f.length() > 0);
            assertTrue(Arrays.asList(tmpDir.listFiles()).contains(f));
            assertTrue(corruptedPagesFileNamePattern().matcher(f.getName()).matches());

            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                List<String> lines = br.lines().collect(toList());
                List<String> pageStrs = LongStream.of(pageIds).mapToObj(pageId -> grpId + ":" + pageId).collect(toList());

                assertEqualsCollections(lines, pageStrs);
            }
        }
        finally {
            if (tmpDir.exists())
                assertTrue(U.delete(tmpDir));
        }
    }

    /**
     * Checks the correctness of the {@link DiagnosticProcessor#walDirs}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWalDirs() throws Exception {
        IgniteEx n = startGrid(0);

        // Work + archive dirs.
        File[] expWalDirs = expWalDirs(n);
        assertEquals(2, expWalDirs.length);
        assertEqualsCollections(F.asList(expWalDirs), F.asList(walDirs(n.context())));

        stopAllGrids();
        cleanPersistenceDir();

        n = startGrid(0,
            (Consumer<IgniteConfiguration>)cfg -> cfg.getDataStorageConfiguration().setWalArchivePath(DFLT_WAL_PATH));

        // Only work dir.
        expWalDirs = expWalDirs(n);
        assertEquals(1, expWalDirs.length);
        assertEqualsCollections(F.asList(expWalDirs), F.asList(walDirs(n.context())));

        stopAllGrids();
        cleanPersistenceDir();

        n = startGrid(0,
            (Consumer<IgniteConfiguration>)cfg -> cfg.setDataStorageConfiguration(new DataStorageConfiguration()));

        // No wal dirs.
        assertNull(expWalDirs(n));
        assertNull(walDirs(n.context()));
    }

    /**
     * Check that when an CorruptedTreeException is thrown, a "corruptedPages_TIMESTAMP.txt"
     * will be created and a warning will be in the log.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOutputDiagnosticCorruptedPagesInfo() throws Exception {
        ListeningTestLogger listeningTestLog = new ListeningTestLogger(GridAbstractTest.log);

        IgniteEx n = startGrid(0, cfg -> {
            cfg.setGridLogger(listeningTestLog);
        });

        n.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        for (int i = 0; i < 10_000; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, "val_" + i);

        assertNotNull(n.context().diagnostic());

        T2<Integer, Long> anyPageId = findAnyPageId(n);
        assertNotNull(anyPageId);

        LogListener logLsnr = LogListener.matches("CorruptedTreeException has occurred. " +
            "To diagnose it, make a backup of the following directories: ").build();

        listeningTestLog.registerListener(logLsnr);

        n.context().failure().process(new FailureContext(FailureType.CRITICAL_ERROR,
            new CorruptedTreeException("Test ex", null, DEFAULT_CACHE_NAME, anyPageId.get1(), anyPageId.get2())));

        assertTrue(logLsnr.check());

        Path diagnosticPath = getFieldValue(n.context().diagnostic(), "diagnosticPath");

        List<File> corruptedPagesFiles = Arrays.stream(diagnosticPath.toFile().listFiles())
            .filter(f -> corruptedPagesFileNamePattern().matcher(f.getName()).matches()).collect(toList());

        assertEquals(1, corruptedPagesFiles.size());
        assertTrue(corruptedPagesFiles.get(0).length() > 0);
    }

    /**
     * Find first any page id for test.
     *
     * @param n Node.
     * @return Page id in WAL.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private T2<Integer, Long> findAnyPageId(IgniteEx n) throws IgniteCheckedException {
        try (WALIterator walIter = n.context().cache().context().wal().replay(new WALPointer(0, 0, 0))) {
            while (walIter.hasNextX()) {
                WALRecord walRecord = walIter.nextX().get2();

                if (walRecord instanceof PageSnapshot) {
                    PageSnapshot rec = (PageSnapshot)walRecord;

                    return new T2<>(rec.groupId(), rec.fullPageId().pageId());
                }
            }
        }

        return null;
    }

    /**
     * Getting expected WAL directories.
     *
     * @param n Node.
     * @return WAL directories.
     */
    @Nullable private File[] expWalDirs(IgniteEx n) {
        FileWriteAheadLogManager walMgr = walMgr(n);

        if (walMgr != null) {
            SegmentRouter sr = walMgr.getSegmentRouter();
            assertNotNull(sr);

            File workDir = sr.getWalWorkDir();
            return sr.hasArchive() ? F.asArray(workDir, sr.getWalArchiveDir()) : F.asArray(workDir);
        }

        return null;
    }

    /**
     * Getting pattern corrupted pages file name.
     *
     * @return Pattern.
     */
    private Pattern corruptedPagesFileNamePattern() {
        return Pattern.compile("corruptedPages_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}_\\d{3}\\.txt");
    }
}
