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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SimpleSegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.SegmentHeader;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticAction.PRINT_TO_FILE;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticAction.PRINT_TO_LOG;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DiagnosticAction.PRINT_TO_RAW_FILE;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.FILE_FORMAT;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.RAW_FILE_FORMAT;

/**
 *
 */
public class DiagnosticProcessorTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache0";

    /** Test directory for dump. */
    private static final String TEST_DUMP_PAGE_FILE = "testDumpPage";

    /** One time configured diagnosticProcessor. */
    private static DiagnosticProcessor diagnosticProcessor;

    /** One time configured page id for searching. */
    private static T2<Integer, Long> expectedPageId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();

        try {
            IgniteEx ignite = startGrid("node0");

            ignite.cluster().active(true);

            ignite.createCache(CACHE_NAME);
            try (IgniteDataStreamer<Integer, Integer> st = ignite.dataStreamer(CACHE_NAME)) {
                st.allowOverwrite(true);

                for (int i = 0; i < 10_000; i++)
                    st.addData(i, i);
            }

            diagnosticProcessor = ignite.context().diagnostic();
            expectedPageId = findAnyPageId();
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        diagnosticProcessor = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void dumpPageHistoryToDefaultDir() throws Exception {
        diagnosticProcessor.dumpPageHistory(new PageHistoryDiagnoster.DiagnosticPageBuilder()
            .pageIds(expectedPageId)
            .addAction(PRINT_TO_LOG)
            .addAction(PRINT_TO_FILE)
        );

        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER);
        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith(FILE_FORMAT))[0];

        List<String> records = Files.readAllLines(dumpFile.toPath());

        assertTrue(!records.isEmpty());

        assertTrue(records.stream().anyMatch(line -> line.contains("CheckpointRecord")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void dumpRawPageHistoryToDefaultDir() throws Exception {
        diagnosticProcessor.dumpPageHistory(new PageHistoryDiagnoster.DiagnosticPageBuilder()
            .pageIds(expectedPageId)
            .addAction(PRINT_TO_RAW_FILE)
        );

        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER);
        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith(RAW_FILE_FORMAT))[0];

        try (SegmentIO io = new SegmentIO(0L, new RandomAccessFileIO(dumpFile, StandardOpenOption.READ))) {
            SegmentHeader hdr = RecordV1Serializer.readSegmentHeader(io, new SimpleSegmentFileInputFactory());

            assertFalse(hdr.isCompacted());

            assertEquals(RecordSerializerFactory.LATEST_SERIALIZER_VERSION, hdr.getSerializerVersion());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void dumpPageHistoryToCustomAbsoluteDir() throws Exception {
        Path path = Paths.get(U.defaultWorkDirectory(), TEST_DUMP_PAGE_FILE);
        try {
            diagnosticProcessor.dumpPageHistory(new PageHistoryDiagnoster.DiagnosticPageBuilder()
                .pageIds(expectedPageId)
                .folderForDump(path.toFile())
                .addAction(PRINT_TO_FILE)
            );

            File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith(FILE_FORMAT))[0];

            List<String> records = Files.readAllLines(dumpFile.toPath());

            assertTrue(!records.isEmpty());

            assertTrue(records.stream().anyMatch(line -> line.contains("CheckpointRecord")));
        }
        finally {
            U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), TEST_DUMP_PAGE_FILE, false));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void dumpPageHistoryToCustomRelativeDir() throws Exception {
        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER, TEST_DUMP_PAGE_FILE);

        diagnosticProcessor.dumpPageHistory(new PageHistoryDiagnoster.DiagnosticPageBuilder()
            .pageIds(expectedPageId)
            .folderForDump(new File(TEST_DUMP_PAGE_FILE))
            .addAction(PRINT_TO_FILE)
        );

        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith(FILE_FORMAT))[0];

        List<String> records = Files.readAllLines(dumpFile.toPath());

        assertTrue(!records.isEmpty());

        assertTrue(records.stream().anyMatch(line -> line.contains("CheckpointRecord")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void dumpOnlyCheckpointRecordBecausePageIdNotSet() throws Exception {
        diagnosticProcessor.dumpPageHistory(new PageHistoryDiagnoster.DiagnosticPageBuilder()
            .addAction(PRINT_TO_LOG)
            .addAction(PRINT_TO_FILE)
        );

        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER);

        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith(FILE_FORMAT))[0];

        List<String> records = Files.readAllLines(dumpFile.toPath());

        assertTrue(records.stream().allMatch(line -> line.contains("CheckpointRecord")));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test(expected = NullPointerException.class)
    public void throwExceptionBecauseNotAnyActionsWasSet() throws IgniteCheckedException {
        diagnosticProcessor.dumpPageHistory(new PageHistoryDiagnoster.DiagnosticPageBuilder()
            .pageIds(expectedPageId)
        );
    }

    /**
     * Find first any page id for test.
     *
     * @return Page id in WAL.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    private T2<Integer, Long> findAnyPageId() throws org.apache.ignite.IgniteCheckedException {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

        try (WALIterator it = factory.iterator(new IteratorParametersBuilder().filesOrDirs(U.defaultWorkDirectory()))) {
            while (it.hasNext()) {
                WALRecord record = it.next().get2();

                if (record instanceof PageSnapshot) {
                    PageSnapshot rec = (PageSnapshot)record;

                    return new T2<>(rec.groupId(), rec.fullPageId().pageId());
                }
            }
        }

        throw new IgniteCheckedException();
    }
}
