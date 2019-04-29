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

package org.apache.ignite.internal.processors.maintain;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.maintain.DebugProcessor.DEFAULT_TARGET_FOLDER;
import static org.apache.ignite.internal.processors.maintain.DebugProcessor.DebugAction.PRINT_TO_FILE;
import static org.apache.ignite.internal.processors.maintain.DebugProcessor.DebugAction.PRINT_TO_LOG;

/**
 *
 */
public class DebugProcessorTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache0";
    /** Test directory for dump. */
    private static final String TEST_DUMP_PAGE_FILE = "testDumpPage";

    /** One time configured debugProcessor. */
    private static DebugProcessor debugProcessor;
    /** One time configured page id for searching. */
    private static long expectedPageId;

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

            debugProcessor = ignite.context().debug();
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void dumpPageHistoryToDefaultDir() throws Exception {
        debugProcessor.dumpPageHistory(new DebugProcessor.DebugPageBuilder()
            .pageIds(expectedPageId)
            .addAction(PRINT_TO_LOG)
            .addAction(PRINT_TO_FILE)
        );

        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER);
        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith("txt"))[0];

        List<String> records = Files.readAllLines(dumpFile.toPath());

        assertTrue(records.size() > 0);

        assertTrue(records.stream().anyMatch(line -> line.contains("PageSnapshot")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void dumpPageHistoryToCustomAbsoluteDir() throws Exception {
        Path path = Paths.get(U.defaultWorkDirectory(), TEST_DUMP_PAGE_FILE);
        try {
            debugProcessor.dumpPageHistory(new DebugProcessor.DebugPageBuilder()
                .pageIds(expectedPageId)
                .folderForDump(path.toFile())
                .addAction(PRINT_TO_FILE)
            );

            File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith("txt"))[0];

            List<String> records = Files.readAllLines(dumpFile.toPath());

            assertTrue(records.size() > 0);

            assertTrue(records.stream().anyMatch(line -> line.contains("PageSnapshot")));
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

        debugProcessor.dumpPageHistory(new DebugProcessor.DebugPageBuilder()
            .pageIds(expectedPageId)
            .folderForDump(new File(TEST_DUMP_PAGE_FILE))
            .addAction(PRINT_TO_FILE)
        );

        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith("txt"))[0];

        List<String> records = Files.readAllLines(dumpFile.toPath());

        assertTrue(records.size() > 0);

        assertTrue(records.stream().anyMatch(line -> line.contains("PageSnapshot")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void dumpOnlyCheckpointRecordBecausePageIdNotSet() throws Exception {
        debugProcessor.dumpPageHistory(new DebugProcessor.DebugPageBuilder()
            .addAction(PRINT_TO_LOG)
            .addAction(PRINT_TO_FILE)
        );

        Path path = Paths.get(U.defaultWorkDirectory(), DEFAULT_TARGET_FOLDER);

        File dumpFile = path.toFile().listFiles((dir, name) -> name.endsWith("txt"))[0];

        List<String> records = Files.readAllLines(dumpFile.toPath());

        assertTrue(records.stream().allMatch(line -> line.contains("CheckpointRecord")));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test(expected = NullPointerException.class)
    public void throwExceptionBecauseNotAnyActionsWasSet() throws IgniteCheckedException {
        debugProcessor.dumpPageHistory(new DebugProcessor.DebugPageBuilder()
            .pageIds(expectedPageId)
        );
    }

    /**
     * Find first any page id for test.
     *
     * @return Page id in WAL.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    private long findAnyPageId() throws org.apache.ignite.IgniteCheckedException {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory();

        try (WALIterator it = factory.iterator(new IteratorParametersBuilder().filesOrDirs(U.defaultWorkDirectory()))) {
            while (it.hasNext()) {
                WALRecord record = it.next().get2();

                if (record instanceof PageSnapshot)
                    return ((PageSnapshot)record).fullPageId().pageId();
            }
        }

        return 0;
    }
}