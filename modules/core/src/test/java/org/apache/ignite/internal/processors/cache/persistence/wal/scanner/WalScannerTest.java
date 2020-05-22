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

package org.apache.ignite.internal.processors.cache.persistence.wal.scanner;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixCountRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.processors.cache.persistence.DummyPageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static junit.framework.TestCase.assertTrue;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder.withIteratorParameters;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printToFile;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.printToLog;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.WalScanner.buildWalScanner;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class WalScannerTest {
    /** **/
    private static final String TEST_DUMP_FILE = "output.txt";

    /** **/
    private static FileWALPointer ZERO_POINTER = new FileWALPointer(0, 0, 0);

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldFindCorrectRecords() throws Exception {
        // given: Iterator with random value and value which should be find by scanner.
        long expPageId = 984;
        int grpId = 123;

        PageSnapshot expPageSnapshot = new PageSnapshot(new FullPageId(expPageId, grpId), dummyPage(1024, expPageId), 1024);
        CheckpointRecord expCheckpoint = new CheckpointRecord(new FileWALPointer(5738, 0, 0));
        FixCountRecord expDeltaPage = new FixCountRecord(grpId, expPageId, 4);

        WALIterator mockedIter = mockWalIterator(
            new IgniteBiTuple<>(ZERO_POINTER, expPageSnapshot),
            new IgniteBiTuple<>(ZERO_POINTER, new PageSnapshot(new FullPageId(455, grpId), dummyPage(4096, 455), 1024)),
            new IgniteBiTuple<>(ZERO_POINTER, expCheckpoint),
            new IgniteBiTuple<>(ZERO_POINTER, new MetastoreDataRecord("key", new byte[0])),
            new IgniteBiTuple<>(ZERO_POINTER, new PartitionMetaStateRecord(grpId, 1, OWNING, 1)),
            new IgniteBiTuple<>(ZERO_POINTER, expDeltaPage),
            new IgniteBiTuple<>(ZERO_POINTER, new FixCountRecord(grpId, 98348, 4))
        );

        IgniteWalIteratorFactory mockedFactory = mock(IgniteWalIteratorFactory.class);
        when(mockedFactory.iterator(any(IteratorParametersBuilder.class))).thenReturn(mockedIter);

        // Test scanner handler for holding found value instead of printing its.
        List<WALRecord> holder = new ArrayList<>();
        ScannerHandler recordCaptor = (rec) -> holder.add(rec.get2());

        Set<T2<Integer, Long>> groupAndPageIds = new HashSet<>();

        groupAndPageIds.add(new T2<>(grpId, expPageId));

        // when: Scanning WAL for searching expected page.
        buildWalScanner(withIteratorParameters(), mockedFactory)
            .findAllRecordsFor(groupAndPageIds)
            .forEach(recordCaptor);

        // then: Should be find only expected value.
        assertEquals(holder.size(), 3);

        assertEquals(expPageSnapshot, holder.get(0));
        assertEquals(expCheckpoint, holder.get(1));
        assertEquals(expDeltaPage, holder.get(2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldFindCorrectRecordsForMoreThanOnePages() throws Exception {
        // given: Iterator with random value and value which should be find by scanner with several ids.
        long expPageId1 = 984;
        long expPageId2 = 9584;
        long expPageId3 = 98344;

        int grpId = 123;

        PageSnapshot expPageSnapshot = new PageSnapshot(new FullPageId(expPageId1, grpId), dummyPage(1024, expPageId1), 1024);
        CheckpointRecord expCheckpoint = new CheckpointRecord(new FileWALPointer(5738, 0, 0));
        FixCountRecord expDeltaPage1 = new FixCountRecord(grpId, expPageId2, 4);
        FixCountRecord expDeltaPage2 = new FixCountRecord(grpId, expPageId3, 4);

        WALIterator mockedIter = mockWalIterator(
            new IgniteBiTuple<>(ZERO_POINTER, expPageSnapshot),
            new IgniteBiTuple<>(ZERO_POINTER, new PageSnapshot(new FullPageId(455, grpId), dummyPage(1024, 455), 1024)),
            new IgniteBiTuple<>(ZERO_POINTER, expCheckpoint),
            new IgniteBiTuple<>(ZERO_POINTER, new MetastoreDataRecord("key", new byte[0])),
            new IgniteBiTuple<>(ZERO_POINTER, new PartitionMetaStateRecord(grpId, 1, OWNING, 1)),
            new IgniteBiTuple<>(ZERO_POINTER, expDeltaPage1),
            new IgniteBiTuple<>(ZERO_POINTER, new FixCountRecord(grpId, 98348, 4)),
            new IgniteBiTuple<>(ZERO_POINTER, new PartitionMetaStateRecord(grpId, 1, OWNING, 1)),
            new IgniteBiTuple<>(ZERO_POINTER, expDeltaPage2)
        );

        IgniteWalIteratorFactory mockedFactory = mock(IgniteWalIteratorFactory.class);
        when(mockedFactory.iterator(any(IteratorParametersBuilder.class))).thenReturn(mockedIter);

        List<WALRecord> holder = new ArrayList<>();
        ScannerHandler recordCaptor = (rec) -> holder.add(rec.get2());

        Set<T2<Integer, Long>> groupAndPageIds = new HashSet<>();

        groupAndPageIds.add(new T2<>(grpId, expPageId1));
        groupAndPageIds.add(new T2<>(grpId, expPageId2));
        groupAndPageIds.add(new T2<>(grpId, expPageId3));

        // when: Scanning WAL for searching expected page.
        buildWalScanner(withIteratorParameters(), mockedFactory)
            .findAllRecordsFor(groupAndPageIds)
            .forEach(recordCaptor);

        // then: Should be find only expected value.
        assertEquals(4, holder.size());

        assertEquals(expPageSnapshot, holder.get(0));
        assertEquals(expCheckpoint, holder.get(1));
        assertEquals(expDeltaPage1, holder.get(2));
        assertEquals(expDeltaPage2, holder.get(3));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldDumpToLogFoundRecord() throws Exception {
        // given: Test logger for interception of logging.
        long expPageId = 984;
        int grpId = 123;

        IgniteLogger log = mock(IgniteLogger.class);

        when(log.isInfoEnabled()).thenReturn(true);

        ArgumentCaptor<String> valCapture = ArgumentCaptor.forClass(String.class);
        doNothing().when(log).info(valCapture.capture());

        WALIterator mockedIter = mockWalIterator(
            new IgniteBiTuple<>(ZERO_POINTER, new PageSnapshot(new FullPageId(expPageId, grpId), dummyPage(1024, expPageId), 1024)),
            new IgniteBiTuple<>(ZERO_POINTER, new CheckpointRecord(new FileWALPointer(5738, 0, 0))),
            new IgniteBiTuple<>(ZERO_POINTER, new FixCountRecord(grpId, expPageId, 4))
        );

        IgniteWalIteratorFactory factory = mock(IgniteWalIteratorFactory.class);
        when(factory.iterator(any(IteratorParametersBuilder.class))).thenReturn(mockedIter);

        Set<T2<Integer, Long>> groupAndPageIds = new HashSet<>();

        groupAndPageIds.add(new T2<>(grpId, expPageId));

        // when: Scanning WAL for searching expected page.
        buildWalScanner(withIteratorParameters(), factory)
            .findAllRecordsFor(groupAndPageIds)
            .forEach(printToLog(log));

        // then: Should be find only expected value from log.
        List<String> actualRecords = valCapture.getAllValues();

        assertEquals(actualRecords.size(), 1);

        assertRecord(actualRecords.get(0), "PageSnapshot [", "PAGE_RECORD");
        assertRecord(actualRecords.get(0), "CheckpointRecord [", "CHECKPOINT_RECORD");
        assertRecord(actualRecords.get(0), "FixCountRecord [", "BTREE_FIX_COUNT");
    }

    /**
     * @param actual Actual value to check.
     * @param oneOfExpected One of expected value.
     */
    private static void assertRecord(String actual, String... oneOfExpected) {
        assertTrue(actual, Arrays.stream(oneOfExpected).anyMatch(actual::contains));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldDumpToFileFoundRecord() throws Exception {
        // given: File for dumping records.
        File targetFile = Paths.get(U.defaultWorkDirectory(), TEST_DUMP_FILE).toFile();

        long expectedPageId = 984;
        int grpId = 123;

        WALIterator mockedIter = mockWalIterator(
            new IgniteBiTuple<>(ZERO_POINTER, new PageSnapshot(new FullPageId(expectedPageId, grpId), dummyPage(1024, expectedPageId), 1024)),
            new IgniteBiTuple<>(ZERO_POINTER, new CheckpointRecord(new FileWALPointer(5738, 0, 0))),
            new IgniteBiTuple<>(ZERO_POINTER, new FixCountRecord(grpId, expectedPageId, 4))
        );

        IgniteWalIteratorFactory factory = mock(IgniteWalIteratorFactory.class);
        when(factory.iterator(any(IteratorParametersBuilder.class))).thenReturn(mockedIter);

        Set<T2<Integer, Long>> groupAndPageIds = new HashSet<>();

        groupAndPageIds.add(new T2<>(grpId, expectedPageId));

        List<String> actualRecords;

        try {
            // when: Scanning WAL for searching expected page.
            buildWalScanner(withIteratorParameters(), factory)
                .findAllRecordsFor(groupAndPageIds)
                .forEach(printToFile(targetFile));

            actualRecords = Files.readAllLines(targetFile.toPath());
        }
        finally {
            targetFile.delete();
        }

        // then: Should be find only expected value from file. PageSnapshot string representation is 11 lines long.
        assertEquals(13, actualRecords.size());

        assertTrue(actualRecords.get(0), actualRecords.get(0).contains("PageSnapshot ["));
        assertTrue(actualRecords.get(11), actualRecords.get(11).contains("CheckpointRecord ["));
        assertTrue(actualRecords.get(12), actualRecords.get(12).contains("FixCountRecord ["));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldDumpToFileAndLogFoundRecord() throws Exception {
        // given: File for dumping records and test logger for interception of records.
        File targetFile = Paths.get(U.defaultWorkDirectory(), TEST_DUMP_FILE).toFile();

        long expPageId = 984;
        int grpId = 123;

        IgniteLogger log = mock(IgniteLogger.class);

        when(log.isInfoEnabled()).thenReturn(true);

        ArgumentCaptor<String> valCapture = ArgumentCaptor.forClass(String.class);
        doNothing().when(log).info(valCapture.capture());

        WALIterator mockedIter = mockWalIterator(
            new IgniteBiTuple<>(ZERO_POINTER, new PageSnapshot(new FullPageId(expPageId, grpId), dummyPage(1024, expPageId), 1024)),
            new IgniteBiTuple<>(ZERO_POINTER, new CheckpointRecord(new FileWALPointer(5738, 0, 0))),
            new IgniteBiTuple<>(ZERO_POINTER, new FixCountRecord(grpId, expPageId, 4))
        );

        IgniteWalIteratorFactory factory = mock(IgniteWalIteratorFactory.class);
        when(factory.iterator(any(IteratorParametersBuilder.class))).thenReturn(mockedIter);

        Set<T2<Integer, Long>> groupAndPageIds = new HashSet<>();

        groupAndPageIds.add(new T2<>(grpId, expPageId));

        List<String> actualFileRecords = null;

        try {
            // when: Scanning WAL for searching expected page.
            buildWalScanner(withIteratorParameters(), factory)
                .findAllRecordsFor(groupAndPageIds)
                .forEach(printToLog(log).andThen(printToFile(targetFile)));

            actualFileRecords = Files.readAllLines(targetFile.toPath());
        }
        finally {
            targetFile.delete();
        }

        actualFileRecords = actualFileRecords.stream()
            .filter(it -> it.startsWith("Next WAL record ::"))
            .collect(Collectors.toList());

        // then: Should be find only expected value from file.
        assertEquals(actualFileRecords.size(), 3);

        assertTrue(actualFileRecords.get(0), actualFileRecords.get(0).contains("PageSnapshot ["));
        assertTrue(actualFileRecords.get(1), actualFileRecords.get(1).contains("CheckpointRecord ["));
        assertTrue(actualFileRecords.get(2), actualFileRecords.get(2).contains("FixCountRecord ["));

        // then: Should be find only expected value from log.
        List<String> actualLogRecords = valCapture.getAllValues();

        assertEquals(actualLogRecords.size(), 1);

        assertTrue(actualLogRecords.get(0), actualLogRecords.get(0).contains("PageSnapshot ["));
        assertTrue(actualLogRecords.get(0), actualLogRecords.get(0).contains("CheckpointRecord ["));
        assertTrue(actualLogRecords.get(0), actualLogRecords.get(0).contains("FixCountRecord ["));
    }

    /**
     * @param first Not null first value for return.
     * @param tail Other values.
     * @return Mocked WAL iterator.
     */
    private WALIterator mockWalIterator(
        IgniteBiTuple<WALPointer, WALRecord> first,
        IgniteBiTuple<WALPointer, WALRecord>... tail
    ) {
        Boolean[] hasNextReturn = new Boolean[tail.length + 1];
        Arrays.fill(hasNextReturn, true);
        hasNextReturn[tail.length] = false;

        WALIterator mockedIter = mock(WALIterator.class);
        when(mockedIter.hasNext()).thenReturn(true, hasNextReturn);

        when(mockedIter.next()).thenReturn(first, tail);

        return mockedIter;
    }

    /** */
    public static byte[] dummyPage(int pageSize, long pageId) {
        ByteBuffer pageBuf = ByteBuffer.allocateDirect(pageSize);

        DummyPageIO.VERSIONS.latest().initNewPage(GridUnsafe.bufferAddress(pageBuf), pageId, pageSize);

        byte[] pageData = new byte[pageSize];

        pageBuf.get(pageData);

        GridUnsafe.cleanDirectBuffer(pageBuf);

        return pageData;
    }
}
