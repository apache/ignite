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

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.PHYSICAL;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.EXCHANGE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.HEADER_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.METASTORE_DATA_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PAGE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.PART_META_UPDATE_STATE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.WalScannerTest.dummyPage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/**
 *
 */
@RunWith(Parameterized.class)
public class FilteredWalIteratorTest {
    /** Count of different records sequence per filter. */
    private static final int ITERATORS_COUNT_PER_FILTER = 20;

    /** Count of records on one iterator. */
    private static final int RECORDS_COUNT_IN_ITERATOR = 30;

    /** **/
    private static Random random = new Random();

    /** **/
    private static FileWALPointer ZERO_POINTER = new FileWALPointer(0, 0, 0);

    /** **/
    private static IgniteBiTuple<WALPointer, WALRecord> TEST_RECORD = new IgniteBiTuple<>(
        ZERO_POINTER, new MetastoreDataRecord("key", new byte[0])
    );

    /** Customized iterator for test. */
    private WALIterator mockedIter;

    /** Iterator filter for test. */
    private Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter;

    /** Expected result for iterator and filter. */
    private List<IgniteBiTuple<WALPointer, WALRecord>> expRes;

    /**
     * @param nameOfCase Case name. It required only for printing test name.
     * @param mockedIter Basic WAL iterator.
     * @param filter Filter by which record should be filtered.
     * @param expRes Expected record result.
     */
    public FilteredWalIteratorTest(
        String nameOfCase,
        WALIterator mockedIter,
        Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter,
        List<IgniteBiTuple<WALPointer, WALRecord>> expRes) {
        this.mockedIter = mockedIter;
        this.filter = filter;
        this.expRes = expRes;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void shouldReturnCorrectlyFilteredRecords() throws IgniteCheckedException {
        FilteredWalIterator filteredIter = new FilteredWalIterator(mockedIter, filter);

        List<IgniteBiTuple<WALPointer, WALRecord>> ans = new ArrayList<>();
        try (WALIterator it = filteredIter) {
            while (it.hasNext())
                ans.add(it.next());
        }

        assertNotNull(ans);
        assertEquals(expRes, ans);
    }

    /**
     * @return Datas for test.
     */
    @Parameterized.Parameters(name = "{0} case №{index}")
    public static Iterable<Object[]> providedTestData() {
        ArrayList<Object[]> res = new ArrayList<>();

        res.addAll(prepareTestCaseData("PhysicalFilter", r -> r.get2().type().purpose() == PHYSICAL));
        res.addAll(prepareTestCaseData("CheckpointFilter", r -> r.get2() instanceof CheckpointRecord));

        return res;
    }

    /**
     * Prepare bunch of data given filter.
     *
     * @param testCaseName Human readable name of filter.
     * @param filter Filter for test.
     * @return Prepared data.
     */
    private static List<Object[]> prepareTestCaseData(
        String testCaseName,
        Predicate<IgniteBiTuple<WALPointer, WALRecord>> filter
    ) {
        ArrayList<Object[]> res = new ArrayList<>(ITERATORS_COUNT_PER_FILTER);

        Boolean[] hasNextReturn = new Boolean[RECORDS_COUNT_IN_ITERATOR + 1];
        Arrays.fill(hasNextReturn, true);
        hasNextReturn[RECORDS_COUNT_IN_ITERATOR] = false;

        for (int i = 0; i < ITERATORS_COUNT_PER_FILTER; i++) {
            List<IgniteBiTuple<WALPointer, WALRecord>> tuples = randomRecords();

            WALIterator mockedIter = Mockito.mock(WALIterator.class);
            when(mockedIter.hasNext()).thenReturn(true, hasNextReturn);
            when(mockedIter.next()).thenReturn(TEST_RECORD, tuples.toArray(new IgniteBiTuple[] {}));

            res.add(new Object[] {testCaseName, mockedIter, filter, tuples.stream().filter(filter).collect(toList())});
        }

        return res;
    }

    /**
     * @return Random records list for iteration.
     */
    private static List<IgniteBiTuple<WALPointer, WALRecord>> randomRecords() {
        ArrayList<IgniteBiTuple<WALPointer, WALRecord>> res = new ArrayList<>(RECORDS_COUNT_IN_ITERATOR);

        for (int i = 0; i < RECORDS_COUNT_IN_ITERATOR; i++)
            res.add(randomRecord());

        return res;
    }

    /**
     * @return Random test record.
     */
    private static IgniteBiTuple<WALPointer, WALRecord> randomRecord() {
        int recordId = random.nextInt(9);

        switch (recordId) {
            case 0:
                return new IgniteBiTuple<>(ZERO_POINTER, new MetastoreDataRecord("key", new byte[0]));
            case 1:
                return new IgniteBiTuple<>(ZERO_POINTER, new CheckpointRecord(new FileWALPointer(5738, 0, 0)));
            case 2:
                return new IgniteBiTuple<>(ZERO_POINTER, new PageSnapshot(new FullPageId(1, 1), dummyPage(1024, 1), 1024));
            case 3:
                return new IgniteBiTuple<>(ZERO_POINTER, new PartitionMetaStateRecord(1, 1, OWNING, 1));
            case 4:
                return new IgniteBiTuple<>(ZERO_POINTER, new CustomizeTypeRecord(METASTORE_DATA_RECORD));
            case 5:
                return new IgniteBiTuple<>(ZERO_POINTER, new CustomizeTypeRecord(PAGE_RECORD));
            case 6:
                return new IgniteBiTuple<>(ZERO_POINTER, new CustomizeTypeRecord(PART_META_UPDATE_STATE));
            case 7:
                return new IgniteBiTuple<>(ZERO_POINTER, new CustomizeTypeRecord(HEADER_RECORD));
            case 8:
                return new IgniteBiTuple<>(ZERO_POINTER, new CustomizeTypeRecord(EXCHANGE));
        }

        return null;
    }

    /**
     * Test class for represent record with different type.
     */
    public static class CustomizeTypeRecord extends WALRecord {
        /** **/
        private final RecordType type;

        /**
         * @param type Custom type for this record.
         */
        public CustomizeTypeRecord(RecordType type) {
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public RecordType type() {
            return type;
        }
    }
}
