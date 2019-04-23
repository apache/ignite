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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CHECKPOINT_RECORD;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class FilteredWalIteratorTest {

    private FileWALPointer ZERO_POINTER = new FileWALPointer(0, 0, 0);
    private IgniteBiTuple<WALPointer, WALRecord> TEST_RECORD = new IgniteBiTuple<>(ZERO_POINTER, new MetastoreDataRecord("key", new byte[0]));

    @Test
    public void onNext() throws IgniteCheckedException {
        WALIterator mockedIterator = Mockito.mock(WALIterator.class);

        CheckpointRecord expectedRecord = new CheckpointRecord(new FileWALPointer(5738, 0, 0));

        List<IgniteBiTuple<FileWALPointer, ? extends WALRecord>> records = Arrays.asList(
            new IgniteBiTuple<>(ZERO_POINTER, new MetastoreDataRecord("key", new byte[0])),
            new IgniteBiTuple<>(ZERO_POINTER, expectedRecord),
            new IgniteBiTuple<>(ZERO_POINTER, new PageSnapshot(new FullPageId(1, 1), new byte[0], 10)),
            new IgniteBiTuple<>(ZERO_POINTER, new PageSnapshot(new FullPageId(2, 1), new byte[0], 10))
        );

        Boolean[] hasNextReturn = new Boolean[records.size() + 1];
        Arrays.fill(hasNextReturn, true);
        hasNextReturn[records.size()] = false;

        for (int i = 0; i < records.size(); i++) {
            Collections.shuffle(records);

            when(mockedIterator.hasNext()).thenReturn(true, hasNextReturn);
            when(mockedIterator.next()).thenReturn(TEST_RECORD, records.toArray(new IgniteBiTuple[] {}));
        }

        FilteredWalIterator iterator = new FilteredWalIterator(
            mockedIterator,
            (record) -> record.get2().type() == CHECKPOINT_RECORD
        );

        WALRecord ans = null;

        try (WALIterator it = iterator) {
            while (it.hasNext()) {
                if (ans != null)
                    fail("Should be only one value");

                ans = it.next().get2();
            }
        }

        assertNotNull(ans);
        assertEquals(ans, expectedRecord);
    }

}