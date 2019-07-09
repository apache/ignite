/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Arrays;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** */
public class WALRecordTest {

    /** */
    @Test
    public void testRecordTypeIndex() {
        RecordType[] recordTypes = RecordType.values();

        for (RecordType recordType : recordTypes)
            assertSame(recordType, RecordType.fromIndex(recordType.index()));

        int minIdx = Arrays.stream(recordTypes).mapToInt(RecordType::index).min().orElse(Integer.MIN_VALUE);

        assertTrue(minIdx >= 0);

        int maxIdx = Arrays.stream(recordTypes).mapToInt(RecordType::index).max().orElse(Integer.MAX_VALUE);

        assertTrue(maxIdx < 256);
    }
}