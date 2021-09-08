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

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.Arrays;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import org.apache.ignite.testframework.wal.record.RecordUtils;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
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

    /** */
    @Test
    public void testAllTestWalRecordBuilderConfigured() {
        RecordType[] recordTypes = RecordType.values();

        for (RecordType recordType : recordTypes)
            assertNotNull(
                "Test's builder of WAL record with type '" + recordType + "' not found. " +
                    "Please, add such builder to RecordUtils for test purposes.",
                RecordUtils.buildWalRecord(recordType)
            );
    }
}
