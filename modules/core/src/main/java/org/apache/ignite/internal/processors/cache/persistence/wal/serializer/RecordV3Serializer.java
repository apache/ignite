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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import org.apache.ignite.internal.pagemem.wal.record.FilteredRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;

/**
 * Record V3 serializer.
 */
public class RecordV3Serializer extends RecordV2Serializer {
    /**
     * Create an instance of Record V3 serializer.
     *
     * @param dataSerializer V3 data serializer.
     * @param marshalledMode Marshalled mode.
     * @param skipPositionCheck Skip position check mode.
     * @param recordFilter Record type filter. {@link FilteredRecord} is deserialized instead of original record.
     */
    public RecordV3Serializer(
        RecordDataV3Serializer dataSerializer,
        boolean writePointer,
        boolean marshalledMode,
        boolean skipPositionCheck,
        IgniteBiPredicate<RecordType, WALPointer> recordFilter
    ) {
        super(dataSerializer, writePointer, marshalledMode, skipPositionCheck, recordFilter);
    }

    /** {@inheritDoc} */
    @Override public int version() {
        return 3;
    }
}
