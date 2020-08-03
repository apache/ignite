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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class RecordSerializerFactoryImpl implements RecordSerializerFactory {
    /** Context. */
    private GridCacheSharedContext cctx;

    /** Write pointer. */
    private boolean needWritePointer;

    /** Read record filter. */
    private @Nullable IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordDeserializeFilter;

    /**
     * Marshalled mode flag.
     * Records are not deserialized in this mode, {@link MarshalledRecord} with binary representation are read instead.
     */
    private boolean marshalledMode;

    /** Skip position check flag. Should be set for reading compacted wal file with skipped physical records. */
    private boolean skipPositionCheck;

    /**
     * @param cctx Cctx.
     */
    public RecordSerializerFactoryImpl(GridCacheSharedContext cctx) {
        this(cctx, null);
    }

    /**
     * @param cctx Cctx.
     */
    public RecordSerializerFactoryImpl(
        GridCacheSharedContext cctx,
        @Nullable IgniteBiPredicate<WALRecord.RecordType, WALPointer> readTypeFilter
    ) {
        this.cctx = cctx;
        this.recordDeserializeFilter = readTypeFilter;
    }

    /** {@inheritDoc} */
    @Override public RecordSerializer createSerializer(int ver) throws IgniteCheckedException {
        if (ver <= 0)
            throw new IgniteCheckedException("Failed to create a serializer (corrupted WAL file).");

        switch (ver) {
            case 1:
                return new RecordV1Serializer(
                    new RecordDataV1Serializer(cctx),
                    needWritePointer,
                    marshalledMode,
                    skipPositionCheck,
                    recordDeserializeFilter);

            case 2:
                return new RecordV2Serializer(
                    new RecordDataV2Serializer(cctx),
                    needWritePointer,
                    marshalledMode,
                    skipPositionCheck,
                    recordDeserializeFilter
                );

            default:
                throw new IgniteCheckedException("Failed to create a serializer with the given version " +
                    "(forward compatibility is not supported): " + ver);
        }
    }

    /**
     * @return Write pointer.
     */
    public boolean writePointer() {
        return needWritePointer;
    }

    /** {@inheritDoc} */
    @Override public RecordSerializerFactoryImpl writePointer(boolean writePointer) {
        this.needWritePointer = writePointer;

        return this;
    }

    /**
     * @return Read type filter.
     */
    public IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordDeserializeFilter() {
        return recordDeserializeFilter;
    }

    /** {@inheritDoc} */
    @Override public RecordSerializerFactoryImpl recordDeserializeFilter(
        @Nullable IgniteBiPredicate<WALRecord.RecordType, WALPointer> readTypeFilter
    ) {
        this.recordDeserializeFilter = readTypeFilter;

        return this;
    }

    /**
     * @return Marshalled mode. Records are not deserialized in this mode,  with binary representation are read instead.
     */
    public boolean marshalledMode() {
        return marshalledMode;
    }

    /** {@inheritDoc} */
    @Override public RecordSerializerFactoryImpl marshalledMode(boolean marshalledMode) {
        this.marshalledMode = marshalledMode;

        return this;
    }

    /**
     * @return Skip position check flag. Should be set for reading compacted wal file with skipped physical records.
     */
    public boolean skipPositionCheck() {
        return skipPositionCheck;
    }

    /** {@inheritDoc} */
    @Override public RecordSerializerFactoryImpl skipPositionCheck(boolean skipPositionCheck) {
        this.skipPositionCheck = skipPositionCheck;

        return this;
    }
}
