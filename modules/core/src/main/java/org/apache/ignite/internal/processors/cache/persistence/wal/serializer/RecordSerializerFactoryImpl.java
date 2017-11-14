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
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lang.IgnitePredicate;

/**
 *
 */
public class RecordSerializerFactoryImpl implements RecordSerializerFactory {
    /** Context. */
    private GridCacheSharedContext cctx;

    /** Write pointer. */
    private boolean writePointer;

    /** Read type filter. */
    private IgnitePredicate<WALRecord.RecordType> readTypeFilter;

    /**
     * @param cctx Cctx.
     */
    public RecordSerializerFactoryImpl(GridCacheSharedContext cctx) {
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public RecordSerializer createSerializer(int ver) throws IgniteCheckedException {
        if (ver <= 0)
            throw new IgniteCheckedException("Failed to create a serializer (corrupted WAL file).");

        switch (ver) {
            case 1:
                if (readTypeFilter != null)
                    throw new IgniteCheckedException("Read type filter is allowed only for version 2 or higher.");

                return new RecordV1Serializer(new RecordDataV1Serializer(cctx), writePointer);

            case 2:
                RecordDataV2Serializer dataV2Serializer = new RecordDataV2Serializer(new RecordDataV1Serializer(cctx));

                return new RecordV2Serializer(dataV2Serializer, writePointer, readTypeFilter);

            default:
                throw new IgniteCheckedException("Failed to create a serializer with the given version " +
                    "(forward compatibility is not supported): " + ver);
        }
    }

    /**
     * @return Write pointer.
     */
    public boolean writePointer() {
        return writePointer;
    }

    /**
     * @param writePointer New write pointer.
     */
    public RecordSerializerFactoryImpl writePointer(boolean writePointer) {
        this.writePointer = writePointer;

        return this;
    }

    /**
     * @return Read type filter.
     */
    public IgnitePredicate<WALRecord.RecordType> readTypeFilter() {
        return readTypeFilter;
    }

    /**
     * @param readTypeFilter New read type filter.
     */
    public RecordSerializerFactoryImpl readTypeFilter(IgnitePredicate<WALRecord.RecordType> readTypeFilter) {
        this.readTypeFilter = readTypeFilter;

        return this;
    }
}
