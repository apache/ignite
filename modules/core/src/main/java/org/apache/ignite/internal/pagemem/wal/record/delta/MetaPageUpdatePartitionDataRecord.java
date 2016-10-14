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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.database.tree.io.PagePartitionMetaIO;

/**
 *
 */
public class MetaPageUpdatePartitionDataRecord extends PageDeltaRecord {
    /** */
    private long updateCntr;

    /** */
    private long globalRmvId;

    /** */
    private int partSize;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     */
    public MetaPageUpdatePartitionDataRecord(int cacheId, long pageId, long updateCntr, long globalRmvId, int partSize) {
        super(cacheId, pageId);

        this.updateCntr = updateCntr;
        this.globalRmvId = globalRmvId;
        this.partSize = partSize;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updateCntr;
    }

    /**
     * @return Global remove ID.
     */
    public long globalRemoveId() {
        return globalRmvId;
    }

    /**
     * @return Partition size.
     */
    public int partitionSize() {
        return partSize;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(ByteBuffer buf) throws IgniteCheckedException {
        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(buf);

        io.setUpdateCounter(buf, updateCntr);
        io.setGlobalRemoveId(buf, globalRmvId);
        io.setSize(buf, partSize);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS;
    }
}
