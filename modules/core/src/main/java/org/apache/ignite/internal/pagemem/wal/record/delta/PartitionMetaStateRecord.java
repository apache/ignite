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
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.database.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;

/**
 *
 */
public class PartitionMetaStateRecord extends PageDeltaRecord {
    /** State. */
    private final byte state;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     */
    public PartitionMetaStateRecord(int cacheId, long pageId, GridDhtPartitionState state) {
        super(cacheId, pageId);
        this.state = (byte)state.ordinal();
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(ByteBuffer buf) throws IgniteCheckedException {
        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(buf);

        io.setPartitionState(buf, state);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return WALRecord.RecordType.PART_META_UPDATE_STATE;
    }

    /**
     *
     */
    public byte state() {
        return state;
    }
}
