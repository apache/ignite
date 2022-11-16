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

import java.io.DataInput;
import java.io.IOException;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;

/**
 * Record data V3 serializer.
 */
public class RecordDataV3Serializer extends RecordDataV2Serializer {
    /**
     * Create an instance of V3 data serializer.
     *
     * @param cctx Cache shared context.
     */
    public RecordDataV3Serializer(GridCacheSharedContext cctx) {
        super(cctx);
    }

    /** */
    @Override protected void readPartitionState(DataInput buf, CacheState state) throws IOException {
        int partId = buf.readShort() & 0xFFFF;
        long size = buf.readLong();
        long partCntr = buf.readLong();
        long partPendingCntr = buf.readLong();
        byte partState = buf.readByte();

        state.addPartitionState(partId, size, partCntr, partPendingCntr, partState);
    }
}
