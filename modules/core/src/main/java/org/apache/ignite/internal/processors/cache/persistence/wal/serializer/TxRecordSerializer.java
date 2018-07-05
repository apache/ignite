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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionState;

/**
 * {@link TxRecord} WAL serializer.
 */
public class TxRecordSerializer {
    /**
     * Writes {@link TxRecord} to given buffer.
     *
     * @param rec TxRecord.
     * @param buf Byte buffer.
     * @throws IgniteCheckedException In case of fail.
     */
    public void write(TxRecord rec, ByteBuffer buf) throws IgniteCheckedException {
        buf.put((byte)rec.state().ordinal());
        RecordV1Serializer.putVersion(buf, rec.nearXidVersion(), true);
        RecordV1Serializer.putVersion(buf, rec.writeVersion(), true);

        Map<Short, Collection<Short>> participatingNodes = rec.participatingNodes();

        if (participatingNodes != null && !participatingNodes.isEmpty()) {
            buf.putInt(participatingNodes.size());

            for (Map.Entry<Short, Collection<Short>> e : participatingNodes.entrySet()) {
                buf.putShort(e.getKey());

                Collection<Short> backupNodes = e.getValue();

                buf.putInt(backupNodes.size());

                for (short backupNode : backupNodes)
                    buf.putShort(backupNode);
            }
        }
        else
            buf.putInt(0); // Put zero size of participating nodes.

        buf.putLong(rec.timestamp());
    }

    /**
     * Reads {@link TxRecord} from given input.
     *
     * @param in Input
     * @return TxRecord.
     * @throws IOException In case of fail.
     * @throws IgniteCheckedException In case of fail.
     */
    public TxRecord read(ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        byte txState = in.readByte();
        TransactionState state = TransactionState.fromOrdinal(txState);

        GridCacheVersion nearXidVer = RecordV1Serializer.readVersion(in, true);
        GridCacheVersion writeVer = RecordV1Serializer.readVersion(in, true);

        int participatingNodesSize = in.readInt();

        Map<Short, Collection<Short>> participatingNodes = U.newHashMap(participatingNodesSize);

        for (int i = 0; i < participatingNodesSize; i++) {
            short primaryNode = in.readShort();

            int backupNodesSize = in.readInt();

            Collection<Short> backupNodes = new ArrayList<>(backupNodesSize);

            for (int j = 0; j < backupNodesSize; j++) {
                short backupNode = in.readShort();

                backupNodes.add(backupNode);
            }

            participatingNodes.put(primaryNode, backupNodes);
        }

        long ts = in.readLong();

        return new TxRecord(state, nearXidVer, writeVer, participatingNodes, ts);
    }

    /**
     * Returns size of marshalled {@link TxRecord} in bytes.
     *
     * @param rec TxRecord.
     * @return Size of TxRecord in bytes.
     * @throws IgniteCheckedException In case of fail.
     */
    public int size(TxRecord rec) throws IgniteCheckedException {
        int size = 0;

        size += /* transaction state. */ 1;
        size += CacheVersionIO.size(rec.nearXidVersion(), true);
        size += CacheVersionIO.size(rec.writeVersion(), true);

        size += /* primary nodes count. */ 4;

        Map<Short, Collection<Short>> participatingNodes = rec.participatingNodes();

        if (participatingNodes != null && !participatingNodes.isEmpty()) {
            for (Collection<Short> backupNodes : participatingNodes.values()) {
                size += /* Compact ID. */ 2;

                size += /* size of backup nodes. */ 4;

                size += /* Compact ID. */ 2 * backupNodes.size();
            }
        }

        size += /* Timestamp */ 8;

        return size;
    }
}
