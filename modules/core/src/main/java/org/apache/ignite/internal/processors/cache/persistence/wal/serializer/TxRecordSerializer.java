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
import org.apache.ignite.internal.pagemem.wal.record.MvccTxRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionState;

/**
 * {@link TxRecord} WAL serializer.
 */
public class TxRecordSerializer {
    /** Mvcc version record size. */
    static final int MVCC_VERSION_SIZE = 8 + 8 + 4;

    /**
     * Reads {@link MvccVersion} from given input.
     *
     * @param in Data input to read from.
     * @return Mvcc version.
     */
    public MvccVersion readMvccVersion(ByteBufferBackedDataInput in) throws IOException {
        in.ensure(MVCC_VERSION_SIZE);

        long coordVer = in.readLong();
        long cntr = in.readLong();
        int opCntr = in.readInt();

        return new MvccVersionImpl(coordVer, cntr, opCntr);
    }

    /**
     * Writes {@link MvccVersion} to given buffer.
     *
     * @param buf Buffer to write.
     * @param mvccVer Mvcc version.
     */
    public void putMvccVersion(ByteBuffer buf, MvccVersion mvccVer) {
        buf.putLong(mvccVer.coordinatorVersion());
        buf.putLong(mvccVer.counter());

        buf.putInt(mvccVer.operationCounter());
    }

    /**
     * Writes {@link TxRecord} to given buffer.
     *
     * @param rec TxRecord.
     * @param buf Byte buffer.
     */
    public void write(TxRecord rec, ByteBuffer buf) {
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
     */
    public TxRecord readTx(ByteBufferBackedDataInput in) throws IOException {
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
     */
    public int size(TxRecord rec) {
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

    /**
     * Reads {@link MvccTxRecord} from given input.
     *
     * @param in Input
     * @return MvccTxRecord.
     * @throws IOException In case of fail.
     */
    public MvccTxRecord readMvccTx(ByteBufferBackedDataInput in) throws IOException {
        byte txState = in.readByte();
        TransactionState state = TransactionState.fromOrdinal(txState);

        GridCacheVersion nearXidVer = RecordV1Serializer.readVersion(in, true);
        GridCacheVersion writeVer = RecordV1Serializer.readVersion(in, true);
        MvccVersion mvccVer = readMvccVersion(in);

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

        return new MvccTxRecord(state, nearXidVer, writeVer, participatingNodes, mvccVer, ts);
    }

    /**
     * Writes {@link MvccTxRecord} to given buffer.
     *
     * @param rec MvccTxRecord.
     * @param buf Byte buffer.
     * @throws IgniteCheckedException In case of fail.
     */
    public void write(MvccTxRecord rec, ByteBuffer buf) throws IgniteCheckedException {
        buf.put((byte)rec.state().ordinal());

        RecordV1Serializer.putVersion(buf, rec.nearXidVersion(), true);
        RecordV1Serializer.putVersion(buf, rec.writeVersion(), true);
        putMvccVersion(buf, rec.mvccVersion());

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
     * Returns size of marshalled {@link TxRecord} in bytes.
     *
     * @param rec TxRecord.
     * @return Size of TxRecord in bytes.
     * @throws IgniteCheckedException In case of fail.
     */
    public int size(MvccTxRecord rec) throws IgniteCheckedException {
       return size((TxRecord)rec) + MVCC_VERSION_SIZE;
    }
}
