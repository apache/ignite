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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionState;

/**
 * {@link TxRecord} WAL serializer.
 */
public class TxRecordSerializer {
    /** Cache shared context. */
    private GridCacheSharedContext cctx;

    /** Class loader to unmarshal consistent ids. */
    private ClassLoader classLoader;

    /**
     * Create an instance of serializer.
     *
     * @param cctx Cache shared context.
     */
    public TxRecordSerializer(GridCacheSharedContext cctx) {
        this.cctx = cctx;

        classLoader = U.resolveClassLoader(cctx.gridConfig());
    }

    /**
     * Writes {@link TxRecord} to given buffer.
     *
     * @param record TxRecord.
     * @param buf Byte buffer.
     * @throws IgniteCheckedException In case of fail.
     */
    public void writeTxRecord(TxRecord record, ByteBuffer buf) throws IgniteCheckedException {
        buf.put((byte) record.state().ordinal());
        RecordV1Serializer.putVersion(buf, record.nearXidVersion(), true);
        RecordV1Serializer.putVersion(buf, record.writeVersion(), true);

        if (record.participatingNodes() != null) {
            buf.putInt(record.participatingNodes().keySet().size());

            for (Object primaryNode : record.participatingNodes().keySet()) {
                writeConsistentId(primaryNode, buf);

                Collection<Object> backupNodes = record.participatingNodes().get(primaryNode);

                buf.putInt(backupNodes.size());

                for (Object backupNode : backupNodes)
                    writeConsistentId(backupNode, buf);
            }
        }
        else {
            // Put zero size of participating nodes.
            buf.putInt(0);
        }

        buf.put((byte) (record.remote() ? 1 : 0));

        if (record.remote())
            writeConsistentId(record.primaryNode(), buf);

        buf.putLong(record.timestamp());
    }

    /**
     * Reads {@link TxRecord} from given input.
     *
     * @param in Input
     * @return TxRecord.
     * @throws IOException In case of fail.
     * @throws IgniteCheckedException In case of fail.
     */
    public TxRecord readTxRecord(ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        byte txState = in.readByte();
        TransactionState state = TransactionState.fromOrdinal(txState);

        GridCacheVersion nearXidVer = RecordV1Serializer.readVersion(in, true);
        GridCacheVersion writeVer = RecordV1Serializer.readVersion(in, true);

        int participatingNodesSize = in.readInt();
        Map<Object, Collection<Object>> participatingNodes = new HashMap<>(2 * participatingNodesSize);

        for (int i = 0; i < participatingNodesSize; i++) {
            Object primaryNode = readConsistentId(in);

            int backupNodesSize = in.readInt();

            Collection<Object> backupNodes = new ArrayList<>(backupNodesSize);

            for (int j = 0; j < backupNodesSize; j++) {
                Object backupNode = readConsistentId(in);

                backupNodes.add(backupNode);
            }

            participatingNodes.put(primaryNode, backupNodes);
        }

        boolean hasRemote = in.readByte() == 1;

        Object primaryNode = null;

        if (hasRemote)
            primaryNode = readConsistentId(in);

        long timestamp = in.readLong();

        return new TxRecord(state, nearXidVer, writeVer, participatingNodes, primaryNode, timestamp);
    }

    /**
     * Returns size of marshalled {@link TxRecord} in bytes.
     *
     * @param record TxRecord.
     * @return Size of TxRecord in bytes.
     * @throws IgniteCheckedException In case of fail.
     */
    public int sizeOfTxRecord(TxRecord record) throws IgniteCheckedException {
        int size = 0;

        size += /* transaction state. */ 1;
        size += CacheVersionIO.size(record.nearXidVersion(), true);
        size += CacheVersionIO.size(record.writeVersion(), true);

        size += /* primary nodes count. */ 4;

        if (record.participatingNodes() != null) {
            for (Object primaryNode : record.participatingNodes().keySet()) {
                size += /* byte array length. */ 4;
                size += marshalConsistentId(primaryNode).length;

                Collection<Object> backupNodes = record.participatingNodes().get(primaryNode);

                size += /* size of backup nodes. */ 4;

                for (Object backupNode : backupNodes) {
                    size += /* byte array length. */ 4;
                    size += marshalConsistentId(backupNode).length;
                }
            }
        }

        size += /* Is primary node exist. */ 1;

        if (record.remote()) {
            size += /* byte array length. */ 4;
            size += marshalConsistentId(record.primaryNode()).length;
        }

        size += /* Timestamp */ 8;

        return size;
    }

    /**
     * Marshal consistent id to byte array.
     *
     * @param consistentId Consistent id.
     * @return Marshalled byte array.
     * @throws IgniteCheckedException In case of fail.
     */
    public byte[] marshalConsistentId(Object consistentId) throws IgniteCheckedException {
        return cctx.marshaller().marshal(consistentId);
    }

    /**
     * Read consistent id from given input.
     *
     * @param in Input.
     * @return Consistent id.
     * @throws IOException In case of fail.
     * @throws IgniteCheckedException In case of fail.
     */
    public Object readConsistentId(ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        int len = in.readInt();
        in.ensure(len);

        byte[] content = new byte[len];
        in.readFully(content);

        return cctx.marshaller().unmarshal(content, classLoader);
    }

    /**
     * Write consistent id to given buffer.
     *
     * @param consistentId Consistent id.
     * @param buf Byte buffer.
     * @throws IgniteCheckedException In case of fail.
     */
    public void writeConsistentId(Object consistentId, ByteBuffer buf) throws IgniteCheckedException {
        byte[] content = marshalConsistentId(consistentId);

        buf.putInt(content.length);
        buf.put(content);
    }
}
