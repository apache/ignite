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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotWithoutTxs;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * First enlist request.
 */
public class GridDhtTxQueryFirstEnlistRequest extends GridDhtTxQueryEnlistRequest {
    /** */
    private static final long serialVersionUID = -7494735627739420176L;

    /** Tx initiator. Primary node in case of remote DHT tx. */
    private UUID subjId;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private long crdVer;

    /** */
    private long cntr;

    /** */
    private long cleanupVer;

    /** */
    private long timeout;

    /** */
    private int taskNameHash;

    /** */
    private UUID nearNodeId;

    /** Near tx version. */
    private GridCacheVersion nearXidVer;

    /**
     *
     */
    public GridDhtTxQueryFirstEnlistRequest() {
    }

    /**
     * @param cacheId Cache id.
     * @param dhtFutId DHT future id.
     * @param subjId Subject id.
     * @param topVer Topology version.
     * @param lockVer Lock version.
     * @param snapshot Mvcc snapshot.
     * @param timeout Timeout.
     * @param taskNameHash Task name hash.
     * @param nearNodeId Near node id.
     * @param nearXidVer Near xid version.
     * @param op Operation.
     * @param batchId Batch id.
     * @param keys Keys.
     * @param vals Values.
     * @param updCntrs Update counters.
     */
    GridDhtTxQueryFirstEnlistRequest(int cacheId,
        IgniteUuid dhtFutId,
        UUID subjId,
        AffinityTopologyVersion topVer,
        GridCacheVersion lockVer,
        MvccSnapshot snapshot,
        long timeout,
        int taskNameHash,
        UUID nearNodeId,
        GridCacheVersion nearXidVer,
        EnlistOperation op,
        int batchId,
        List<KeyCacheObject> keys,
        List<Message> vals,
        GridLongList updCntrs) {
        super(cacheId, dhtFutId, lockVer, op, batchId, snapshot.operationCounter(), keys, vals, updCntrs);
        this.cacheId = cacheId;
        this.subjId = subjId;
        this.topVer = topVer;
        this.crdVer = snapshot.coordinatorVersion();
        this.cntr = snapshot.counter();
        this.cleanupVer = snapshot.cleanupVersion();
        this.timeout = timeout;
        this.taskNameHash = taskNameHash;
        this.nearNodeId = nearNodeId;
        this.nearXidVer = nearXidVer;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Near node id.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return Near transaction ID.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Max lock wait time.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @return Subject id.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return MVCC snapshot.
     */
    public MvccSnapshot mvccSnapshot() {
        return new MvccSnapshotWithoutTxs(crdVer, cntr, operationCounter(), cleanupVer);
    }

    /**
     * @return Coordinator version.
     */
    public long coordinatorVersion() {
        return crdVer;
    }

    /**
     * @return Counter.
     */
    public long counter() {
        return cntr;
    }

    /**
     * @return Cleanup version.
     */
    public long cleanupVersion() {
        return cleanupVer;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 156;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 11:
                if (!writer.writeLong("cleanupVer", cleanupVer))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeLong("cntr", cntr))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeLong("crdVer", crdVer))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeUuid("nearNodeId", nearNodeId))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMessage("nearXidVer", nearXidVer))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 11:
                cleanupVer = reader.readLong("cleanupVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                cntr = reader.readLong("cntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                crdVer = reader.readLong("crdVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                nearNodeId = reader.readUuid("nearNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                nearXidVer = reader.readMessage("nearXidVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxQueryFirstEnlistRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxQueryFirstEnlistRequest.class, this);
    }
}
