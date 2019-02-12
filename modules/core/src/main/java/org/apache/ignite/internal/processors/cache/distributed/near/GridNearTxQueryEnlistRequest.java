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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridNearTxQueryEnlistRequest extends GridCacheIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long threadId;

    /** */
    private IgniteUuid futId;

    /** */
    private boolean clientFirst;

    /** */
    private int miniId;

    /** */
    private UUID subjId;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private GridCacheVersion lockVer;

    /** */
    private MvccSnapshot mvccSnapshot;

    /** */
    private int[] cacheIds;

    /** */
    private int[] parts;

    /** */
    private String schema;

    /** */
    private String qry;

    /** */
    @GridDirectTransient
    private Object[] params;

    /** */
    private byte[] paramsBytes;

    /** */
    private int flags;

    /** */
    private long timeout;

    /** */
    private long txTimeout;

    /** */
    private int taskNameHash;

    /** */
    private int pageSize;

    /** */
    public GridNearTxQueryEnlistRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache id.
     * @param threadId Thread id.
     * @param futId Future id.
     * @param miniId Mini fture id.
     * @param subjId Subject id.
     * @param topVer Topology version.
     * @param lockVer Lock version.
     * @param mvccSnapshot MVCC snspshot.
     * @param cacheIds Involved cache ids.
     * @param parts Partitions.
     * @param schema Schema name.
     * @param qry Query string.
     * @param params Query parameters.
     * @param flags Flags.
     * @param pageSize Fetch page size.
     * @param timeout Timeout milliseconds.
     * @param txTimeout Tx timeout milliseconds.
     * @param taskNameHash Task name hash.
     * @param clientFirst {@code True} if this is the first client request.
     */
    public GridNearTxQueryEnlistRequest(
        int cacheId,
        long threadId,
        IgniteUuid futId,
        int miniId,
        UUID subjId,
        AffinityTopologyVersion topVer,
        GridCacheVersion lockVer,
        MvccSnapshot mvccSnapshot,
        int[] cacheIds,
        int[] parts,
        String schema,
        String qry,
        Object[] params,
        int flags,
        int pageSize,
        long timeout,
        long txTimeout,
        int taskNameHash,
        boolean clientFirst) {
        this.cacheIds = cacheIds;
        this.parts = parts;
        this.schema = schema;
        this.qry = qry;
        this.params = params;
        this.flags = flags;
        this.pageSize = pageSize;
        this.txTimeout = txTimeout;
        this.cacheId = cacheId;
        this.threadId = threadId;
        this.futId = futId;
        this.miniId = miniId;
        this.subjId = subjId;
        this.topVer = topVer;
        this.lockVer = lockVer;
        this.mvccSnapshot = mvccSnapshot;
        this.timeout = timeout;
        this.taskNameHash = taskNameHash;
        this.clientFirst = clientFirst;
    }

    /**
     * @return Thread id.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Future id.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @return Subject id.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Lock version.
     */
    public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @return MVCC snapshot.
     */
    public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Involved cache ids.
     */
    public int[] cacheIds() {
        return cacheIds;
    }

    /**
     * @return Partitions.
     */
    public int[] partitions() {
        return parts;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schema;
    }

    /**
     * @return Query string.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Query parameters.
     */
    public Object[] parameters() {
        return params;
    }

    /**
     * @return Flags.
     */
    public int flags() {
        return flags;
    }

    /**
     * @return Fetch page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Timeout milliseconds.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @return Tx timeout milliseconds.
     */
    public long txTimeout() {
        return txTimeout;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return {@code True} if this is the first client request.
     */
    public boolean firstClientRequest() {
        return clientFirst;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 22;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (params != null && paramsBytes == null)
            paramsBytes = U.marshal(ctx, params);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (paramsBytes != null && params == null)
            params = U.unmarshal(ctx, paramsBytes, ldr);
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
            case 4:
                if (!writer.writeIntArray("cacheIds", cacheIds))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeBoolean("clientFirst", clientFirst))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeInt("flags", flags))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("lockVer", lockVer))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("mvccSnapshot", mvccSnapshot))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeInt("pageSize", pageSize))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeByteArray("paramsBytes", paramsBytes))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeIntArray("parts", parts))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeString("qry", qry))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeString("schema", schema))
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
                if (!writer.writeLong("threadId", threadId))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeLong("txTimeout", txTimeout))
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
            case 4:
                cacheIds = reader.readIntArray("cacheIds");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                clientFirst = reader.readBoolean("clientFirst");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                flags = reader.readInt("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                lockVer = reader.readMessage("lockVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                mvccSnapshot = reader.readMessage("mvccSnapshot");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                pageSize = reader.readInt("pageSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                paramsBytes = reader.readByteArray("paramsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                parts = reader.readIntArray("parts");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                qry = reader.readString("qry");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                schema = reader.readString("schema");

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
                threadId = reader.readLong("threadId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                txTimeout = reader.readLong("txTimeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearTxQueryEnlistRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 151;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxQueryEnlistRequest.class, this);
    }
}
