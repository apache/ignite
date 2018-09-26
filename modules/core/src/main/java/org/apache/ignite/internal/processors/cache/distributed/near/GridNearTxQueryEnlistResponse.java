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
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.ExceptionAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * A response to {@link GridNearTxQueryEnlistRequest}.
 */
public class GridNearTxQueryEnlistResponse extends GridCacheIdMessage implements ExceptionAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Error. */
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

    /** Mini future id. */
    private int miniId;

    /** Result. */
    private long res;

    /** Remove mapping flag. */
    private boolean removeMapping;

    /** */
    private GridCacheVersion lockVer;

    /** New DHT nodes involved into transaction. */
    @GridDirectCollection(UUID.class)
    private Collection<UUID> newDhtNodes;

    /**
     * Default constructor.
     */
    public GridNearTxQueryEnlistResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache id.
     * @param futId Future id.
     * @param miniId Mini future id.
     * @param lockVer Lock version.
     * @param err Error.
     */
    public GridNearTxQueryEnlistResponse(int cacheId, IgniteUuid futId, int miniId, GridCacheVersion lockVer, Throwable err) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.lockVer = lockVer;
        this.err = err;
    }

    /**
     * @param cacheId Cache id.
     * @param futId Future id.
     * @param miniId Mini future id.
     * @param lockVer Lock version.
     * @param res Result.
     * @param removeMapping Remove mapping flag.
     * @param newDhtNodes New DHT nodes involved into transaction.
     */
    public GridNearTxQueryEnlistResponse(int cacheId, IgniteUuid futId, int miniId, GridCacheVersion lockVer, long res,
        boolean removeMapping, Set<UUID> newDhtNodes) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.lockVer = lockVer;
        this.res = res;
        this.removeMapping = removeMapping;
        this.newDhtNodes = newDhtNodes;
    }

    /**
     * @return Loc version.
     */
    public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @return Future id.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future id.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @return Result.
     */
    public long result() {
        return res;
    }

    /**
     * @return New DHT nodes involved into transaction.
     */
    public Collection<UUID> newDhtNodes() {
        return newDhtNodes;
    }

    /**
     * @return Remove mapping flag.
     */
    public boolean removeMapping() {
        return removeMapping;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
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
            case 3:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("lockVer", lockVer))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeBoolean("removeMapping", removeMapping))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeLong("res", res))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("newDhtNodes", newDhtNodes, MessageCollectionItemType.UUID))
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
            case 3:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                lockVer = reader.readMessage("lockVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                removeMapping = reader.readBoolean("removeMapping");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                res = reader.readLong("res");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                newDhtNodes = reader.readCollection("newDhtNodes", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridNearTxQueryEnlistResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 152;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (err != null && errBytes == null)
            errBytes = U.marshal(ctx.marshaller(), err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null)
            err = U.unmarshal(ctx, errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxQueryEnlistResponse.class, this);
    }
}
