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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
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
 * A response to {@link GridNearTxEnlistRequest}.
 */
public class GridNearTxEnlistResponse extends GridCacheIdMessage implements ExceptionAware {
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
    private GridCacheReturn res;

    /** */
    private GridCacheVersion lockVer;

    /** */
    private GridCacheVersion dhtVer;

    /** */
    private IgniteUuid dhtFutId;

    /** New DHT nodes involved into transaction. */
    @GridDirectCollection(UUID.class)
    private Collection<UUID> newDhtNodes;

    /**
     * Default constructor.
     */
    public GridNearTxEnlistResponse() {
        // No-op.
    }

    /**
     * Constructor for normal result.
     *
     * @param cacheId Cache id.
     * @param futId Future id.
     * @param miniId Mini future id.
     * @param lockVer Lock version.
     * @param res Result.
     * @param dhtVer Dht version.
     * @param dhtFutId Dht future id.
     * @param newDhtNodes New DHT nodes involved into transaction.
     */
    public GridNearTxEnlistResponse(int cacheId,
        IgniteUuid futId,
        int miniId,
        GridCacheVersion lockVer,
        GridCacheReturn res,
        GridCacheVersion dhtVer,
        IgniteUuid dhtFutId,
        Set<UUID> newDhtNodes) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.lockVer = lockVer;
        this.res = res;
        this.dhtVer = dhtVer;
        this.dhtFutId = dhtFutId;
        this.newDhtNodes = newDhtNodes;
    }

    /**
     * Constructor for error result.
     *
     * @param cacheId Cache id.
     * @param futId Future id.
     * @param miniId Mini future id.
     * @param lockVer Lock version.
     * @param err Error.
     */
    public GridNearTxEnlistResponse(int cacheId, IgniteUuid futId, int miniId, GridCacheVersion lockVer,
        Throwable err) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.lockVer = lockVer;
        this.err = err;
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
    public GridCacheReturn result() {
        return res;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /**
     * @return Dht version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * @return Dht future id.
     */
    public IgniteUuid dhtFutureId() {
        return dhtFutId;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 11;
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
                if (!writer.writeIgniteUuid("dhtFutId", dhtFutId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("dhtVer", dhtVer))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("lockVer", lockVer))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("newDhtNodes", newDhtNodes, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMessage("res", res))
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
                dhtFutId = reader.readIgniteUuid("dhtFutId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                dhtVer = reader.readMessage("dhtVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                lockVer = reader.readMessage("lockVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                newDhtNodes = reader.readCollection("newDhtNodes", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                res = reader.readMessage("res");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearTxEnlistResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 160;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (err != null && errBytes == null)
            errBytes = U.marshal(ctx.marshaller(), err);

        if (res != null)
            res.prepareMarshal(cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (errBytes != null)
            err = U.unmarshal(ctx, errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (res != null)
            res.finishUnmarshal(cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxEnlistResponse.class, this);
    }
}
