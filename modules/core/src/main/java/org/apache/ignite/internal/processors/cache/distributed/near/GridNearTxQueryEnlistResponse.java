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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

public class GridNearTxQueryEnlistResponse extends GridCacheIdMessage {

    /** Future ID. */
    private IgniteUuid futId;

    /** Error. */
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

    /** */
    private int miniId;

    /** */
    private long res;

    private boolean removeMapping;

    /** */
    private GridCacheVersion lockVer;

    public GridNearTxQueryEnlistResponse() {
        // No-op.
    }

    /**
     * @param futId
     * @param miniId
     * @param lockVer
     * @param err
     */
    public GridNearTxQueryEnlistResponse(int cacheId, IgniteUuid futId, int miniId, GridCacheVersion lockVer, long res,Throwable err) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.lockVer = lockVer;
        this.res = res;
        this.err = err;
    }

    public GridCacheVersion version() {
        return lockVer;
    }

    public IgniteUuid futureId() {
        return futId;
    }

    public int miniId() {
        return miniId;
    }

    public long result() {
        return res;
    }

    public void removeMapping(boolean removeMapping) {
        this.removeMapping = removeMapping;
    }

    public boolean removeMapping() {
        return removeMapping;
    }

    @Nullable @Override public Throwable error() {
        return err;
    }

    @Override public boolean addDeploymentInfo() {
        return false;
    }

    @Override public byte fieldsCount() {
        return 9;
    }

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

        }

        return true;
    }

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

        }

        return reader.afterMessageRead(GridNearTxQueryEnlistResponse.class);
    }

    @Override public short directType() {
        return 147;
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
}
