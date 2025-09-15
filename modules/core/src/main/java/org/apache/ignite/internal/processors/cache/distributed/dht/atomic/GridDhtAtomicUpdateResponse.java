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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * DHT atomic cache backup update response.
 */
public class GridDhtAtomicUpdateResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Future version. */
    private long futId;

    /** */
    private UpdateErrors errs;

    /** Evicted readers. */
    @GridToStringInclude
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> nearEvicted;

    /** */
    private int partId;

    /**
     * Empty constructor.
     */
    public GridDhtAtomicUpdateResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param partId Partition.
     * @param futId Future ID.
     * @param addDepInfo Deployment info.
     */
    public GridDhtAtomicUpdateResponse(int cacheId, int partId, long futId, boolean addDepInfo) {
        this.cacheId = cacheId;
        this.partId = partId;
        this.futId = futId;
        this.addDepInfo = addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Future version.
     */
    public long futureId() {
        return futId;
    }

    /**
     * Sets update error.
     *
     * @param err Error.
     */
    public void onError(IgniteCheckedException err) {
        if (errs == null)
            errs = new UpdateErrors();

        errs.onError(err);
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return errs != null ? errs.error() : null;
    }

    /**
     * @return Evicted readers.
     */
    Collection<KeyCacheObject> nearEvicted() {
        return nearEvicted;
    }

    /**
     * @param nearEvicted Evicted near cache keys.
     */
    public void nearEvicted(List<KeyCacheObject> nearEvicted) {
        this.nearEvicted = nearEvicted;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        // Can be null if client near cache was removed, in this case assume do not need prepareMarshal.
        if (cctx != null) {
            prepareMarshalCacheObjects(nearEvicted, cctx);

            if (errs != null)
                errs.prepareMarshal(this, cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(nearEvicted, cctx, ldr);

        if (errs != null)
            errs.finishUnmarshal(this, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 4:
                if (!writer.writeMessage(errs))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong(futId))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection(nearEvicted, MessageCollectionItemType.KEY_CACHE_OBJECT))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt(partId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 4:
                errs = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                nearEvicted = reader.readCollection(MessageCollectionItemType.KEY_CACHE_OBJECT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                partId = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 39;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateResponse.class, this);
    }
}
