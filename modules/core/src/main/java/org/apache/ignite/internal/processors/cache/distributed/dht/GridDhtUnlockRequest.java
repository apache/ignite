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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedUnlockRequest;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * DHT cache unlock request.
 */
public class GridDhtUnlockRequest extends GridDistributedUnlockRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near keys. */
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> nearKeys;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtUnlockRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param dhtCnt Key count.
     * @param addDepInfo Deployment info flag.
     */
    public GridDhtUnlockRequest(int cacheId, int dhtCnt, boolean addDepInfo) {
        super(cacheId, dhtCnt, addDepInfo);
    }

    /**
     * @return Near keys.
     */
    public List<KeyCacheObject> nearKeys() {
        return nearKeys;
    }

    /**
     * Adds a Near key.
     *
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void addNearKey(KeyCacheObject key)
        throws IgniteCheckedException {
        if (nearKeys == null)
            nearKeys = new ArrayList<>();

        nearKeys.add(key);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        prepareMarshalCacheObjects(nearKeys, ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        finishUnmarshalCacheObjects(nearKeys, ctx.cacheContext(cacheId), ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtUnlockRequest.class, this);
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
            case 8:
                if (!writer.writeCollection("nearKeys", nearKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("partIds", partIds, MessageCollectionItemType.INT))
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
            case 8:
                nearKeys = reader.readCollection("nearKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                partIds = reader.readCollection("partIds", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtUnlockRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 36;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }
}
