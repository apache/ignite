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

package org.apache.ignite.internal.processors.cache.transactions;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.INT;
import static org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType.MSG;

/**
 * Near cache unlock request.
 */
public class GridRollbackToSavepointRequest extends GridRollbackToSavepointMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Transaction version. */
    private GridCacheVersion ver;

    /** Cache IDs with their keys. */
    @GridToStringInclude
    @GridDirectMap(keyType = Integer.class, valueType = List.class)
    private LinkedHashMap<Integer, List<KeyCacheObject>> caches;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridRollbackToSavepointRequest() {
        // No-op.
    }

    /**
     * @param ver Version.
     * @param caches Cache ids with their keys.
     * @param futId Future id.
     * @param miniId Mini future id.
     */
    public GridRollbackToSavepointRequest(
        GridCacheVersion ver,
        LinkedHashMap<Integer, List<KeyCacheObject>> caches,
        IgniteUuid futId,
        int miniId
    ) {
        super(futId, miniId);

        this.ver = ver;
        this.caches = caches;
    }

    /**
     * @return Mapping.
     */
    public Map<Integer, List<KeyCacheObject>> caches() {
        return caches;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return First cache id from {@link #caches()}.
     */
    public int cacheId() {
        return caches.keySet().iterator().next();
    }

    /**
     * @return Max amount of keys in {@link #caches()} (needed only locally for response optimization).
     */
    public int keySize() {
        return caches.values().stream().mapToInt(List::size).max().getAsInt();
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
            case 2:
                if (!writer.writeMessage("ver", ver))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection("cacheIds", caches.keySet(), INT))
                    return false;

                for (Map.Entry<Integer, List<KeyCacheObject>> entry : caches.entrySet()) {
                    if (!writer.writeCollection("keys" + entry.getKey(), entry.getValue(), MSG))
                        return false;
                }

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
            case 2:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                Collection<Integer> cacheIds = reader.readCollection("cacheIds", INT);

                if (!reader.isLastRead())
                    return false;

                caches = new LinkedHashMap<>(U.capacity(cacheIds.size()));

                for (Integer id : cacheIds) {
                    caches.put(id, reader.readCollection("keys" + id, MSG));

                    if (!reader.isLastRead())
                        return false;
                }

                reader.incrementState();

        }

        return reader.afterMessageRead(GridRollbackToSavepointRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 2048;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException{
        for (Map.Entry<Integer, List<KeyCacheObject>> e : caches.entrySet()) {
            CacheObjectContext objCtx = ctx.cacheObjectContext(e.getKey());

            for (KeyCacheObject key : e.getValue())
                key.prepareMarshal(objCtx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        for (Map.Entry<Integer, List<KeyCacheObject>> e : caches.entrySet()) {
            CacheObjectContext cacheCtx = ctx.cacheContext(e.getKey()).cacheObjectContext();

            for (KeyCacheObject key : e.getValue())
                key.finishUnmarshal(cacheCtx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRollbackToSavepointRequest.class, this, super.toString());
    }
}
