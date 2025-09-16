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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Class to pass to remote nodes transaction changes.
 *
 * @see TransactionConfiguration#setTxAwareQueriesEnabled(boolean)
 * @see ExecutionContext#transactionChanges(Collection)
 * @see ExecutionContext#transactionChanges(int, int[], Function)
 * @see QueryStartRequest#queryTransactionEntries()
 */
public class QueryTxEntry implements CalciteMessage {
    /** Cache id. */
    private int cacheId;

    /** Entry key. */
    private KeyCacheObject key;

    /** Entry value. */
    private CacheObject val;

    /** Entry version. */
    private GridCacheVersion ver;

    /**
     * Empty constructor.
     */
    public QueryTxEntry() {
        // No-op.
    }

    /**
     * @param cacheId Cache id.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     */
    public QueryTxEntry(int cacheId, KeyCacheObject key, CacheObject val, GridCacheVersion ver) {
        this.cacheId = cacheId;
        this.key = key;
        this.val = val;
        this.ver = ver;
    }

    /** @return Cache id. */
    public int cacheId() {
        return cacheId;
    }

    /** @return Entry key. */
    public KeyCacheObject key() {
        return key;
    }

    /** @return Entry value. */
    public CacheObject value() {
        return val;
    }

    /** @return Entry version. */
    public GridCacheVersion version() {
        return ver;
    }

    /** */
    public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        CacheObjectContext coctx = ctx.cacheContext(cacheId).cacheObjectContext();

        key.prepareMarshal(coctx);

        if (val != null)
            val.prepareMarshal(coctx);
    }

    /** */
    public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        CacheObjectContext coctx = ctx.cacheContext(cacheId).cacheObjectContext();

        key.finishUnmarshal(coctx, ldr);

        if (val != null)
            val.finishUnmarshal(coctx, ldr);

    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.QUERY_TX_ENTRY;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt(cacheId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeKeyCacheObject(key))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCacheObject(val))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMessage(ver))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                cacheId = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                key = reader.readKeyCacheObject();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                val = reader.readCacheObject();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                ver = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }
}
