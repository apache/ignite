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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheInvokeDirectResult implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private KeyCacheObject key;

    /** */
    @GridToStringInclude
    private CacheObject res;

    /** */
    @GridToStringInclude(sensitive = true)
    @GridDirectTransient
    private Exception err;

    /** */
    private byte[] errBytes;

    /**
     * Required for {@link Message}.
     */
    public CacheInvokeDirectResult() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param res Result.
     */
    public CacheInvokeDirectResult(KeyCacheObject key, CacheObject res) {
        this.key = key;
        this.res = res;
    }

    /**
     * @param key Key.
     * @param err Exception thrown by {@link EntryProcessor#process(MutableEntry, Object...)}.
     */
    public CacheInvokeDirectResult(KeyCacheObject key, Exception err) {
        this.key = key;
        this.err = err;
    }

    /**
     * @return Key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Result.
     */
    public CacheObject result() {
        return res;
    }

    /**
     * @return Error.
     */
    @Nullable public Exception error() {
        return err;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        key.prepareMarshal(ctx.cacheObjectContext());

        if (err != null && errBytes == null)
            errBytes = U.marshal(ctx.marshaller(), err);

        if (res != null)
            res.prepareMarshal(ctx.cacheObjectContext());
    }

    /**
     * @param ctx Cache context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        key.finishUnmarshal(ctx.cacheObjectContext(), ldr);

        if (errBytes != null && err == null)
            err = U.unmarshal(ctx.marshaller(), errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (res != null)
            res.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 93;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 2:
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

        switch (reader.state()) {
            case 0:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                res = reader.readMessage("res");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheInvokeDirectResult.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInvokeDirectResult.class, this);
    }
}