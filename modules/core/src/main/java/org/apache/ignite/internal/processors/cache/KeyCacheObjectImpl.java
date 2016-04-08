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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class KeyCacheObjectImpl extends CacheObjectAdapter implements KeyCacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    private int partition;

    /**
     *
     */
    public KeyCacheObjectImpl() {
        // No-op.
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public KeyCacheObjectImpl(Object val, byte[] valBytes) {
        this(val, valBytes, -1);
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public KeyCacheObjectImpl(Object val, byte[] valBytes, int partition) {
        assert val != null;

        this.val = val;
        this.valBytes = valBytes;
        this.partition = partition;
    }

    @Override public int partition() {
        return partition;
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = ctx.processor().marshal(ctx, val);

        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean internal() {
        assert val != null;

        return val instanceof GridCacheInternal;
    }

    /** {@inheritDoc} */
    @Override public boolean isPlatformType() {
        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
        assert val != null;

        return (T)val;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        assert val != null;

        return val.hashCode();
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 90;
    }

    @Override public byte fieldsCount() {
        return 2;
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
            case 1:
                if (!writer.writeInt("partition", partition))
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
            case 1:
                partition = reader.readInt("partition");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(KeyCacheObjectImpl.class);

    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = ctx.kernalContext().cacheObjects().marshal(ctx, val);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (val == null) {
            assert valBytes != null;

            val = ctx.kernalContext().cacheObjects().unmarshal(ctx, valBytes, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof KeyCacheObjectImpl))
            return false;

        KeyCacheObjectImpl other = (KeyCacheObjectImpl)obj;

        return val.equals(other.val);
    }
}