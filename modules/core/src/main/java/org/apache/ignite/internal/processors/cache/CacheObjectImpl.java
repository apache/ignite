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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 *
 */
public class CacheObjectImpl extends CacheObjectAdapter {
    /**
     *
     */
    public CacheObjectImpl() {
        // No-op.
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public CacheObjectImpl(Object val, byte[] valBytes) {
        assert val != null || valBytes != null;

        this.val = val;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T value(GridCacheContext ctx, boolean cpy) {
        cpy = cpy && needCopy(ctx);

        try {
            if (cpy) {
                byte[] bytes = valueBytes(ctx);

                if (byteArray())
                    return (T)Arrays.copyOf(bytes, bytes.length);
                else
                    return (T)ctx.portable().unmarshal(ctx.cacheObjectContext(), valBytes, U.gridClassLoader());
            }

            if (val != null)
                return (T)val;

            assert valBytes != null;

            val = ctx.portable().unmarshal(ctx.cacheObjectContext(), valBytes, U.gridClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to unmarshal object.", e);
        }

        return (T)val;
    }

    /** {@inheritDoc} */
    @Override public boolean byteArray() {
        return val instanceof byte[];
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(GridCacheContext ctx) throws IgniteCheckedException {
        if (byteArray())
            return (byte[])val;

        if (valBytes == null)
            valBytes = ctx.portable().marshal(ctx.cacheObjectContext(), val);

        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        if (valBytes == null && !byteArray())
            valBytes = ctx.kernalContext().portable().marshal(ctx, val);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert val != null || valBytes != null;

        if (val == null && ctx.isUnmarshalValues())
            val = ctx.portable().unmarshal(ctx.cacheObjectContext(), valBytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        assert val != null || valBytes != null;

        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        boolean byteArr = byteArray();

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("valBytes", byteArr ? (byte[])val : valBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBoolean("byteArr", byteArr))
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
                valBytes = reader.readByteArray("valBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                boolean byteArr = reader.readBoolean("byteArr");

                if (!reader.isLastRead())
                    return false;

                if (byteArr) {
                    val = valBytes;

                    valBytes = null;
                }

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 89;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        assert false;

        return super.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        assert false;

        return super.equals(obj);
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(GridCacheContext ctx) {
        return this;
    }
}
