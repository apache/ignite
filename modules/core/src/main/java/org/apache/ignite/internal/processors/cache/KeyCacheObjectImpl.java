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
import java.util.Arrays;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class KeyCacheObjectImpl extends CacheObjectAdapter implements KeyCacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int part = -1;

    /**
     *
     */
    public KeyCacheObjectImpl() {
        // No-op.
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     * @param part Partition.
     */
    public KeyCacheObjectImpl(Object val, byte[] valBytes, int part) {
        assert val != null;

        this.val = val;
        this.valBytes = valBytes;
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject copy(int part) {
        if (this.part == part)
            return this;

        return new KeyCacheObjectImpl(val, valBytes, part);
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public void partition(int part) {
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = ctx.kernalContext().cacheObjects().marshal(ctx, val);

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
    @Nullable @Override public <T> T value(CacheObjectValueContext ctx, boolean cpy) {
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

        if (val.getClass().isArray()) {
            if (val instanceof byte[])
                return Arrays.hashCode((byte[])val);
            else if (val instanceof int[])
                return Arrays.hashCode((int[])val);
            else if (val instanceof boolean[])
                return Arrays.hashCode((boolean[])val);
            else if (val instanceof float[])
                return Arrays.hashCode((float[])val);
            else if (val instanceof double[])
                return Arrays.hashCode((double[])val);
            else if (val instanceof long[])
                return Arrays.hashCode((long[])val);
            else if (val instanceof char[])
                return Arrays.hashCode((char[])val);
            else
                throw new IgniteException("Wrong cache index type: " + val.getClass().getName());
        } else
            return val.hashCode();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 90;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
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
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(KeyCacheObjectImpl.class);
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
                if (!writer.writeInt("part", part))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = ctx.kernalContext().cacheObjects().marshal(ctx, val);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
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

        if (val.getClass().isArray() && val.getClass().equals(other.val.getClass())){
            if (val instanceof byte[])
                return Arrays.equals((byte[])val, (byte[])other.val);
            else if (val instanceof int[])
                return Arrays.equals((int[])val, (int[])other.val);
            else if (val instanceof boolean[])
                return Arrays.equals((boolean[])val, (boolean[])other.val);
            else if (val instanceof float[])
                return Arrays.equals((float[])val, (float[])other.val);
            else if (val instanceof double[])
                return Arrays.equals((double[])val, (double[])other.val);
            else if (val instanceof long[])
                return Arrays.equals((long[])val, (long[])other.val);
            else if (val instanceof char[])
                return Arrays.equals((char[])val, (char[])other.val);
            else
                throw new IgniteException("Wrong cache index type: " + val.getClass().getName());
        }

        return val.equals(other.val);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(S.INCLUDE_SENSITIVE ? getClass().getSimpleName() : "KeyCacheObject",
            "part", part, true,
            "val", val, true,
            "hasValBytes", valBytes != null, false);
    }
}
